#!/usr/bin/env python2.7
"""Helpers for interacting with hypervisors and virtual machines"""

from . import sfdefaults
from . import SolidFireError, UnauthorizedError, TimeoutError
from .logutil import GetLogger
from .sfclient import SFClient
import inspect
import libvirt
import libvirt_qemu
import multiprocessing
from pyVim import connect as connectVSphere
# pylint: disable=no-name-in-module
from pyVmomi import vim, vmodl
# pylint: enable=no-name-in-module
import Queue
import requests
import sys
import time
from xml.etree import ElementTree

# Fix late-model python not working with self signed certs out of the box
try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass
try:
    import ssl
    #pylint: disable=protected-access
    ssl._create_default_https_context = ssl._create_unverified_context
    #pylint: enable=protected-access
except AttributeError:
    pass

# Register libvirt handler to catch error messages that would otherwise go to stderr
def libvirt_callback(_, err):
    # Log libvirt errors only at highest verbosity
    GetLogger().debug2("* * * libvirt error: {}".format(str(err)))
libvirt.registerErrorHandler(f=libvirt_callback, ctx=None)

import socket
socket.setdefaulttimeout(15)

class VirtualizationError(SolidFireError):
    """Parent exception for virtualization errors"""
    pass

class VirtualMachine(object):
    """
    Base class for virtual machines.  Do not instantiate this class directly, use the Create static method
    """

    def __init__(self, vmName, hostServer, hostUsername, hostPassword):
        self.vmName = vmName
        self.hostServer = hostServer
        self.hostUsername = hostUsername
        self.hostPassword = hostPassword
        self.vmType = "Unknown"
        self.log = GetLogger()

        self._unpicklable = ["log"]

    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.iteritems():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()

    def __str__(self):
        return "{{{} on {} ({})}}".format(self.vmName, self.vmType, self.hostServer)

    # VM operations
    def PowerOn(self):
        raise NotImplementedError()
    def PowerOff(self):
        raise NotImplementedError()
    def GetPowerState(self):
        raise NotImplementedError()
    def GetPXEMacAddress(self):
        raise NotImplementedError()
    def SetPXEBoot(self):
        raise NotImplementedError()
    def WaitForUp(self, timeout=300):
        raise NotImplementedError()


    @staticmethod
    def Create(vmName, mgmtServer, mgmtUsername, mgmtPassword):
        """
        Factory method to create a VirtualMachine of the correct sub-type

        Args:
            vmName:         the name of the VM on the hpervisor (str)
            mgmtServer:     the server used to manage the hypervisor, or the hypervisor itself (str)
            mgmtUsername:   the username for the management server (str)
            mgmtPassword:   the password for the management server (str)
        """
        log = GetLogger()

        # Try each type of VirtualMachine class until one works
        log.info("Connecting to hypervisor")
        for classname in ["VirtualMachineVMware", "VirtualMachineKVM"]:
            classdef = getattr(sys.modules[__name__], classname)
            log.debug2("Trying {} for VM {}".format(classname, vmName))
            try:
                return classdef(vmName, mgmtServer, mgmtUsername, mgmtPassword)
            except (VirtualizationError, TimeoutError) as ex:
                log.debug2(str(ex))

        raise VirtualizationError("Could not create VM object; check connection to management server and VM exists on server")

class VMHost(object):
    """
    Base class for virtualization hosts (hypervisors).  Do not instantiate this class directly, use the Create static method
    """

    def __init__(self, vmhostName, mgmtServer, mgmtUsername, mgmtPassword):
        self.vmhostName = vmhostName
        self.mgmtServer = mgmtServer
        self.mgmtUsername = mgmtUsername
        self.mgmtPassword = mgmtPassword
        self.hostType = "Unknown"
        self.log = GetLogger()

        self._unpicklable = ["log"]

    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.iteritems():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()

    def __str__(self):
        return "{{{} host {}}}".format(self.hostType, self.vmhostName)

    # Hypervisor operations
    def CreateDatastores(self, includeInternalDrives=False, includeSlotDrives=False):
        raise NotImplementedError()


    @staticmethod
    def Create(vmhostName, mgmtServer, mgmtUsername, mgmtPassword):
        """
        Factory method to create a VMHost of the correct sub-type

        Args:
            vmhostName:     the IP/name of the hypervisor (str)
            mgmtServer:     the server used to manage the hypervisor, or the hypervisor itself (str)
            mgmtUsername:   the username for the management server (str)
            mgmtPassword:   the password for the management server (str)
        """
        log = GetLogger()

        # Try each type of VMHost class until one works
        for classname, classdef in inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and \
                                                                                 issubclass(member, VMHost) and \
                                                                                 member != VMHost):
            log.debug2("Trying {} for VMHost {}".format(classname, vmhostName))
            try:
                return classdef(vmhostName, mgmtServer, mgmtUsername, mgmtPassword)
            except (VirtualizationError, TimeoutError) as ex:
                log.debug2(str(ex))

        raise VirtualizationError("Could not create VMhost object; check connection to management server and host exists on server")

# ================================================================================================================================
# VMware support

def VMwareConnect(server, username, password):
    """
    Connect to a vSphere server

    Args:
        server:     IP or hostname to connect to (str)
        username:   username for the server (str)
        password:   password for the server (str)
    """
    log = GetLogger()
    log.debug2("Trying to connect to VMware on {}".format(server))
    try:
        service = connectVSphere.SmartConnect(host=server, user=username, pwd=password)
    except vim.fault.InvalidLogin:
        raise UnauthorizedError.IPContext(ip=server)
    except vim.fault.HostConnectFault as e:
        raise VirtualizationError("Could not connect: " + str(e), e)
    except requests.exceptions.ConnectionError as e:
        raise VirtualizationError("Could not connect: " + str(e), e)
    except vmodl.MethodFault as e:
        raise VirtualizationError("Could not connect: " + str(e), e)
    except (socket.timeout, socket.error, socket.gaierror, socket.herror) as e:
        raise VirtualizationError("Could not connect: " + str(e), e)
    return service

def VMwareDisconnect(connection):
    """
    Disconnect a vsphere connection

    Args:
        connection:     the vsphere connection to close (vim.ServiceInstance)
    """
    connectVSphere.Disconnect(connection)

class VMwareConnection(object):
    """
    Context manager for vSphere connections
    """

    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password
        self.connection = None

    def __enter__(self):
        self.connection = VMwareConnect(self.server, self.username, self.password)
        return self.connection

    def __exit__(self, extype, value, tb):
        VMwareDisconnect(self.connection)

def VMwareFindObjectGetProperties(connection, obj_name, obj_type, properties=None, parent=None):
    """
    Search for an object in vSphere and retrieve a list of properties

    Arguments:
        connection:     the vsphere connection to use (vim.ServiceInstance)
        obj_name:       the name of the object to find (str)
        obj_type:       the type of the object, ex vim.VirtualMachine (vim.Something)
        properties:     the properties of the object to retrieve.  Set to None to return all properties, but this is very slow! (list of str)
        parent:         the parent object to start the search, such as a datacenter or host.  Set to None to search the entire vCenter (vim.Something)

    Returns:
        The requested vim object with only the requested properties
    """

    if obj_type == vim.VirtualMachine and obj_name in sfdefaults.blacklisted_vm_names:
        raise VirtualizationError('{} is a reserved name and cannot be used here'.format(obj_name))

    parent = parent or connection.content.rootFolder
    if not properties:
        prop_spec = vim.PropertyCollector.PropertySpec(all=True, type=obj_type)
    else:
        if 'name' not in properties:
            properties.append('name')
        prop_spec = vim.PropertyCollector.PropertySpec(all=False, pathSet=properties, type=obj_type)

    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=parent, type=[obj_type], recursive=True)
    trav_spec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False, type=vim.view.ContainerView)
    obj_spec = vim.PropertyCollector.ObjectSpec(obj=view, selectSet=[trav_spec], skip=False)
    filter_spec = vim.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec], reportMissingObjectsInResults=False)
    ret_options = vim.PropertyCollector.RetrieveOptions()
    result = connection.content.propertyCollector.RetrievePropertiesEx(specSet=[filter_spec], options=ret_options)
    while True:
        for obj_result in result.objects:
            try:
                match = False
                for prop in obj_result.propSet:
                    #if prop.name == 'name':
                    #    mylog.debug('found {}'.format(prop.val))
                    if prop.name == 'name' and prop.val == obj_name:
                        result_list.append(obj_result.obj)
                        match = True
                        break
                if match:
                    break
            except vmodl.fault.ManagedObjectNotFound:
                # The object was deleted while we were querying it
                continue
            if result_list:
                break
        if not result.token:
            break
        result = connection.content.propertyCollector.ContinueRetrievePropertiesEx(token=result.token)
    if not result_list:
        raise VirtualizationError('Could not find {}'.format(obj_name))
    return result_list[0]

def VMwareWaitForTasks(connection, tasks):
    """
    Wait for all outstanding tasks to complete

    Args:
        connection:     the vsphere connection to use (vim.ServiceInstance)
        tasks:          the list of tasks to wait for (list of vim.Task)
    """
    log = GetLogger()

    pc = connection.content.propertyCollector
    taskList = [str(task) for task in tasks]

    # Create filter
    objSpecs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task) for task in tasks]
    propSpec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task, pathSet=[], all=True)
    filterSpec = vmodl.query.PropertyCollector.FilterSpec()
    filterSpec.objectSet = objSpecs
    filterSpec.propSet = [propSpec]
    task_filter = pc.CreateFilter(filterSpec, True)

    try:
        version, state = None, None

        # Loop looking for updates till the state moves to a completed state.
        while len(taskList):
            update = pc.CheckForUpdates(version)
            if not update:
                time.sleep(1)
                continue
            for filterSet in update.filterSet:
                for objSet in filterSet.objectSet:
                    task = objSet.obj
                    if not str(task) in taskList:
                        continue
                    for change in objSet.changeSet:

                        # Initial change when the task starts
                        if change.name == 'info':
                            log.debug("progress={}%".format(change.val.progress or 0))
                            state = change.val.state

                        # Final change when the task finishes or fails
                        elif change.name == 'info.state':
                            state = change.val

                        # Progress update change
                        elif change.name == "info.progress":
                            log.debug("progress={}%".format(change.val or 0))

                        else:
                            continue

                        if state == vim.TaskInfo.State.success:
                            # Remove task from taskList
                            taskList.remove(str(task))
                        elif state == vim.TaskInfo.State.error:
                            log.debug(task.info.error)
                            raise VirtualizationError(task.info.error.msg)
            # Move to next version
            version = update.version
    finally:
        if task_filter:
            task_filter.Destroy()

class VirtualMachineVMware(VirtualMachine):
    """
    VMware implementation of VirtualMachine class
    """

    def __init__(self, vmName, vsphereServer, vsphereUsername, vspherePassword):
        super(VirtualMachineVMware, self).__init__(vmName, vsphereServer, vsphereUsername, vspherePassword)
        self.vmType = "VMware"

        # Test the connection and make sure the VM exists
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

    def PowerOn(self):
        """
        Power On this VM
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

            self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                return

            self.log.info("Waiting for {} to turn on".format(self.vmName))
            task = vm.PowerOn()
            VMwareWaitForTasks(vsphere, [task])

    def PowerOff(self):
        """
        Power Off this VM
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
            
            self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
                return True

            self.log.info("Waiting for {} to turn off".format(self.vmName))
            task = vm.PowerOff()
            VMwareWaitForTasks(vsphere, [task])

    def GetPowerState(self):
        """
        Get the current power state of this VM

        Returns:
            A string containing 'on' or 'off' (str)
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
            self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))

            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                return "on"
            else:
                return "off"

    def GetPXEMacAddress(self):
        """
        Get the MAC address of the VM to use when PXE booting

        Returns:
            A string containing the MAC address in 00:00:00:00:00 format (str)
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'config.hardware'])
            macs = []
            for dev in vm.config.hardware.device:
                if isinstance(dev, vim.vm.device.VirtualEthernetCard):
                    macs.append(dev.macAddress)
            if not macs:
                raise SolidFireError("Could not find any ethernet devices in VM {}".format(self.vmName))

            if len(macs) == 1:
                return macs[0]
            elif len(macs) == 2:
                return macs[0]
            elif len(macs) == 4:
                return macs[2]
            return macs[0]
    
    def SetPXEBoot(self):
        """
        Set the boot order of this VM to PXE boot first
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'config.hardware'])
            disks = []
            nics = []
            for device in vm.config.hardware.device:
                if isinstance(device, vim.vm.device.VirtualDisk):
                    disks.append(device.key)
                elif isinstance(device, vim.vm.device.VirtualEthernetCard):
                    nics.append(device.key)
            boot_disk = vim.vm.BootOptions.BootableDiskDevice()
            boot_disk.deviceKey = sorted(disks)[0]
            boot_nic = vim.vm.BootOptions.BootableEthernetDevice()
            if len(nics) == 1:
                boot_nic.deviceKey = sorted(nics)[0]
            elif len(nics) == 2:
                boot_nic.deviceKey = sorted(nics)[1]
            else:
                boot_nic.deviceKey = sorted(nics)[2]
            config = vim.vm.ConfigSpec()
            config.bootOptions = vim.vm.BootOptions(bootOrder=[boot_nic, vim.vm.BootOptions.BootableCdromDevice(), boot_disk])
            task = vm.ReconfigVM_Task(config)
            VMwareWaitForTasks(vsphere, [task])

    def WaitForUp(self, timeout=300):
        """
        Wait for this VM to be powered on and the guest OS booted up
        """
        start_time = time.time()
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            # Wait for VM to be powered on
            while True:
                vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ["name", "runtime.powerState"])
                self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))
                if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                    self.log.info("VM is powered on")
                    break
                if timeout > 0 and time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for VM to power on")
                time.sleep(2)

            self.log.info("Waiting for VMware tools")
            # Wait for VMwware tools to be running
            while True:
                vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ["name", "guest.toolsRunningStatus", "guest.toolsStatus"])
                self.log.debug2("{} guest.toolsRunningStatus={}".format(self.vmName, vm.guest.toolsRunningStatus))
                if vm.guest.toolsRunningStatus == vim.VirtualMachineToolsStatus.toolsNotInstalled:
                    self.log.warning("VMware tools are not installed in this VM; cannot detect VM boot/health")
                    return
                if vm.guest.toolsStatus == vim.VirtualMachineToolsStatus.toolsOk:
                    self.log.info("VMware tools are running")
                    break
                if timeout > 0 and time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for VMware tools to start")
                time.sleep(2)

            # Wait for VM heartbeat to be green
            while True:
                vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ["name", "guestHeartbeatStatus"])
                self.log.debug2("{} guestHeartbeatStatus={}".format(self.vmName, vm.guestHeartbeatStatus))
                if vm.guestHeartbeatStatus == vim.ManagedEntityStatus.green:
                    self.log.info("VM guest heartbeat is green")
                    break
                if timeout > 0 and time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for guest heartbeat")
                time.sleep(2)

class VMHostVMware(VMHost):
    """
    VMware implementation of VMHost class
    """

    def __init__(self, vmhostName, mgmtServer, mgmtUsername, mgmtPassword):
        super(VMHostVMware, self).__init__(vmhostName, mgmtServer, mgmtUsername, mgmtPassword)
        self.hostType = "VMware"

        # Test the connection and make sure the host exists
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name"])

    def CreateDatastores(self, includeInternalDrives=False, includeSlotDrives=False):
        """
        Create/attach datastores on any volumes currently connected to this host

        Args:
            includeInternalDrives:      include disks attached to internal SATA/AHCI bus
            includeSlotDrives:          include external disks in slots
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])
            storage_manager = host.configManager.storageSystem
            datastore_manager = host.configManager.datastoreSystem

            # Go through each HBA and make a reference from LUN => datastore name
            lun2name = {}
            for adapter in storage_manager.storageDeviceInfo.hostBusAdapter:
                if adapter.driver == "nvme":
                    self.log.debug("Skipping NVRAM device")
                    continue

                elif type(adapter) == vim.host.InternetScsiHba:
                    # iSCSI, either software or HBA
                    # volumeName.volumeID naming
                    for target in [hba for hba in storage_manager.storageDeviceInfo.scsiTopology.adapter if hba.adapter == adapter.key][0].target:
                        for lun in target.lun:
                            name = ".".join(target.transport.iScsiName.split(".")[-2:])
                            if lun.lun != 0:
                                name += "-lun{}".format(lun.lun)
                            lun2name[lun.scsiLun] = name

                elif type(adapter) == vim.host.BlockHba:
                    if adapter.driver == "ahci" and includeInternalDrives:
                        # Internal SATA adapter
                        # "sdimmX" naming
                        for target in [hba for hba in storage_manager.storageDeviceInfo.scsiTopology.adapter if hba.adapter == adapter.key][0].target:
                            for lun in target.lun:
                                lun2name[lun.scsiLun] = "sdimm{}".format(target.target)

                    if (adapter.driver == "mpt2sas" or adapter.driver == "mpt3sas") and includeSlotDrives:
                        # SAS adapter for nodes
                        # "slotX" naming
                        for target in [hba for hba in storage_manager.storageDeviceInfo.scsiTopology.adapter if hba.adapter == adapter.key][0].target:
                            for lun in target.lun:
                                lun2name[lun.scsiLun] = "slot{}".format(target.target)

                else:
                    self.log.warning("Skipping unknown HBA {}".format(adapter.device))

            # Go through the list of connected LUNs and make a reference from device => datastore name
            device2name = {}
            for disk in storage_manager.storageDeviceInfo.scsiLun:
                if disk.key in lun2name:
                    device2name[disk.deviceName] = lun2name[disk.key]
                    print "{} => {}".format(disk.deviceName, lun2name[disk.key])

            # Get a list of available disks and create datastores on them
            for disk in datastore_manager.QueryAvailableDisksForVmfs():
                if disk.devicePath in device2name:
                    option_list = datastore_manager.QueryVmfsDatastoreCreateOptions(devicePath=disk.devicePath)
                    create_spec = option_list[0].spec
                    create_spec.vmfs.volumeName = device2name[disk.devicePath]
                    self.log.info("Creating datastore {}".format(device2name[disk.devicePath]))
                    datastore_manager.CreateVmfsDatastore(spec=create_spec)

# ================================================================================================================================

# ================================================================================================================================
# KVM support

class LibvirtConnect(object):
    """
    Helper for connecting to libvirt on a remote system, including adjustable timeout

    Sometimes the libvirt client hangs trying to connect to a non-libvirt system. This helper uses a
    subprocess to try to connect and kills the process if it times out.
    """

    # URI template
    CONNECTION_URI = "qemu+ssh://{}@{}/system?socket=/var/run/libvirt/libvirt-sock&no_verify=1"

    @staticmethod
    def Connect(server, username, keyfile=None, timeout=3):
        """
        Connect to libvirt on a remote server

        Args:
            server:     IP or hostname to connect to (str)
            username:   username to use for the connection (str)
            keyfile:    optional path to keyfile to use (str)
            timeout:    connection timeout (int)
        """
        _conn = LibvirtConnect(server, username, keyfile)
        if not _conn._TestConnect(timeout):
            raise _conn.exception
        return _conn._ConnectInternal()

    def __init__(self, server, username, keyfile=None):
        self.server = server
        self.username = username
        self.keyfile = keyfile
        self.log = GetLogger()
        self.exception = None

    def _TestConnect(self, timeout):
        """
        Test the connection using a subprocess so it can be terminated if it hangs

        Args:
            timeout:    timeout in seconds to wait for a successfull connection (int)

        Returns:
            True if the connection was successfull, False otherwise (bool)
        """
        self.log.debug2("Trying to connect to libvirt on {}".format(self.server))
        self.exception = None

        errq = multiprocessing.Queue()
        proc = multiprocessing.Process(target=self._ConnectInternal, args=(errq,))
        proc.name = "LibvirtConnect"
        proc.daemon = True
        proc.start()

        proc.join(timeout)
        if proc.is_alive():
            raise TimeoutError("Could not connect to libvirt on {}".format(self.server))

        try:
            self.exception = errq.get_nowait()
            return False
        except Queue.Empty:
            self.exception = None
        return True

    def _ConnectInternal(self, exceptionQueue=None):
        """
        Connect to libvirt

        Args:
            exceptionQueue:     use this queue for exceptions instead of raising them (multiprocessing.Queue)
        """
        uri = LibvirtConnect.CONNECTION_URI.format(self.username, self.server)
        if self.keyfile:
            uri += "&kefile={}".format(self.keyfile)

        conn = None
        try:
            conn = libvirt.open(uri)
        except libvirt.libvirtError as ex:
            new_ex = VirtualizationError("Could not connect: {}".format(str(ex)), ex)
            if exceptionQueue:
                exceptionQueue.put(new_ex)
            else:
                raise new_ex

        if not conn:
            ex = VirtualizationError("Failed to connect")
            if exceptionQueue:
                exceptionQueue.put(ex)
            else:
                raise ex

        return conn

def LibvirtDisconnect(connection):
    """
    Disconnect a libvirt connection

    Args:
        connection:     the libvirt connection to close (libvirt.virConnect)
    """
    connection.close()

class LibvirtConnection(object):
    """
    Connection manager for libvirt connections
    """

    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password
        self.connection = None

    def __enter__(self):
        self.connection = LibvirtConnect.Connect(self.server, self.username)
        return self.connection

    def __exit__(self, extype, value, tb):
        if self.connection:
            LibvirtDisconnect(self.connection)

def LibvirtFindVM(connection, vmName):
    """
    Find a VM with the given name

    Args:
        connection:     the libvirt connection to use (libvirt.virConnect)
        vmName:         the name of the VM to find (str)

    Returns:
        A libvirt VM (libvirt.virDomain)
    """
    log = GetLogger()
    log.debug2("Searching for a VM named {}".format(vmName))
    try:
        vm = connection.lookupByName(vmName)
    except libvirt.libvirtError as ex:
        raise VirtualizationError("Could not find VM {}: {}".format(vmName, ex.message), ex)
    if not vm:
        raise VirtualizationError("Could not find VM {}".format(vmName))
    return vm

class VirtualMachineKVM(VirtualMachine):
    """
    KVM implementation of VirtualMachine class
    """

    def __init__(self, vmName, hostServer, hostUsername, hostPassword):
        super(VirtualMachineKVM, self).__init__(vmName, hostServer, hostUsername, hostPassword)
        self.vmType = "KVM"

        # Test the connection and make sure the VM exists
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            LibvirtFindVM(conn, self.vmName)

    def PowerOn(self):
        """
        Power On this VM
        """
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            state = vm.state()
            if state[0] == libvirt.VIR_DOMAIN_RUNNING:
                return

            try:
                vm.create()
            except libvirt.libvirtError as ex:
                raise VirtualizationError("Failed to power on {}: {}".format(self.vmName, ex.message), ex)

    def PowerOff(self):
        """
        Power Off this VM
        """
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            if vm.state()[0] != libvirt.VIR_DOMAIN_RUNNING:
                return

            try:
                vm.destroy()
            except libvirt.libvirtError as ex:
                if ("domain is not running" in ex.message):
                    return
                raise VirtualizationError("Failed to power off {}: {}".format(self.vmName, ex.message), ex)

    def GetPowerState(self):
        """
        Get the current power state of this VM

        Returns:
            A string containing 'on' or 'off' (str)
        """
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            state = vm.state()
            # VIR_DOMAIN_NOSTATE        =   0   no state
            # VIR_DOMAIN_RUNNING        =   1   the domain is running
            # VIR_DOMAIN_BLOCKED        =   2   the domain is blocked on resource
            # VIR_DOMAIN_PAUSED         =   3   the domain is paused by user
            # VIR_DOMAIN_SHUTDOWN       =   4   the domain is being shut down
            # VIR_DOMAIN_SHUTOFF        =   5   the domain is shut off
            # VIR_DOMAIN_CRASHED        =   6   the domain is crashed
            # VIR_DOMAIN_PMSUSPENDED    =   7   the domain is suspended by guest power management
            if state[0] == libvirt.VIR_DOMAIN_RUNNING:
                return "on"
            else:
                return "off"

    def GetPXEMacAddress(self):
        """
        Get the MAC address of the VM to use when PXE booting

        Returns:
            A string containing the MAC address in 00:00:00:00:00 format (str)
        """
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
            mac_list = []
            for node in vm_xml.findall("devices/interface/mac"):
                mac_list.append(node.get("address"))
            if not mac_list:
                raise VirtualizationError("Could not find any ethernet devices in VM {}".format(self.vmName))

            if len(mac_list) == 1:
                return mac_list[0]
            elif len(mac_list) == 2:
                return mac_list[0]
            elif len(mac_list) == 4:
                return mac_list[2]
            return mac_list[0]

    def SetPXEBoot(self):
        """
        Set the boot order of this VM to PXE boot first
        """
        return

        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            vm_xml = ElementTree.fromstring(vm.XMLDesc(0))

            # Remove all of the existing boot entries but keep them in a local list
            boot_entries = []
            os_el = vm_xml.find("os")
            for boot_el in vm_xml.findall("os/boot"):
                boot_entries.append(boot_el)
                os_el.remove(boot_el)

            # Find the PXE entry in the list and remove it, then add it to the beginning of the list
            pxe_entry = None
            for idx, entry in enumerate(boot_entries):
                if entry.get("dev") == "pxe":
                    pxe_entry = boot_entries.pop(idx)
                    break
            if not pxe_entry:
                pxe_entry = ElementTree.Element("boot")
                pxe_entry.set("dev", "pxe")
            boot_entries.insert(0, pxe_entry)

            # Add all of the boot entries back and commit the changes to the VM
            os_el.extend(boot_entries)
            try:
                conn.defineXML(ElementTree.tostring(vm_xml))
            except libvirt.libvirtError as ex:
                raise VirtualizationError("Could not update VM {}: {}".format(self.vmName, str(ex)))

    def WaitForUp(self, timeout=300):
        """
        Wait for this VM to be powered on and the guest OS booted up
        """
        start_time = time.time()
        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            # Wait for VM to be powered on
            while True:
                vm = LibvirtFindVM(conn, self.vmName)
                state = vm.state()
                if state[0] == libvirt.VIR_DOMAIN_RUNNING:
                    self.log.info("VM is powered on")
                    break
                if timeout > 0 and time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for VM to power on")
                time.sleep(2)

            # Wait for qemu agent
            self.log.info("Waiting for guest agent")
            while True:
                if timeout > 0 and time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for VM guest agent to start")
                time.sleep(1)
                try:
                    libvirt_qemu.qemuAgentCommand(vm, '{"execute":"guest-ping"}', 10, 0)
                    self.log.info("VM guest agent is running")
                    break
                except libvirt.libvirtError as ex:
                    if ex.get_error_code() == 74: # Guest agent not configured in domain XML
                        self.log.warning("Guest agent is not configured for VM; cannot detect VM boot/health")
                        return
                    elif ex.get_error_code() == 86: # Guest agent not responding
                        continue
                    else:
                        raise VirtualizationError("Failed to wait for guest agent: {}".format(str(ex)), ex)

class VMHostKVM(VMHost):
    """
    KVM implementation of VMHost class
    """

    def __init__(self, vmhostName, mgmtServer, mgmtUsername, mgmtPassword):
        super(VMHostKVM, self).__init__(vmhostName, mgmtServer, mgmtUsername, mgmtPassword)
        self.hostType = "KVM"

        self.client = SFClient(self.mgmtServer, self.mgmtUsername, self.mgmtPassword)

    def CreateDatastores(self, includeInternalDrives=False, includeSlotDrives=False):
        """
        Create filesystem on any volumes currently connected to this host
        """
        self.client.SetupVolumes()


# ================================================================================================================================
