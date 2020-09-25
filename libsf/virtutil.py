#!/usr/bin/env python2.7
"""Helpers for interacting with hypervisors and virtual machines"""

from . import sfdefaults
from . import SolidFireError, UnauthorizedError, TimeoutError, UnknownObjectError, ClientConnectionError, ConnectionError
from .logutil import GetLogger
from .sfclient import SFClient, OSType
from copy import deepcopy
import functools
import inspect
import libvirt
import libvirt_qemu
import multiprocessing
from pyVim import connect as connectVSphere
# pylint: disable=no-name-in-module
from pyVmomi import vim, vmodl
# pylint: enable=no-name-in-module
import six.moves.queue
import requests
import socket
import sys
import time
from xml.etree import ElementTree
import six

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

class VirtualizationError(SolidFireError):
    """Parent exception for virtualization errors"""
    pass

class VirtualMachine(object):
    """
    Base class for virtual machines.  Do not instantiate this class directly, use the Attach static method
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
        for key, value in self.__dict__.items():
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
    def Attach(vmName, mgmtServer, mgmtUsername, mgmtPassword):
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
    Base class for virtualization hosts (hypervisors).  Do not instantiate this class directly, use the Attach static method
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
        for key, value in self.__dict__.items():
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
    def Attach(vmhostName, mgmtServer, mgmtUsername, mgmtPassword, hint=None):
        """
        Factory method to create a VMHost of the correct sub-type

        Args:
            vmhostName:     the IP/name of the hypervisor (str)
            mgmtServer:     the server used to manage the hypervisor, or the hypervisor itself (str)
            mgmtUsername:   the username for the management server (str)
            mgmtPassword:   the password for the management server (str)
            hint:           try to connect using this hypervisor class name (str)
        """
        log = GetLogger()

        # Try each type of VMHost class until one works
        for classname, classdef in inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and \
                                                                                 issubclass(member, VMHost) and \
                                                                                 member != VMHost):
            if hint and classname != hint:
                continue
            log.debug2("Trying {} for VMHost {}".format(classname, vmhostName))
            try:
                return classdef(vmhostName, mgmtServer, mgmtUsername, mgmtPassword)
            except (VirtualizationError, TimeoutError, ConnectionError, ClientConnectionError, UnauthorizedError) as ex:
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
        raise UnknownObjectError('Could not find {}'.format(obj_name))
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
                            log.debug("Task {} [{}] progress={}%".format(task.info.descriptionId, task.info.key, change.val.progress or 0))
                            state = change.val.state

                        # Final change when the task finishes or fails
                        elif change.name == 'info.state':
                            state = change.val

                        # Progress update change
                        elif change.name == "info.progress" and change.val:
                            log.debug("Task {} [{}] progress={}%".format(task.info.descriptionId, task.info.key, change.val))
#                            log.debug("Task {} [{}] progress={}%".format(task.info.descriptionId, task.info.key, change.val or 0))

                        else:
                            continue

                        if state == vim.TaskInfo.State.success:
                            log.debug("Task {} [{}] completed".format(task.info.descriptionId, task.info.key))
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

def SetVMwareTimeout(mo, timeout):
    """
    Set the low level connection timeout for a managed object
    """
    # Override HTTPSConnectionWrapper.__init__ in pyVmomi.SoapAdapter
    # The new init calls the old init, then sets the timeout value
    from pyVmomi.SoapAdapter import HTTPSConnectionWrapper
    oldinit = HTTPSConnectionWrapper.__init__
    def newinit(self, *args, **kwargs):
        oldinit(self, *args, **kwargs)
        self._wrapped.timeout = timeout
    HTTPSConnectionWrapper.__init__ = newinit
    # Drop all connections from the pool so they are forced to reconnect with the new __init__ we just created
    mo._GetStub().DropConnections()

class VirtualMachineVMware(VirtualMachine):
    """
    VMware implementation of VirtualMachine class
    """

    def __init__(self, vmName, vsphereServer, vsphereUsername, vspherePassword):
        super(VirtualMachineVMware, self).__init__(vmName, vsphereServer, vsphereUsername, vspherePassword)
        self.vmType = "VMware"
        self.vsphereConnection = None

        # Test the connection and make sure the VM exists
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

    #pylint: disable=no-self-argument, not-callable
    def _vsphere_session(f):
        @functools.wraps(f)
        def wrapped(self, *args, **kwargs):
            if self.vsphereConnection:
                self.log.debug2("VirtualMachineVMware reusing vsphere connection")
                return f(self, *args, **kwargs)
            else:
                self.log.debug2("VirtualMachineVMware creating new vsphere connection")
                try:
                    with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as self.vsphereConnection:
                        return f(self, *args, **kwargs)
                finally:
                    self.vsphereConnection = None
        return wrapped
    #pylint: enable=no-self-argument, not-callable

    @_vsphere_session
    def PowerOn(self):
        """
        Power On this VM
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

        self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            return

        self.log.info("Waiting for {} to turn on".format(self.vmName))
        task = vm.PowerOn()
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def PowerOff(self):
        """
        Power Off this VM
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

        self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
            return True

        self.log.info("Waiting for {} to turn off".format(self.vmName))
        task = vm.PowerOff()
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def GetPowerState(self):
        """
        Get the current power state of this VM

        Returns:
            A string containing 'on' or 'off' (str)
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
        self.log.debug2("{} runtime.powerState={}".format(self.vmName, vm.runtime.powerState))

        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            return "on"
        else:
            return "off"

    @_vsphere_session
    def GetPXEMacAddress(self):
        """
        Get the MAC address of the VM to use when PXE booting

        Returns:
            A string containing the MAC address in 00:00:00:00:00 format (str)
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'config.hardware'])
        macs = []
        for dev in vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualEthernetCard):
                macs.append(dev.macAddress)
        if not macs:
            raise SolidFireError("Could not find any ethernet devices in VM {}".format(self.vmName))

        if len(macs) == 1:
            idx = 0
        elif len(macs) == 2:
            idx = 0
        else: # 4 NICs, or any other config we do not recognize
            idx = 2

        self.log.debug2("Getting MAC address from NIC {} ({})".format(idx, macs[idx]))
        return macs[idx]

    @_vsphere_session
    def SetPXEBoot(self):
        """
        Set the boot order of this VM to PXE boot first
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'config.hardware'])
        disks = []
        nics = []
        cdrom_present = False
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualDisk):
                disks.append(device.key)
            elif isinstance(device, vim.vm.device.VirtualEthernetCard):
                nics.append(device.key)
            elif isinstance(device, vim.vm.device.VirtualCdrom):
                cdrom_present = True
        nics = sorted(nics)
        boot_disk = vim.vm.BootOptions.BootableDiskDevice()
        boot_disk.deviceKey = sorted(disks)[0]
        boot_nic = vim.vm.BootOptions.BootableEthernetDevice()
        if len(nics) == 1:
            idx = 0
        elif len(nics) == 2:
            idx = 0
        else: # 4 NICs, or any other config we do not recognize
            idx = 2

        self.log.debug2("Picking NIC {} to PXE boot from ({})".format(idx, nics[idx]))
        boot_nic.deviceKey = nics[idx]

        if cdrom_present:
            boot_devices = [boot_nic, vim.vm.BootOptions.BootableCdromDevice(), boot_disk]
        else:
            boot_devices = [boot_nic, boot_disk]
        config = vim.vm.ConfigSpec()
        config.bootOptions = vim.vm.BootOptions(bootOrder=boot_devices)
        task = vm.ReconfigVM_Task(config)
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def WaitForUp(self, timeout=300):
        """
        Wait for this VM to be powered on and the guest OS booted up
        """
        start_time = time.time()
        # Wait for VM to be powered on
        while True:
            vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ["name", "runtime.powerState"])
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
            vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ["name", "guest.toolsRunningStatus", "guest.toolsStatus"])
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
            vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ["name", "guestHeartbeatStatus"])
            self.log.debug2("{} guestHeartbeatStatus={}".format(self.vmName, vm.guestHeartbeatStatus))
            if vm.guestHeartbeatStatus == vim.ManagedEntityStatus.green:
                self.log.info("VM guest heartbeat is green")
                break
            if timeout > 0 and time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for guest heartbeat")
            time.sleep(2)

    @_vsphere_session
    def Delete(self):
        """
        Delete this virtual machine
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name'])
        task = vm.Destroy_Task()
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def SetVMXProperty(self, name, value):
        """
        Set a advanced property of this VM.  These are stored as key-value pairs in the VMX file.

        Arg:
            name:      the name (key) of the property to set
            value:     the value to set
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name'])

        self.log.debug("Setting property {}={} on VM {}".format(name, value, self.vmName))
        option = vim.option.OptionValue(key=name, value=value)
        config = vim.vm.ConfigSpec()
        config.extraConfig = [option]
        task = vm.ReconfigVM_Task(config)
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def AddNetworkAdapter(self, networkName):
        """
        Add a new NIC to this VM

        Args:
            networkName:    the name of the port group to connect the NIC to (str)
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name'])
        network = VMwareFindObjectGetProperties(self.vsphereConnection, networkName, vim.Network, [])

        nic_spec = vim.vm.device.VirtualDeviceSpec()
        nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        nic_spec.device = vim.vm.device.VirtualVmxnet3()
        nic_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
        nic_spec.device.backing.network = network
        nic_spec.device.backing.deviceName = networkName
        nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        nic_spec.device.connectable.startConnected = True
        nic_spec.device.connectable.startConnected = True
        nic_spec.device.connectable.allowGuestControl = True
        nic_spec.device.connectable.connected = False
        nic_spec.device.connectable.status = 'untried'
        nic_spec.device.wakeOnLanEnabled = True
        nic_spec.device.addressType = 'assigned'

        config = vim.vm.ConfigSpec()
        config.deviceChange = [nic_spec]
        task = vm.ReconfigVM_Task(spec=config)
        VMwareWaitForTasks(self.vsphereConnection, [task])

    @_vsphere_session
    def AddDisk(self, sizeGB, datastoreName, thinProvision=True):
        """
        Add a new virtual disk to this VM

        Args:
            sizeGB:         the size of the disk, in GB (int)
            datastoreName:  the name of the datastore to put the disk in (str)
            thinProvision:  make this disk thinly provisioned
        """
        vm = VMwareFindObjectGetProperties(self.vsphereConnection, self.vmName, vim.VirtualMachine, ['name', 'config'])
        ds = VMwareFindObjectGetProperties(self.vsphereConnection, datastoreName, vim.Datastore, ["name"])
        # Find the SCSI controller and current LUNs
        controller = None
        used_luns = set([7])
        for dev in vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk):
                used_luns.add(dev.unitNumber)
            if isinstance(dev, vim.vm.device.VirtualSCSIController):
                controller = dev
        if not controller:
            raise VirtualizationError("Could not find a SCSI controller to attach the disk to")
        available_lun = None
        for lun in range(17):
            if lun not in used_luns:
                available_lun = lun
                break
        if available_lun is None:
            raise VirtualizationError("There are no free LUNs on the SCSI controller")

        self.log.debug("Adding LUN {} in datastore {}".format(available_lun, ds.name))
        disk_spec = vim.vm.device.VirtualDeviceSpec()
        disk_spec.fileOperation = "create"
        disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        disk_spec.device = vim.vm.device.VirtualDisk()
        disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        disk_spec.device.backing.thinProvisioned = thinProvision
        disk_spec.device.backing.diskMode = 'persistent'
        disk_spec.device.backing.fileName = "[{}] {}-{}.vmdk".format(ds.name, self.vmName, available_lun)
        disk_spec.device.backing.datastore = ds
        disk_spec.device.unitNumber = available_lun
        disk_spec.device.capacityInKB = sizeGB * 1024 * 1024
        disk_spec.device.controllerKey = controller.key

        config = vim.vm.ConfigSpec()
        config.deviceChange = [disk_spec]
        task = vm.ReconfigVM_Task(spec=config)
        VMwareWaitForTasks(self.vsphereConnection, [task])


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

            self.log.info("Querying connected disk devices on {}".format(self.vmhostName))

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

                #elif type(adapter) == vim.host.BlockHba:
                elif adapter.driver == "ahci" or adapter.driver == "vmw_ahci":
                    # Internal SATA adapter
                    # "sdimmX" naming
                    if not includeInternalDrives:
                        self.log.info("Skipping adapter {} for internal drives".format(adapter.device))
                        continue
                    for target in [hba for hba in storage_manager.storageDeviceInfo.scsiTopology.adapter if hba.adapter == adapter.key][0].target:
                        for lun in target.lun:
                            lun2name[lun.scsiLun] = "{}-sdimm{}".format(self.vmhostName, target.target)

                elif adapter.driver == "mpt2sas" or adapter.driver == "mpt3sas":
                    # SAS adapter for slot drives on the front of the chassis
                    # "slotX" naming
                    if not includeSlotDrives:
                        self.log.info("Skipping adapter {} for slot drives".format(adapter.device))
                        continue
                    for target in [hba for hba in storage_manager.storageDeviceInfo.scsiTopology.adapter if hba.adapter == adapter.key][0].target:
                        for lun in target.lun:
                            lun2name[lun.scsiLun] = "{}-slot{}".format(self.vmhostName, target.target)

                elif adapter.driver == "vmkusb":
                    # Skip USB drives
                    continue

                else:
                    self.log.warning("Skipping unknown HBA {}".format(adapter.device))
                    self.log.debug("adapter = {}".format(adapter))

            # Go through the list of connected LUNs and make a reference from device => datastore name
            device2name = {}
            for disk in storage_manager.storageDeviceInfo.scsiLun:
                if disk.key in lun2name:
                    device2name[disk.deviceName] = lun2name[disk.key]
                    self.log.debug("{} => {}".format(disk.deviceName, lun2name[disk.key]))

            # Get a list of available disks and create datastores on them
            available_devices = [disk.devicePath for disk in datastore_manager.QueryAvailableDisksForVmfs()]
            for device_path in sorted(device2name, key=device2name.get):
                if device_path in available_devices:
                    self.log.info("Creating datastore {}".format(device2name[device_path]))
                    option_list = datastore_manager.QueryVmfsDatastoreCreateOptions(devicePath=device_path)
                    create_spec = option_list[0].spec
                    create_spec.vmfs.volumeName = device2name[device_path]
                    datastore_manager.CreateVmfsDatastore(spec=create_spec)

    def CreateVswitch(self, switchName, nicNames, mtu=1500):
        """
        Create a vswitch on this host

        Args:
            switchName:     the name of the switch to create (str)
            nicNames:       optional list of physical uplink NICs to add to the switch (list of str)
            mtu:            the MTU of the vswitch (int)
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])
            pnics = [pnic.device for pnic in host.config.network.pnic]
            if not all([nic in pnics for nic in nicNames]):
                raise VirtualizationError("Could not find all specified NICs")

            network_manager = host.configManager.networkSystem

            switch_spec = vim.host.VirtualSwitch.Specification()
            switch_spec.mtu = mtu
            switch_spec.numPorts = 128
            switch_spec.bridge = vim.host.VirtualSwitch.BondBridge(nicDevice=nicNames)
            network_manager.AddVirtualSwitch(vswitchName=switchName, spec=switch_spec)

    def CreatePortgroup(self, portgroupName, switchName, vlan=0):
        """
        Create a port group on this host

        Args:
            portgroupName:      the name of the port group to create
            switchName:         the name of the vswitch to create the port group on
            vlan:               what VLAN to tag for this port group
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])
            vswitches = [sw.name for sw in host.config.network.vswitch]
            if switchName not in vswitches:
                raise UnknownObjectError("Could not find vswitch {}".format(switchName))
            pgroups = [pg.spec.name for pg in host.config.network.portgroup]
            if portgroupName in pgroups:
                raise VirtualizationError("Port group {} already exists".format(portgroupName))

            network_manager = host.configManager.networkSystem

            sec_policy = vim.host.NetworkPolicy.SecurityPolicy()
            sec_policy.allowPromiscuous = True
            sec_policy.forgedTransmits = True
            sec_policy.macChanges = True

            pg_spec = vim.host.PortGroup.Specification()
            pg_spec.name = portgroupName
            pg_spec.vswitchName = switchName
            pg_spec.vlanId = vlan
            pg_spec.policy = vim.host.NetworkPolicy(security=sec_policy)
            network_manager.AddPortGroup(portgrp=pg_spec)

    def DeletePortgroup(self, portgroupName):
        """
        Delete a port group on this host

        Args:
            portgroupName:      the name of the port group to delete
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])

            spec = None
            for pg in host.config.network.portgroup:
                if pg.spec.name == portgroupName:
                    spec = pg.spec
                    break
            if not spec:
                raise UnknownObjectError("Cannot find port group {}".format(portgroupName))

            config = vim.host.NetworkConfig()

            security = vim.host.NetworkPolicy.SecurityPolicy()
            security.allowPromiscuous = False if spec.policy.security.allowPromiscuous is None else spec.policy.security.allowPromiscuous
            security.forgedTransmits = True if spec.policy.security.forgedTransmits is None else spec.policy.security.forgedTransmits
            security.macChanges = True if spec.policy.security.macChanges is None else spec.policy.security.macChanges

            config.portgroup = [vim.host.PortGroup.Config()]
            config.portgroup[0].changeOperation = vim.host.ConfigChange.Operation.remove
            config.portgroup[0].spec = vim.host.PortGroup.Specification()
            config.portgroup[0].spec.name = portgroupName
            config.portgroup[0].spec.vlanId = -1
            config.portgroup[0].spec.vswitchName = spec.vswitchName
            config.portgroup[0].spec.policy = vim.host.NetworkPolicy()

            network_manager = host.configManager.networkSystem
            network_manager.UpdateNetworkConfig(config, vim.host.ConfigChange.Mode.modify)

    def RenamePortgroup(self, oldname, newname):
        """
        Rename a portgroup

        Args:
            oldname:    the port group to rename
            newname:    the new name for the port group
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])
            network_manager = host.configManager.networkSystem

            spec = None
            for pg in host.config.network.portgroup:
                if pg.spec.name == oldname:
                    spec = pg.spec
                    break
            if not spec:
                raise UnknownObjectError("Cannot find port group {}".format(oldname))

            spec.name = newname
            network_manager.UpdatePortGroup(oldname, spec)

    def SetNetworkInfo(self, nicName, ipAddress, subnetMask, mtu=1500):
        """
        Configure the network

        Args:
            nicName:    the NIC to configure (str)
            ipAddress:  the IP address to set (str)
            subnetMask: the subnet mask to set
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])

            network_manager = host.configManager.networkSystem
            spec = None
            for vnic in network_manager.networkConfig.vnic:
                if vnic.device == nicName:
                    spec = vnic.spec
                    break
            if not spec:
                raise UnknownObjectError("Could not find NIC {}".format(nicName))

            spec.ip.dhcp = False
            spec.ip.ipAddress = ipAddress
            spec.ip.subnetMask = subnetMask
            spec.mtu = mtu
            network_manager.UpdateVirtualNic(nicName, spec)

    def SetHostname(self, hostname):
        """
        Set the hostname of this host

        Args:
            hostname:   the new hostname to set
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])
            network_manager = host.configManager.networkSystem
            dns_config = network_manager.dnsConfig
            dns_config.hostName = hostname
            network_manager.UpdateDnsConfig(dns_config)

    def CreateVirtualMachine(self, vmName, cpuCount, memGB, diskGB, netLabel, datastore, guestType="ubuntu64Guest", hardwareVersion=None):
        """
        Create a virtual machine on this host

        Args:
            vmName:             the name for the new VM
            cpuCount:           the number of vCPUs for the VM
            memGB:              the memory size for the VM
            diskGB:             the size of the disk for the VM
            netLabel:           the name of the network to put the VM on
            datastore:          the datastore to put the VM in
            guestType:          the VM guest type
            hardwareVersion:    the virtual hardware version to create the VM with

        Returns:
            The new virtual machine (VirtualMachine)
        """
        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vsphere:
            host = VMwareFindObjectGetProperties(vsphere, self.vmhostName, vim.HostSystem, ["name", "configManager"])

            # Find the datacenter for this host
            dc = host
            while type(dc) != vim.Datacenter:
                dc = dc.parent

            # Create the NIC spec for this VM
            network = VMwareFindObjectGetProperties(vsphere, netLabel, vim.Network, [])
            nic_spec = vim.vm.device.VirtualDeviceSpec()
            nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
            nic_spec.device = vim.vm.device.VirtualVmxnet3()
            nic_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
            nic_spec.device.backing.network = network
            nic_spec.device.backing.deviceName = netLabel
            nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
            nic_spec.device.connectable.startConnected = True
            nic_spec.device.connectable.startConnected = True
            nic_spec.device.connectable.allowGuestControl = True
            nic_spec.device.connectable.connected = False
            nic_spec.device.connectable.status = 'untried'
            nic_spec.device.wakeOnLanEnabled = True
            nic_spec.device.addressType = 'assigned'

            # Create the scsi controller spec for this VM
            controller_spec = vim.vm.device.VirtualDeviceSpec()
            controller_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
            controller_spec.device = vim.vm.device.VirtualLsiLogicController()
            controller_spec.device.sharedBus = vim.vm.device.VirtualSCSIController.Sharing.noSharing

            # Configure the VM
            config = vim.vm.ConfigSpec()
            config.name = vmName
            config.numCPUs = int(cpuCount)
            config.memoryMB = int(memGB * 1024)
            config.guestId = guestType
            config.version = "vmx-{}".format(hardwareVersion)
            config.files = vim.vm.FileInfo(vmPathName="[{}] {}".format(datastore, vmName))
            config.deviceChange = [nic_spec, controller_spec]

            # Create the VM
            task = dc.vmFolder.CreateVM_Task(config=config,
                                             pool=host.parent.resourcePool)
            VMwareWaitForTasks(vsphere, [task])

            # Get a reference to the created VM
            vm = VMwareFindObjectGetProperties(vsphere, vmName, vim.VirtualMachine, ["config"])

            try:
                # Create disk and CDROM spec to add to the VM
                ds = VMwareFindObjectGetProperties(vsphere, datastore, vim.Datastore, [])
                disk_spec = vim.vm.device.VirtualDeviceSpec()
                disk_spec.fileOperation = "create"
                disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
                disk_spec.device = vim.vm.device.VirtualDisk()
                disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                disk_spec.device.backing.thinProvisioned = True
                disk_spec.device.backing.diskMode = 'persistent'
                disk_spec.device.backing.datastore = ds
                disk_spec.device.unitNumber = 0
                disk_spec.device.capacityInKB = int(diskGB * 1024 * 1024)

                cdrom_spec = vim.vm.device.VirtualDeviceSpec()
                cdrom_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
                cdrom_spec.device = vim.vm.device.VirtualCdrom()
                cdrom_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
                cdrom_spec.device.connectable.allowGuestControl = True
                cdrom_spec.device.connectable.startConnected = True
                cdrom_spec.device.backing = vim.vm.device.VirtualCdrom.RemotePassthroughBackingInfo()
                cdrom_spec.device.backing.deviceName = ""
                cdrom_spec.device.backing.exclusive = False

                # Find the SCSI and IDE controllers
                disk_spec.device.controllerKey = None
                cdrom_spec.device.controllerKey = None
                for dev in vm.config.hardware.device:
                    if not disk_spec.device.controllerKey and isinstance(dev, vim.vm.device.VirtualSCSIController):
                        disk_spec.device.controllerKey = dev.key
                        self.log.debug2("Adding disk to controller {}".format(dev.key))
                    if not cdrom_spec.device.controllerKey and isinstance(dev, vim.vm.device.VirtualIDEController) and len(dev.device) < 2:
                        cdrom_spec.device.controllerKey = dev.key
                        self.log.debug2("Adding cdrom to controller {}".format(dev.key))

                # Add the disk and CDROM to the VM
                config = vim.vm.ConfigSpec()
                config.deviceChange = [disk_spec, cdrom_spec]
                task = vm.ReconfigVM_Task(spec=config)
                VMwareWaitForTasks(vsphere, [task])
            except:
                # Clean up the VM
                self.log.error("Exception, removing incomplete VM")
                try:
                    task = vm.Destroy_Task()
                    VMwareWaitForTasks(vsphere, [task])
                except (SolidFireError, vmodl.MethodFault, vmodl.RuntimeFault):
                    # Cleanup is best effort, ignore any errors that happen here
                    pass
                # Now raise the original exception
                raise
            return VirtualMachineVMware(vmName, self.mgmtServer, self.mgmtUsername, self.mgmtPassword)

    def CreateVMNode(self, vmName, mgmtNet, storageNet, datastores, cpuCount=6, memGB=24, sliceDriveSize=60, blockDriveSize=50, blockDriveCount=10):
        """
        Create a virtual SF node

        Args:
            vmName:             the name for the new VM (str)
            mgmtNet:            the name of the network to put the 1G NICs on (str)
            storageNet:         the name of the network to put the 10G NICs on (str)
            datastores:         the list of datastores to put the VM disks in (list of str)
            cpuCount:           the number of vCPUs for the VM (int)
            memGB:              the memory size for the VM (int)
            sliceDriveSize:     the size of the slice drive in GB (int)
            blockDriveSize:     the size of the block drive in GB (int)
            blockDriveCount:    the number of block drives (int)

        Returns:
            The new virtual node (VirtualMachine)
        """
        dest_datastores = deepcopy(datastores)
        if len(datastores) > 1:
            vm_datastore = dest_datastores.pop(0)
        else:
            vm_datastore = dest_datastores[0]
        self.log.info("Creating base VM")
        # Base VM disk is 100MB boot + 32GB root + 8GB var/log + 8GB NVRAM + slice size
        vm = self.CreateVirtualMachine(vmName, cpuCount, memGB, 48.1 + sliceDriveSize, storageNet, vm_datastore, "ubuntu64Guest", 10)

        with VMwareConnection(self.mgmtServer, self.mgmtUsername, self.mgmtPassword) as vm.vsphereConnection:
            try:
                self.log.info("Enabling disk UUIDs")
                vm.SetVMXProperty("disk.EnableUUID", "true")

                self.log.info("Adding network adapters")
                vm.AddNetworkAdapter(storageNet)
                vm.AddNetworkAdapter(mgmtNet)
                vm.AddNetworkAdapter(mgmtNet)

                self.log.info("Adding block drives")
                for disk_idx in range(blockDriveCount):
                    vm.AddDisk(blockDriveSize, dest_datastores[disk_idx % len(dest_datastores)])
            except:
                # Clean up the VM
                self.log.error("Exception, removing incomplete VM")
                try:
                    vm.Delete()
                except (SolidFireError, vmodl.MethodFault, vmodl.RuntimeFault):
                    # Cleanup is best effort, ignore any errors that happen here
                    pass

                # Now raise the original exception
                raise

        return vm

    def CreateVMNodeManagement(self, vmName, mgmtNet, datastore):
        """
        Create a virtual SF management node

        Args:
            vmName:             the name for the new VM (str)
            mgmtNet:            the name of the network to put the NIC on (str)
            datastore:          the datastore to put the VM in (str)

        Returns:
            The new virtual node (VirtualMachine)
        """
        CPU_COUNT = 1
        MEM_SIZE = 8
        DISK_SIZE = 150
        return self.CreateVirtualMachine(vmName, CPU_COUNT, MEM_SIZE, DISK_SIZE, mgmtNet, datastore, "ubuntu64Guest", 10)

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
        except six.moves.queue.Empty:
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
        raise UnknownObjectError("Could not find VM {}".format(vmName))
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

        log = GetLogger()

        with LibvirtConnection(self.hostServer, self.hostUsername, self.hostPassword) as conn:
            vm = LibvirtFindVM(conn, self.vmName)
            vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
            parent_map = {c:p for p in vm_xml.iter() for c in p}

            # Remove any entries from the os/boot path (legacy)
            for boot_el in vm_xml.findall("os/boot"):
                parent_map[boot_el].remove(boot_el)

            # Remove boot entries from all devices
            for boot_el in vm_xml.findall("devices/*/boot"):
                parent_map[boot_el].remove(boot_el)

            # Find the appropriate NIC and set it to boot order 1
            boot_el = ElementTree.Element("boot")
            boot_el.set("order", "1")
            interfaces = vm_xml.findall("devices/interface")
            if len(interfaces) <= 0:
                raise VirtualizationError("Could not find a NIC to PXE boot from")
            if len(interfaces) == 1:
                nic_order = (0)
            elif len(interfaces) == 2:
                nic_order = (1)
            elif len(interfaces) == 4:
                nic_order = (2, 3)
            else:
                nic_order = range(len(interfaces))

            boot_order = 1
            for idx in nic_order:
                boot_el = ElementTree.Element("boot")
                boot_el.set("order", str(boot_order))
                interfaces[idx].append(boot_el)
                boot_order += 1

            # Find the first disk and set the boot order to follow the NICs
            boot_el = ElementTree.Element("boot")
            boot_el.set("order", str(boot_order))
            disks = vm_xml.findall("devices/disk")
            disks[0].append(boot_el)

            log.debug("Setting boot order to [NIC {}, {}]".format(interfaces[2].find("mac").get("address"), disks[0].find("target").get("dev")))

            # Commit the changes to the VM
            try:
                conn.defineXML(ElementTree.tostring(vm_xml))
            except libvirt.libvirtError as ex:
                raise VirtualizationError("Could not update VM {}: {}".format(self.vmName, str(ex)))
        #pylint: enable=unreachable

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

        self.client = SFClient(self.mgmtServer, self.mgmtUsername, self.mgmtPassword, OSType.Linux)

    def CreateDatastores(self, includeInternalDrives=False, includeSlotDrives=False):
        """
        Create filesystem on any volumes currently connected to this host
        """
        self.client.SetupVolumes()

    def SetHostname(self, hostname):
        """
        Set the hostname of this host

        Args:
            hostname:   the new hostname to set
        """
        self.client.UpdateHostname(hostname)


# ================================================================================================================================
