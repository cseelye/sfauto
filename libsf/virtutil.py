#!/usr/bin/env python2.7
"""Helpers for interacting with hypervisors and virtual machines"""

from . import sfdefaults
from . import SolidFireError, UnauthorizedError, TimeoutError
from .logutil import GetLogger
import libvirt
import multiprocessing
from pyVim import connect as connectVSphere
# pylint: disable=no-name-in-module
from pyVmomi import vim, vmodl
# pylint: enable=no-name-in-module
import Queue
import requests
import sys
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

    def __str__(self):
        return "{{{} on {} ({})}}".format(self.vmName, self.vmType, self.hostServer)

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
        GetLogger().info("Connecting to hypervisor")
        for classname in ["VirtualMachineVMware", "VirtualMachineKVM"]:
            classdef = getattr(sys.modules[__name__], classname)
            log.debug2("Trying {} for VM {}".format(classname, vmName))
            try:
                return classdef(vmName, mgmtServer, mgmtUsername, mgmtPassword)
            except (VirtualizationError, TimeoutError) as ex:
                log.debug2(str(ex))

        raise VirtualizationError("Could not create VM object; check connection to management server and VM exists on server")

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
        obj_type:       the type of the object, ex vim.VirtualMachine (str)
        properties:     the properties of the object to retrieve.  Set to None to return all properties, but this is very slow! (list of str)
        parent:         the parent object to start the search, such as a datacenter or host.  Set to None to search the entire vCenter (vim.Something)

    Returns:
        The requested vim object with only the requested properties
    """
    log = GetLogger()

    log.debug2('Searching for a {} named "{}"'.format(obj_type.__name__, obj_name))
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
            update = pc.WaitForUpdates(version)
            for filterSet in update.filterSet:
                for objSet in filterSet.objectSet:
                    task = objSet.obj
                    for change in objSet.changeSet:
                        if change.name == 'info':
                            state = change.val.state
                        elif change.name == 'info.state':
                            state = change.val
                        else:
                            continue

                        if not str(task) in taskList:
                            continue

                        if state == vim.TaskInfo.State.success:
                            # Remove task from taskList
                            taskList.remove(str(task))
                        elif state == vim.TaskInfo.State.error:
                            raise task.info.error
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
            
            self.log.info("Waiting for {} to turn on".format(self.vmName))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                return True

            task = vm.PowerOn()
            VMwareWaitForTasks(vsphere, [task])

    def PowerOff(self):
        """
        Power Off this VM
        """
        with VMwareConnection(self.hostServer, self.hostUsername, self.hostPassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
            
            self.log.info("Waiting for {} to turn off".format(self.vmName))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
                return True

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
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name'])
            boot_config = vim.option.OptionValue(key='bios.bootDeviceClasses', value='allow:net,cd,hd')
            config = vim.vm.ConfigSpec()
            config.extraConfig = [boot_config]
            task = vm.ReconfigVM_Task(config)
            VMwareWaitForTasks(vsphere, [task])

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
            try:
                vm.destroy()
            except libvirt.libvirtError as ex:
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
            if state == libvirt.VIR_DOMAIN_RUNNING:
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

# ================================================================================================================================

