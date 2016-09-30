#!/usr/bin/env python2.7
"""Helpers for interacting with hypervisors and virtual machines"""

from pyVim import connect as connectVSphere
# pylint: disable=no-name-in-module
from pyVmomi import vim, vmodl
# pylint: enable=no-name-in-module
import requests
from . import sfdefaults
from . import SolidFireError, UnauthorizedError
from .logutil import GetLogger

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


def VMwareConnect(server, username, password):
    try:
        service = connectVSphere.SmartConnect(host=server, user=username, pwd=password)
    except vim.fault.InvalidLogin:
        raise UnauthorizedError.IPContext(ip=server)
    except vim.fault.HostConnectFault as e:
        raise SolidFireError("Could not connect: " + str(e), e)
    except requests.exceptions.ConnectionError as e:
        raise SolidFireError("Could not connect: " + str(e), e)
    except vmodl.MethodFault as e:
        raise SolidFireError("Could not connect: " + str(e), e)
    return service

def VMwareDisconnect(connection):
    connectVSphere.Disconnect(connection)

class VMwareConnection(object):
    """Context manager for vSphere connections"""

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
    """Search for an object in vSphere and retrieve a list of properties
    Arguments:
        connection:     the vsphere connection to use
        obj_name:       the name of the object to find
        obj_type:       the type of the object (ex vim.VirtualMachine)
        properties:     the properties of the object to retrieve.  Set to None to return all properties, but this is very slow!
        parent:         the parent object to start the search, such as a datacenter or host.  Set to None to search the entire vCenter
    Returns:
        The requested vim object with only the requested properties
    """
    log = GetLogger()

    log.debug2('Searching for a {} named "{}"'.format(obj_type.__name__, obj_name))
    if obj_type == vim.VirtualMachine and obj_name in sfdefaults.blacklisted_vm_names:
        raise VmwareError('{} is a reserved name and cannot be used here'.format(obj_name))

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
        raise VmwareError('Could not find {}'.format(obj_name))
    return result_list[0]

def VMwareWaitForTasks(connection, tasks):
    """
    Wait for all outstanding tasks to complete
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

class VMwareVM(object):
    
    def __init__(self, vmName, vsphereServer, vsphereUsername, vspherePassword):
        self.vmName = vmName
        self.vsphereServer = vsphereServer
        self.vsphereUsername = vsphereUsername
        self.vspherePassword = vspherePassword
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
        with VMwareConnection(self.vsphereServer, self.vsphereUsername, self.vspherePassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
            
            self.log.info("Waiting for {} to turn on".format(self.vmName))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                return True

            task = vm.PowerOn()
            VMwareWaitForTasks(vsphere, [task])

    def PowerOff(self):
        with VMwareConnection(self.vsphereServer, self.vsphereUsername, self.vspherePassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])
            
            self.log.info("Waiting for {} to turn off".format(self.vmName))
            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
                return True

            task = vm.PowerOff()
            VMwareWaitForTasks(vsphere, [task])

    def GetPowerState(self):
        with VMwareConnection(self.vsphereServer, self.vsphereUsername, self.vspherePassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name', 'runtime.powerState'])

            if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                return "on"
            else:
                return "off"

    def GetPXEMacAddress(self):
        with VMwareConnection(self.vsphereServer, self.vsphereUsername, self.vspherePassword) as vsphere:
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
        with VMwareConnection(self.vsphereServer, self.vsphereUsername, self.vspherePassword) as vsphere:
            vm = VMwareFindObjectGetProperties(vsphere, self.vmName, vim.VirtualMachine, ['name'])
            boot_config = vim.option.OptionValue(key='bios.bootDeviceClasses', value='allow:net,cd,hd')
            config = vim.vm.ConfigSpec()
            config.extraConfig = [boot_config]
            task = vm.ReconfigVM_Task(config)
            VMwareWaitForTasks(vsphere, [task])








