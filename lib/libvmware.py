import libsf
from libsf import mylog
from pyVim import connect
# pylint: disable-msg=E0611
from pyVmomi import vim, vmodl
# pylint: enable-msg=E0611
import requests.exceptions
import threading
import time

try:
    import requests
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass

class VmwareError(Exception):
    def __init__(self, message, ex=None):
        self.message = message
        self.ex = ex
    def __str__(self):
        return self.message

class DynamicObject(object):

    def __getitem__(self, name):
        if name in dir(self):
            return getattr(self, name)
        else:
            raise KeyError()

    def __str__(self):
        return self._format().strip()

    def _format(self, indent=0):
        output = ''
        for name in dir(self):
            if name.startswith('_'):
                continue
            value = getattr(self, name)
            if type(value) == DynamicObject:
                output += '{}\n'.format(name)
                output += value._format(indent+1)
            else:
                output += ' '* indent + '{} = {} ({})\n'.format(name, value, type(value))
        return output

class VsphereConnection(object):
    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password

    def __enter__(self):
        try:
            self.service = connect.SmartConnect(host=self.server, user=self.username, pwd=self.password)
        except vim.fault.InvalidLogin:
            raise VmwareError("Invalid credentials")
        except vim.fault.HostConnectFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except requests.exceptions.ConnectionError as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except vmodl.MethodFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        return self.service

    def __exit__(self, type, value, tb):
        mylog.debug("Disconnecting from vSphere " + self.server)
        connect.Disconnect(self.service)

def FindObjectGetProperties(connection, obj_name, obj_type, properties=None, parent=None):
    '''Search for an object in vSphere and retrieve a list of properties
    Arguments:
        connection:     the vsphere connection to use
        obj_name:       the name of the object to find
        obj_type:       the type of the object (ex vim.VirtualMachine)
        properties:     the properties of the object to retrieve.  Set to None to return all properties, but this is very slow!
        parent:         the parent object to start the search, such as a datacenter or host.  Set to None to search the entire vCenter
    Returns:
        a DynamicObject with the requested properties
    '''

    mylog.debug('Searching for a {} named "{}"'.format(obj_type.__name__, obj_name))
    parent = parent or connection.content.rootFolder
    if not properties:
        prop_spec = vim.PropertyCollector.PropertySpec(all=True, type=obj_type)
    else:
        if 'name' not in properties:
            prop_list.append('name')
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

def AddPropertyToObj(obj, prop_name, prop_val):
    pieces = prop_name.split('.')
    o = obj
    while len(pieces) > 1:
        attr = pieces.pop(0)
        if not getattr(o, attr, None):
            setattr(o, attr, DynamicObject())
        o = getattr(o, attr)
    setattr(o, pieces[0], prop_val)

def FindVM(connection, vm_name, parent=None):
    obj_name_list = []
    multiple_obj = True
    if isinstance(vm_name, basestring):
        obj_name_list.append(vm_name)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(vm_name)
        except ValueError:
            obj_name_list.append(vm_name)
            multiple_obj = False
    
    if not parent:
        parent = connection.content.rootFolder
    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=parent, type=[vim.VirtualMachine], recursive=True)
    #mylog.debug('Found {} total VMs'.format(len(view.view)))
    for vm in view.view:
        try:
            name = vm.name
        except vmodl.fault.ManagedObjectNotFound:
            # VM was deleted while we were querying it
            continue
        if name in obj_name_list:
            result_list.append(vm)
    
    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find all VMs")
    
    if multiple_obj:
        return result_list
    else:
        return result_list[0]

def FindNetwork(connection, net_name, parent=None):
    obj_name_list = []
    multiple_obj = True
    if isinstance(net_name, basestring):
        obj_name_list.append(net_name)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(net_name)
        except ValueError:
            obj_name_list.append(net_name)
            multiple_obj = False

    if not parent:
        parent = connection.content.rootFolder
    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=parent, type=[vim.Network], recursive=True)
    for o in view.view:
        if o.name in obj_name_list:
            result_list.append(o)

    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find network")

    if multiple_obj:
        return result_list
    else:
        return result_list[0]

def FindHost(connection, host_ip):
    obj_name_list = []
    multiple_obj = True
    if isinstance(host_ip, basestring):
        obj_name_list.append(host_ip)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(host_ip)
        except ValueError:
            obj_name_list.append(host_ip)
            multiple_obj = False

    result_list = []
    for hip in obj_name_list:
        search_index = connection.content.searchIndex
        host = search_index.FindByIp(ip=hip, vmSearch=False)
        if host:
            result_list.append(host)

    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find all hosts")

    if multiple_obj:
        return result_list
    else:
        return result_list[0]


def FindClusterHostIsIn(host):
    '''Get the cluster that a host is in'''
    if type(host.parent) == vim.ClusterComputeResource:
        return host.parent
    return None

def FindDatacenterHostIsIn(host):
    '''Find the datacenter this host is in'''
    upper = host
    while type(upper.parent) != vim.Datacenter:
        upper = upper.parent
    return upper.parent

def FindDatastore(connection, ds_name, parent=None):
    obj_name_list = []
    multiple_obj = True
    if isinstance(ds_name, basestring):
        obj_name_list.append(ds_name)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(ds_name)
        except ValueError:
            obj_name_list.append(ds_name)
            multiple_obj = False

    if not parent:
        parent = connection.content.rootFolder
    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=parent, type=[vim.Datastore], recursive=True)
    for o in view.view:
        if o.name in obj_name_list:
            result_list.append(o)

    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find all datastores")

    if multiple_obj:
        return result_list
    else:
        return result_list[0]

def FindResourcePool(connection, pool_name, parent=None):
    obj_name_list = []
    multiple_obj = True
    if isinstance(pool_name, basestring):
        obj_name_list.append(pool_name)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(pool_name)
        except ValueError:
            obj_name_list.append(pool_name)
            multiple_obj = False

    if not parent:
        parent = connection.content.rootFolder
    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=parent, type=[vim.ResourcePool], recursive=True)
    for o in view.view:
        if o.name in obj_name_list:
            result_list.append(o)

    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find all resource pools")

    if multiple_obj:
        return result_list
    else:
        return result_list[0]

def ShutdownVMs(vsphere, vm_list):
    threads = []
    results = {}
    for vm in vm_list:
        thread_name = "shutdown-" + vm.name
        results[thread_name] = False
        t = threading.Thread(target=_ShutdownVMThread, name=thread_name, args=(vsphere, vm, results))
        t.daemon = True
        threads.append(t)

    allgood = libsf.ThreadRunner(threads, results, len(threads))
    if not allgood:
        raise VmwareError("Not all VMs could be shutdown")

def _ShutdownVMThread(vsphere, vm, results):
    myname = threading.current_thread().name
    results[myname] = False
    mylog.info("  Shutting down VM " + vm.name + " on " + vm.runtime.host.name)
    try:
        if vm.guest.toolsRunningStatus != "guestToolsRunning":
            task = vm.PowerOffVM_Task()
            WaitForTasks(vsphere, [task])
        else:
            vm.ShutdownGuest()
            start_time = time.time()
            while vm.runtime.powerState == "poweredOn":
                time.sleep(5)
                if time.time() - start_time > 300:
                    mylog.warning("  " + vm.name + " failed to shut down in 5 minutes")
                    task = vm.PowerOffVM_Task()
                    WaitForTasks(vsphere, [task])
        results[myname] = True

    except vmodl.MethodFault as e:
        mylog.error("  Failed shutting down VM " + vm.name + ": " + str(e))
        return

def PoweronVMs(vsphere, vm_list):
    threads = []
    results = {}
    for vm in vm_list:
        thread_name = "poweron-" + vm.name
        results[thread_name] = False
        t = threading.Thread(target=_PoweronVMThread, name=thread_name, args=(vsphere, vm, results))
        t.daemon = True
        threads.append(t)

    allgood = libsf.ThreadRunner(threads, results, len(threads))
    if not allgood:
        raise VmwareError("Not all VMs could be brought up")

def _PoweronVMThread(vsphere, vm, results):
    myname = threading.current_thread().name
    results[myname] = False
    mylog.info("  Powering on VM " + vm.name)
    try:
        task = vm.PowerOnVM_Task()
        WaitForTasks(vsphere, [task])
    except vmodl.MethodFault as e:
        mylog.error("  Failed powering on VM " + vm.name + ": " + str(e))
        return

    if vm.guest.toolsStatus == "toolsNotInstalled":
        mylog.warning("  Cannot wait for VM " + vm.name + " to come up because VMware Tools are not installed")
    else:
        mylog.info("  Waiting for VM " + vm.name + " to come up")
        while vm.guest.toolsRunningStatus != "guestToolsRunning":
            time.sleep(1)
        while vm.guestHeartbeatStatus != vim.ManagedEntity.Status.green:
            time.sleep(1)

    results[myname] = True


def WaitForTasks(si, tasks):
   """
   Given the service instance si and tasks, it returns after all the
   tasks are complete
   """

   pc = si.content.propertyCollector

   taskList = [str(task) for task in tasks]

   # Create filter
   objSpecs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                                                            for task in tasks]
   propSpec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task,
                                                         pathSet=[], all=True)
   filterSpec = vmodl.query.PropertyCollector.FilterSpec()
   filterSpec.objectSet = objSpecs
   filterSpec.propSet = [propSpec]
   filter = pc.CreateFilter(filterSpec, True)

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
      if filter:
         filter.Destroy()
