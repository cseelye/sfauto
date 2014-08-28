import libsf
from libsf import mylog
from pyVim import connect
# pylint: disable-msg=E0611
from pyVmomi import vim, vmodl
# pylint: enable-msg=E0611
import requests.exceptions

class VmwareError(Exception):
    def __init__(self, message, ex=None):
        self.message = message
        self.ex = ex
    def __str__(self):
        return self.message

class VsphereConnection(object):
    def __init__(self, server, username, password):
        self.server = server
        self.username = username
        self.password = password

    def __enter__(self):
        try:
            self.service = connect.SmartConnect(host=self.server, user=self.username, pwd=self.password)
        except vmodl.MethodFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except vim.fault.InvalidLogin:
            raise VmwareError("Invalid credentials")
        except vim.fault.HostConnectFault as e:
            raise VmwareError("Could not connect: " + str(e), e)
        except requests.exceptions.ConnectionError as e:
            raise VmwareError("Could not connect: " + str(e), e)
        return self.service

    def __exit__(self, type, value, tb):
        mylog.debug("Disconnecting from vSphere " + self.server)
        connect.Disconnect(self.service)

def FindVM(connection, vm_name, properties=["name"]):
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

    result_list = []
    view = connection.content.viewManager.CreateContainerView(container=connection.content.rootFolder, type=[vim.VirtualMachine], recursive=True)
    for vm in view.view:
        if vm.name in obj_name_list:
            result_list.append(vm)

    if len(result_list) < len(obj_name_list):
        raise VmwareError("Could not find all VMs")

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

def FindObjects(connection, obj_type, obj_name, properties=["name"]):
    """
    Experimental.  This form of retrieving obects gets the properties at the same time, instead of late binding and retrieving
    them when you request them (via obj.prop).  Much slower up front, but might save time overall in some scenarios

    Find an object of a given type and name, retrive the object and the specific properties

    connection: The ServiceInstance connection
    obj_type:   The vMomi type of the object(s) to find (vim.TypeName)
    obj_name:   The name(s) of the object(s) to search for.  If a simple string is given for obj_name, return a single matching object
                If a list of strings is given for obj_name, return a list of matching objects
                To return multiple matches for a single name, pass a list with a single element
    properties: If a list of strings is given for properties, only those properties are retrieved (much faster)
                If an empty list or None is given, then all properties are retrieved (very slow)

    The return value is a dict where the keys are the requested properties, plus a key called "obj" that contains a reference to the entire object
    """

    obj_name_list = []
    multiple_obj = True
    if isinstance(obj_name, basestring):
        obj_name_list.append(obj_name)
        multiple_obj = False
    else:
        try:
            obj_name_list = list(obj_name)
        except ValueError:
            obj_name_list.append(obj_name)
            multiple_obj = False

    view = connection.content.viewManager.CreateContainerView(container=connection.content.rootFolder, type=[obj_type], recursive=True)
    result_list = []
    if properties:
        if "name" not in properties:
            properties.append("name")
        prop_spec = vim.PropertyCollector.PropertySpec(all=False, pathSet=properties, type=obj_type)
    else:
        prop_spec = vim.PropertyCollector.PropertySpec(all=True, type=obj_type)
    trav_spec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False, type=vim.view.ContainerView)
    obj_spec = vim.PropertyCollector.ObjectSpec(obj=view, selectSet=[trav_spec], skip=False)
    filter_spec = vim.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec], reportMissingObjectsInResults=False)
    ret_options = vim.PropertyCollector.RetrieveOptions()
    while True:
        result = connection.content.propertyCollector.RetrievePropertiesEx(specSet=[filter_spec], options=ret_options)
        if result.objects:
            for obj_result in result.objects:
                o = {}
                o["obj"] = obj_result.obj
                for prop in obj_result.propSet:
                    o[str(prop.name)] = prop.val
                # Check that this object matches the requested name(s)
                if o["name"] in obj_name_list:
                    result_list.append(o)
        if not result.token:
            break

    if multiple_obj:
        return result_list
    else:
        return result_list[0]
