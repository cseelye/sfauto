#!/usr/bin/env python

"""
This action will wait for a VM to be booted up and VMware tools running

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vm_name            The name of the VM to wait for

"""

import sys
from optparse import OptionParser
from pyVmomi import vim
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareWaitForVmBootedAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mgmt_server" : libsf.IsValidIpv4Address,
                            "mgmt_user" : None,
                            "mgmt_pass" : None,
                            "vm_name" : None},
            args)

    def Execute(self, vm_name, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Wait for the VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        disk_count = 0
        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:

                # Wait for VM to be powered on
                mylog.info('Waiting for the VM to be powered on')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'runtime.powerState'])
                    mylog.debug('  runtime.powerState = {}'.format(vm.runtime.powerState))
                    if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                        mylog.info("  {} is powered on".format(vm_name))
                        break
                    time.sleep(10)
                
                # Wait for VMware tools to be running
                mylog.info('Waiting for VMware tools to be running')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'guest.toolsRunningStatus', 'guest.toolsStatus'])
                    mylog.debug('  toolsRunningStatus = {}'.format(vm.guest.toolsRunningStatus))
                    mylog.debug('  toolsStatus = {}'.format(vm.guest.toolsStatus))
                
                    if vm.guest.toolsRunningStatus == vim.VirtualMachineToolsStatus.toolsNotInstalled:
                        mylog.warning('VMware Tools are not installed in this VM; cannot detect VM boot/health')
                        break
                
                    if vm.guest.toolsStatus == vim.VirtualMachineToolsStatus.toolsOk:
                        mylog.info('  Tools are running in {}'.format(vm_name))
                        break
                    time.sleep(5)
                
                # Wait for VM heartbeat to be green
                mylog.info('Waiting for VM heartbeat to go green')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'guestHeartbeatStatus'])
                    mylog.debug('  guestHeartbeatStatus = {}'.format(vm.guestHeartbeatStatus))
                    
                    if vm.guestHeartbeatStatus == vim.ManagedEntityStatus.green:
                        mylog.info('  {} guest heartbeat status is green'.format(vm_name))
                        break
                    time.sleep(2)





                #mylog.info('Searching for VM {}'.format(vm_name))
                ## Search for the VM and retrieve only it's name, guest info and powerState
                #view = vsphere.content.viewManager.CreateContainerView(container=vsphere.content.rootFolder, type=[vim.VirtualMachine], recursive=True)
                #trav_spec = vim.PropertyCollector.TraversalSpec(name='tSpecName', path='view', skip=False, type=vim.view.ContainerView)
                #obj_spec = vim.PropertyCollector.ObjectSpec(obj=view, selectSet=[trav_spec], skip=False)
                #prop_spec = vim.PropertyCollector.PropertySpec(all=False, pathSet=['name', 'guest', 'guestHeartbeatStatus', 'runtime.powerState'], type=vim.VirtualMachine)
                #filter_spec = vim.PropertyCollector.FilterSpec(objectSet=[obj_spec], propSet=[prop_spec], reportMissingObjectsInResults=False)
                #ret_options = vim.PropertyCollector.RetrieveOptions()
                #
                ## Wait for VM to be powered on
                #mylog.info('Waiting for the VM to be powered on')
                #while True:
                #    result_list = []
                #    result = vsphere.content.propertyCollector.RetrievePropertiesEx(specSet=[filter_spec], options=ret_options)
                #    while True:
                #        if result.objects:
                #            for obj_result in result.objects:
                #                o = {}
                #                o['obj'] = obj_result.obj
                #                for prop in obj_result.propSet:
                #                    o[str(prop.name)] = prop.val
                #                if o['name'] == vm_name:
                #                    result_list.append(o)
                #        if not result.token:
                #            break
                #        result = vsphere.content.propertyCollector.ContinueRetrievePropertiesEx(token=result.token)
                #    if not result_list:
                #        mylog.error('Could not find VM {}'.format(vm_name))
                #        return False
                #    vm = result_list[0]
                #
                #    mylog.debug('runtime.powerState = {}'.format(vm['runtime.powerState']))
                #    if vm['runtime.powerState'] == vim.VirtualMachinePowerState.poweredOn:
                #        mylog.info("  {} is powered on".format(vm_name))
                #        break
                #
                ## Wait for VMware tools to be running
                #mylog.info('Waiting for VMware tools to be running')
                #while True:
                #    result_list = []
                #    result = vsphere.content.propertyCollector.RetrievePropertiesEx(specSet=[filter_spec], options=ret_options)
                #    while True:
                #        if result.objects:
                #            for obj_result in result.objects:
                #                o = {}
                #                o['obj'] = obj_result.obj
                #                for prop in obj_result.propSet:
                #                    o[str(prop.name)] = prop.val
                #                if o['name'] == vm_name:
                #                    result_list.append(o)
                #        if not result.token:
                #            break
                #        result = vsphere.content.propertyCollector.ContinueRetrievePropertiesEx(token=result.token)
                #    if not result_list:
                #        mylog.error('Could not find VM {}'.format(vm_name))
                #        return False
                #    vm = result_list[0]
                #    
                #    mylog.debug('toolsRunningStatus = {}'.format(vm['guest'].toolsRunningStatus))
                #    mylog.debug('toolsStatus = {}'.format(vm['guest'].toolsStatus))
                #
                #    if vm['guest'].toolsRunningStatus == vim.VirtualMachineToolsStatus.toolsNotInstalled:
                #        mylog.warning('VMware Tools are not installed in this VM; cannot detect VM boot/health')
                #        break
                #
                #    if vm['guest'].toolsStatus == vim.VirtualMachineToolsStatus.toolsOk:
                #        mylog.info('  Tools are running in {}'.format(vm_name))
                #        break
                #    time.sleep(5)
                #
                ## Wait for VM heartbeat to be green
                #mylog.info('Waiting for VM heartbeat to go green')
                #while True:
                #    result_list = []
                #    result = vsphere.content.propertyCollector.RetrievePropertiesEx(specSet=[filter_spec], options=ret_options)
                #    while True:
                #        if result.objects:
                #            for obj_result in result.objects:
                #                o = {}
                #                o['obj'] = obj_result.obj
                #                for prop in obj_result.propSet:
                #                    o[str(prop.name)] = prop.val
                #                if o['name'] == vm_name:
                #                    result_list.append(o)
                #        if not result.token:
                #            break
                #        result = vsphere.content.propertyCollector.ContinueRetrievePropertiesEx(token=result.token)
                #    if not result_list:
                #        mylog.error('Could not find VM {}'.format(vm_name))
                #        return False
                #    vm = result_list[0]
                #
                #    if vm['guest'].toolsRunningStatus == vim.VirtualMachineToolsStatus.toolsNotInstalled:
                #        mylog.warning('VMware Tools are not installed in this VM; cannot detect VM boot/health')
                #        break
                #
                #    mylog.debug('guestHeartbeatStatus = {}'.format(vm['guestHeartbeatStatus']))
                #
                #    if vm['guestHeartbeatStatus'] == vim.ManagedEntityStatus.green:
                #        mylog.info('  {} guest heartbeat status is green'.format(vm_name))
                #        break
                #
                #mylog.passed('{} is up and running'.format(vm_name))

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-s", "--mgmt_server", type="string", dest="mgmt_server", default=sfdefaults.fc_mgmt_server, help="the IP/hostname of the vSphere Server [%default]")
    parser.add_option("-m", "--mgmt_user", type="string", dest="mgmt_user", default=sfdefaults.fc_vsphere_user, help="the vsphere admin username [%default]")
    parser.add_option("-a", "--mgmt_pass", type="string", dest="mgmt_pass", default=sfdefaults.fc_vsphere_pass, help="the vsphere admin password [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to wait for")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, bash=options.bash, csv=options.csv, debug=options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

