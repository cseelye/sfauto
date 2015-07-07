#!/usr/bin/env python2.7

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

    def Execute(self, vm_name, timeout=600, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
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
                start_time = time.time()

                # Wait for VM to be powered on
                mylog.info('Waiting for the VM to be powered on')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'runtime.powerState'])
                    mylog.debug('  runtime.powerState = {}'.format(vm.runtime.powerState))
                    if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                        mylog.info("  {} is powered on".format(vm_name))
                        break
                    if timeout > 0 and time.time() - start_time > timeout:
                        mylog.error('Timeout waiting for {} to power on'.format(vm_name))
                        return False
                    time.sleep(10)

                # Wait for VMware tools to be running
                mylog.info('Waiting for VMware tools to be running')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'guest.toolsRunningStatus', 'guest.toolsStatus'])
                    mylog.debug('  toolsRunningStatus = {}'.format(vm.guest.toolsRunningStatus))
                    mylog.debug('  toolsStatus = {}'.format(vm.guest.toolsStatus))

                    if vm.guest.toolsRunningStatus == vim.VirtualMachineToolsStatus.toolsNotInstalled:
                        mylog.warning('VMware Tools are not installed in this VM; cannot detect VM boot/health')
                        return True

                    if vm.guest.toolsStatus == vim.VirtualMachineToolsStatus.toolsOk:
                        mylog.info('  Tools are running in {}'.format(vm_name))
                        break
                    if timeout > 0 and time.time() - start_time > timeout:
                        mylog.error('Timeout waiting for tools running in {}'.format(vm_name))
                        return False
                    time.sleep(5)

                # Wait for VM heartbeat to be green
                mylog.info('Waiting for VM heartbeat to go green')
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'guestHeartbeatStatus'])
                    mylog.debug('  guestHeartbeatStatus = {}'.format(vm.guestHeartbeatStatus))

                    if vm.guestHeartbeatStatus == vim.ManagedEntityStatus.green:
                        mylog.info('  {} guest heartbeat status is green'.format(vm_name))
                        break
                    if timeout > 0 and time.time() - start_time > timeout:
                        mylog.error('Timeout waiting for guest heatbeat in {}'.format(vm_name))
                        return False
                    time.sleep(2)

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
    parser.add_option("-t", "--timeout", type="int", dest="timeout", default=600, help="how long to wait before giving up (seconds) [%default]")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, timeout=options.timeout, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, bash=options.bash, csv=options.csv, debug=options.debug):
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

