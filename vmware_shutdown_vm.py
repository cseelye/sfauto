#!/usr/bin/env python

"""
This action will power off a VM

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vm_name            The name of the VM to power off
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

class VmwareShutdownVmAction(ActionBase):
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

    def Execute(self, vm_name, wait=True, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Power off the VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        mylog.info('Connecting to vSphere {}'.format(mgmt_server))
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                mylog.info("Searching for VM " + vm_name)
                vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'runtime.powerState'])

                if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
                    mylog.passed('{} is already off'.format(vm_name))
                    return True

                mylog.info('Shutting down {}'.format(vm_name))
                task = vm.ShutdownGuest()
                
                if not wait:
                    mylog.passed('Successfully sent {} the shutdown command')
                    return True

                mylog.info('Waiting for {} to finish shutdown'.format(vm_name))
                timeout = 300
                start_time = time.time()
                while True:
                    vm = libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name', 'runtime.powerState'])
                    mylog.debug('{} runtime.powerstate = {}'.format(vm_name, vm.runtime.powerState))
                    if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
                        mylog.passed('{} is powered off'.format(vm_name))
                        return True
                    if start_time - time.time() > timeout:
                        mylog.error('Timeout waiting for {} to power off'.format(vm_name))
                        break
                    time.sleep(10)
                mylog.warning('Powering off {}'.format(vm_name))
                task = vm.PowerOff()
                libvmware.WaitForTasks(vsphere, [task])
                mylog.info('Successfully powered off {}'.format(vm_name))

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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to power off")
    parser.add_option("--nowait", action="store_false", dest="wait", default=True, help="do not wait for the VM to shutdown")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, wait=options.wait, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, bash=options.bash, csv=options.csv, debug=options.debug):
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
