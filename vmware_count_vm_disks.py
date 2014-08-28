#!/usr/bin/env python

"""
This action will count the number of virtual disks attached to a VM

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vm_name            The name of the VM to check

"""

import sys
from optparse import OptionParser
from pyVmomi import vim

import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareCountVmDisksAction(ActionBase):
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

    def Get(self, vm_name, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Set the multipath policy
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
                mylog.info("Searching for VM " + vm_name)
                vm = libvmware.FindVM(vsphere, vm_name)

                for dev in vm.config.hardware.device:
                    if type(dev) == vim.vm.device.VirtualDisk:
                        disk_count += 1

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        return disk_count

    def Execute(self, vm_name, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Set the multipath policy
        """
        del self
        disk_count = Get(**locals())
        if disk_count is False:
            return False

        if csv or bash:
            sys.stdout.write(str(disk_count))
            sys.stdout.flush()
        else:
            mylog.info("{} has {} virtual disks".format(vm_name, disk_count))

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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to check")
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
