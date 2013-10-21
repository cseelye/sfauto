#!/usr/bin/python

"""
This action will delete a VM

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_name           The name of the VM to delete

"""

import sys
from optparse import OptionParser
import logging
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenDeleteVmAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None,
                            "vm_name" : None,
                            },
            args)

    def Execute(self, vm_name, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Clone a VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the VM
        mylog.info("Searching for VM")
        vm_ref = None
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find VM '" + vm_name + "' - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if not vm_ref or len(vm_ref) <= 0:
            mylog.error("Could not find VM '" + vm_name + "'")
            self.RaiseFailureEvent(message="Could not find VM '" + vm_name + "'")
            return False
        vm_ref = vm_ref[0]
        try:
            vm = session.xenapi.VM.get_record(vm_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not get VM record - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find all of the VM's disks
        all_vbd_ref_list = vm['VBDs']
        disk_vbd_ref_list = dict()
        vdi_list = dict()
        for vbd_ref in all_vbd_ref_list:
            vbd = session.xenapi.VBD.get_record(vbd_ref)
            if vbd['type'].lower() == 'disk':
                disk_vbd_ref_list[vbd_ref] = vbd
                vdi_ref = vbd['VDI']
                vdi = session.xenapi.VDI.get_record(vdi_ref)
                vdi_list[vdi_ref] = vdi

        # Delete the VM object
        mylog.info("Deleting VM " + vm['name_label'])
        try:
            session.xenapi.VM.destroy(vm_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not destroy VM '" + vm_name + "' - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Delete the VM's disks
        mylog.info("Deleting VM disks")
        allgood = True
        for vdi_ref in vdi_list.keys():
            mylog.debug("Destroying VDI " + vdi_list[vdi_ref]['name_label'])
            try:
                session.xenapi.VDI.destroy(vdi_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not destroy VDI " + vdi_list[vdi_ref] + "- " + str(e))
                allgood = False
                continue

        if allgood:
            mylog.passed("Successfully deleted VM")
            return True
        else:
            mylog.error("Failed to remove VM disks")




# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to delete")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.vmhost, options.host_user, options.host_pass, options.debug):
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

