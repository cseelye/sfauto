#!/usr/bin/python

"""
This action will change the name and description of the disks on a Xen VM

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_regex          Regex to match to select VMs to power off
"""

import sys
from optparse import OptionParser
import logging
import re
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenRenameVdiAction(ActionBase):
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
                            "host_pass" : None},
            args)

    def Execute(self, vm_regex=None, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Rename VM disks
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

        mylog.info("Searching for matching VMs")
        vm_list = dict()
        try:
            vm_ref_list = session.xenapi.VM.get_all()
        except XenAPI.Failure as e:
            mylog.error("Could not get VM list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm_ref in vm_ref_list:
            vm = session.xenapi.VM.get_record(vm_ref)
            if vm["is_a_template"]:
                continue
            if vm["is_control_domain"]:
                continue
            if vm["is_snapshot_from_vmpp"]:
                continue
            if vm["is_snapshot"]:
                continue

            vname = vm["name_label"]
            vm_list[vname] = dict()
            vm_list[vname]["ref"] = vm_ref
            vm_list[vname]["vm"] = vm

        matched_vms = dict()
        for vname in sorted(vm_list.keys()):
            vm = vm_list[vname]["vm"]
            vm_ref = vm_list[vname]["ref"]
            if vm_regex:
                m = re.search(vm_regex, vname)
                if m:
                    matched_vms[vname] = vm_list[vname]
            else:
                matched_vms[vname] = vm_list[vname]

        allgood = True
        for vname in sorted(matched_vms.keys()):
            vm_ref = matched_vms[vname]["ref"]
            vm = matched_vms[vname]["vm"]
            mylog.info("Renaming VDI on " + vname)
            for vbd_ref in vm["VBDs"]:
                vbd = session.xenapi.VBD.get_record(vbd_ref)
                if vbd["type"] != "Disk":
                    continue
                vdi_ref = vbd["VDI"]
                try:
                    vdi = session.xenapi.VDI.get_record()
                except XenAPI.Failure as e:
                    mylog.error("Failed to get VDI record - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    allgood = False
                    continue
                try:
                    session.xenapi.VDI.set_name_label(vdi_ref, vname + "-" + vbd["device"])
                    session.xenapi.VDI.set_name_description(vdi_ref, "Disk for " + vname)
                except XenAPI.Failure as e:
                    mylog.error("Failed to set VDI label - " - str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    allgood = False

        if allgood:
            mylog.passed("Successfully renamed VDI on all VMs")
            return True
        else:
            mylog.error("Failed to rename VDI on all VMs")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to rename")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_regex, options.vmhost, options.host_user, options.host_pass, options.debug):
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

