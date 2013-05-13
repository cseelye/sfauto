#!/usr/bin/python

"""
This action will shutdown VMs on a XenServer hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_name           The name of the VM to shutdown

    --vm_regex          Regex to match to select VMs to shutdown

    --vm_count          The number of matching VMs to shutdown
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

class XenShutdownVmsAction(ActionBase):
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

    def Execute(self, vm_name=None, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Shutdown VMs
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
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
            return False

        if vm_name:
            try:
                vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
            except XenAPI.Failure as e:
                mylog.error("Could not find VM " + vm_name + " - " + str(e))
                sys.exit(1)
            vm = session.xenapi.VM.get_record(vm_ref)
            if vm["power_state"] == "Halted":
                mylog.passed(vm_name + " is already shut down")
                return True
            mylog.info("Shutting down " + vm_name)
            try:
                session.xenapi.VM.clean_shutdown(vm_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not shutdown " + vm_name + " - " + str(e))
                super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
                return False

        mylog.info("Searching for matching VMs")

        # Get a list of all VMs
        vm_list = dict()
        try:
            vm_ref_list = session.xenapi.VM.get_all()
        except XenAPI.Failure as e:
            mylog.error("Could not get VM list: " + str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
            return False
        for vm_ref in vm_ref_list:
            vm = session.xenapi.VM.get_record(vm_ref)
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

            if vm_count > 0 and len(matched_vms) >= vm_count:
                break

        shutdown_count = 0
        for vname in sorted(matched_vms.keys()):
            vm_ref = matched_vms[vname]["ref"]
            vm = matched_vms[vname]["vm"]
            if vm["power_state"] == "Halted":
                mylog.passed("  " + vname + " is already shutdown")
                shutdown_count += 1
            else:
                mylog.info("  Shutting down " + vm["name_label"])
                try:
                    session.xenapi.VM.clean_shutdown(vm_ref)
                    shutdown_count += 1
                    mylog.passed("  Successfully shutdown " + vname)
                except XenAPI.Failure as e:
                    mylog.error("  Failed to shutdown " + vm["name_label"] + " - " + str(e))
                    super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)

        if shutdown_count == len(matched_vms):
            mylog.passed("All VMs shutdown successfully")
            return True
        else:
            mylog.error("Not all VMs were shutdown")
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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to power off")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power off")
    parser.add_option("--vm_count", type="string", dest="vm_count", default=None, help="the number of matching VMs to power off")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.debug):
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

