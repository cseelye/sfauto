#!/usr/bin/python

"""
This action will suspend VMs to disk

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_name           The name of the VM to delete

    --vm_regex          Regex to match names of VMs to delete

    --vm_count          The max number of VMs to delete
"""

import sys
from optparse import OptionParser
import multiprocessing
import re
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenSuspendVmsAction(ActionBase):
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
                            },
            args)

    def _VmThread(self, vmHost, hostUser, hostPass, vmRecord, vmRef, results, debug):
        vm_name = vmRecord['name_label']
        results[vm_name] = False

        mylog.debug("  " + vm_name + ": connecting to pool")
        try:
            session = libxen.Connect(vmHost, hostUser, hostPass)
        except libxen.XenError as e:
            mylog.error("  " + vm_name + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), vmName=vm_name, exception=e)
            return

        # Suspend the VM
        mylog.info("  " + vm_name + ": Suspending VM")
        try:
            session.xenapi.VM.suspend(vmRef)
        except XenAPI.Failure as e:
            mylog.error("  " + vm_name + ": Could not suspend VM '" + vm_name + "' - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return

        results[vm_name] = True
        mylog.passed("  " + vm_name + ": Successfully suspended")

    def Execute(self, vm_name=None, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, parallel_thresh=sfdefaults.xenapi_parallel_calls_thresh, parallel_max=sfdefaults.xenapi_parallel_calls_max, debug=False):
        """
        Suspend VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the VMs the user has requested
        matched_vms = dict()
        if vm_name:
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
            vm['ref'] = vm_ref
            matched_vms[vm['name_label']] = vm
        else:
            mylog.info("Searching for matching VMs")
            try:
                vm_list = libxen.GetAllVMs(session)
            except libxen.XenError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            for vname in sorted(vm_list.keys()):
                if vm_regex:
                    m = re.search(vm_regex, vname)
                    if m:
                        matched_vms[vname] = vm_list[vname]
                else:
                    matched_vms[vname] = vm_list[vname]

                if vm_count > 0 and len(matched_vms) >= vm_count:
                    break

        if len(matched_vms.keys()) <= 0:
            mylog.info("No VMs found")
            return True

        mylog.info(str(len(matched_vms.keys())) + " VMs will be suspended: " + ", ".join(matched_vms.keys()))

        # Run the operations in parallel if there are enough
        if len(matched_vms.keys()) <= parallel_thresh:
            parallel_calls = 1
        else:
            parallel_calls = parallel_max

        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for vname in sorted(matched_vms.keys()):
            vm_ref = matched_vms[vname]["ref"]
            vm = matched_vms[vname]
            results[vname] = False
            th = multiprocessing.Process(target=self._VmThread, args=(vmhost, host_user, host_pass, vm, vm_ref, results, debug))
            th.name = "suspend-" + vname
            th.daemon = True
            self._threads.append(th)

        # Run all of the threads
        allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)

        if allgood:
            mylog.passed("Successfully suspended all VMs")
            return True
        else:
            mylog.error("Failed to suspend all VMs")
            return False




# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to suspend")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match names of VMs to suspend")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=0, help="the number of matching VMs to suspend (0 to use all)")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.xenapi_parallel_calls_thresh, help="do not use multiple threads unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.xenapi_parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, vm_regex=options.vm_regex, vm_count=options.vm_count, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, parallel_thresh=options.parallel_thresh, parallel_max=options.parallel_max, debug=options.debug):
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

