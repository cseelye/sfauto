#!/usr/bin/python

"""
This action will migrate XenServer VMs to a random host

When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the hypervisor host

    --host_user         The host username
    SFHOST_USER env var

    --host_pass         The host password
    SFHOST_PASS env var

    --vm_name           The name of a single VM to migrate

    --vm_regex          Regex to match names of VMs to migrate

    --vm_count          The max number of VMs to migrate

    --parallel_thresh   Do not use multiple threads unless there are more than this many

    --parallel_max      The max number of threads to use
"""

import sys
from optparse import OptionParser
import logging
import re
import time
import multiprocessing
import random
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenMigrateVmsRandomAction(ActionBase):
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

    def _VmThread(self, VmHost, HostUser, HostPass, VmRecord, VmRef, HostList, results, debug):
        results[VmRecord['name_label']] = False

        # Select a host to migrate to
        dest_host = random.choice(HostList.keys())
        while dest_host == VmRecord['resident_on']:
            dest_host = random.choice(HostList.keys())

        mylog.debug("  " + VmRecord['name_label'] + ": connecting to " + VmHost)
        try:
            session = libxen.Connect(VmHost, HostUser, HostPass)
        except libxen.XenError as e:
            mylog.error("  " + VmRecord['name_label'] + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), vmName=VmRecord['name_label'], exception=e)
            return

        mylog.info("  " + VmRecord['name_label'] + ": migrating to " + HostList[dest_host]['name_label'])
        retry = 3
        while retry > 0:
            try:
                session.xenapi.VM.pool_migrate(VmRef, dest_host, {})
                results[VmRecord['name_label']] = True
                break
            except XenAPI.Failure as e:
                #if e[0] == "CANNOT_CONTACT_HOST":
                    #time.sleep(30)
                    #retry -= 1
                    #continue
                #else:
                mylog.error("  " + VmRecord['name_label'] + ": Failed to migrate - " + str(e))
                self.RaiseFailureEvent(message=str(e), vmName=VmRecord['name_label'], exception=e)
                return
        if not results[VmRecord['name_label']]:
            mylog.error("  " + VmRecord['name_label'] + ": Failed to migrate")
            self.RaiseFailureEvent(message=str(e), vmName=VmRecord['name_label'], exception=e)

    def Execute(self, vm_name=None, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_xen,  host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, parallel_thresh=sfdefaults.xenapi_parallel_calls_thresh, parallel_max=sfdefaults.xenapi_parallel_calls_max, debug=False):
        """
        Power on VMs
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

        # Get a list of the hosts in the pool
        host_ref_list = session.xenapi.host.get_all()
        hosts = dict()
        for hr in host_ref_list:
            host = session.xenapi.host.get_record(hr)
            hosts[hr] = host

        matched_vms = dict()
        if vm_name:
            try:
                vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
            except XenAPI.Failure as e:
                mylog.error("Could not find VM " + vm_name + " - " + str(e))
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
                mylog.error("Could not get VM record for " + vm_name + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            matched_vms[vm_name] = dict()
            matched_vms[vm_name]["ref"] = vm_ref
            matched_vms[vm_name]["vm"] = vm

        else:
            mylog.info("Searching for matching VMs")

            # Get a list of all VMs
            vm_list = dict()
            try:
                vm_ref_list = session.xenapi.VM.get_all()
            except XenAPI.Failure as e:
                mylog.error("Could not get VM list: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for vm_ref in vm_ref_list:
                vm = session.xenapi.VM.get_record(vm_ref)
                if not vm["is_a_template"] and not vm["is_control_domain"] and vm["power_state"] == "Running":
                    vname = vm["name_label"]
                    if vm_regex:
                        m = re.search(vm_regex, vname)
                        if m:
                            vm_list[vname] = dict()
                            vm_list[vname]["ref"] = vm_ref
                            vm_list[vname]["vm"] = vm
                    else:
                        vm_list[vname] = dict()
                        vm_list[vname]["ref"] = vm_ref
                        vm_list[vname]["vm"] = vm


            if vm_count <= 0 or vm_count > len(vm_list.keys()):
                matched_vms = vm_list
            else:
                matched_vms = dict()
                while len(matched_vms.keys()) < vm_count:
                    vname = random.choice(vm_list.keys())
                    matched_vms[vname] = vm_list[vname]

        # Run the API operations in parallel if there are enough
        if len(matched_vms.keys()) <= parallel_thresh:
            parallel_calls = 1
        else:
            parallel_calls = parallel_max

        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for vname in sorted(matched_vms.keys()):
            vm_ref = matched_vms[vname]["ref"]
            vm = matched_vms[vname]["vm"]
            results[vname] = False
            th = multiprocessing.Process(target=self._VmThread, args=(vmhost, host_user, host_pass, vm, vm_ref, hosts, results, debug))
            th.daemon = True
            self._threads.append(th)

        # Run all of the threads
        allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)
        if allgood:
            mylog.passed("All VMs migrated successfully")
            return True
        else:
            mylog.error("Not all VMs could be migrated")
            return False


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the single VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match names of VMs to power on")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=0, help="the number of matching VMs to power on - 0 to use all [%default]")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.xenapi_parallel_calls_thresh, help="do not use multiple threads unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.xenapi_parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.parallel_thresh, options.parallel_max, options.debug):
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

