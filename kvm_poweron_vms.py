#!/usr/bin/python

"""
This action will power on VMs on a KVM hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_names          The name of the VMs to power on

    --vm_regex          Regex to match to select VMs to power on

    --vm_count          The number of matching VMs to power on

    --thread_max        The max number of threads to use
"""

import sys
from optparse import OptionParser
import logging
import multiprocessing
import re
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmPoweronVmsAction(ActionBase):
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
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")


    def _VmThreadPowerOn(self, vmHost, hostUser, hostPass, connection, vm, results):
        #connect again
        mylog.info("Connecting to " + vm)
        try:
            if connection == "ssh":
                conn = libvirt.open("qemu+ssh://" + vmHost + "/system")
            elif connection == "tcp":
                conn = libvirt.open("qemu+tcp://" + vmHost + "/system")
            else:
                mylog.error("There was an error connecting to libvirt on " + vmHost + " wrong connection type: " + connection)
                return False
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if conn is None:
            mylog.error("Failed to connect")
            self.RaiseFailureEvent(message="Failed to connect")
            return False

        try:
            vm = conn.lookupByName(vm)
        except libvirt.libvirtError as e:
            mylog.error("Unable to power on " + vm)
            conn.close()
            return

        mylog.info("Powering on: " + vm.name())
        try:
            vm.create()
            mylog.passed("Successfully powered on " + vm.name())
            results[vm.name()] = True
        except libvirt.libvirtError as e:
            mylog.error("Failed to power on " + vm.name() + ": " + str(e))

        #close when done
        conn.close()


    def Execute(self, vm_names=None, connection=sfdefaults.kvm_connection, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, thread_max=1, debug=False):
        """
        Power on VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Connecting to " + vmhost)
        try:
            if connection == "ssh":
                conn = libvirt.openReadOnly("qemu+ssh://" + vmhost + "/system")
            elif connection == "tcp":
                conn = libvirt.openReadOnly("qemu+tcp://" + vmhost + "/system")
            else:
                mylog.error("There was an error connecting to libvirt on " + vmhost + " wrong connection type: " + connection)
                return False
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if conn == None:
            mylog.error("Failed to connect")
            self.RaiseFailureEvent(message="Failed to connect")
            return False

        # Shortcut when a list of VMs is specified
        if vm_names != None:
            self._threads = []
            manager = multiprocessing.Manager()
            results = manager.dict()
            for name in vm_names:
                try:
                    vm = conn.lookupByName(name)
                except libvirt.libvirtError as e:
                    mylog.error(str(e))
                    sys.exit(1)
                [state, maxmem, mem, ncpu, cputime] = vm.info()
                if state == libvirt.VIR_DOMAIN_RUNNING:
                    mylog.passed(vm.name() + " is already powered on")
                else:
                    results[vm.name()] = False
                    th = multiprocessing.Process(target=self._VmThreadPowerOn, args=(vmhost, host_user, host_pass, connection, vm.name(), results))
                    th.daemon = True
                    self._threads.append(th)

            allgood = libsf.ThreadRunner(self._threads, results, thread_max)
            if allgood:
                mylog.passed("All VMs powered on successfully")
                return True
            else:
                mylog.error("Not all VMs could be powered on")
                return False


        mylog.info("Searching for matching VMs")
        matched_vms = []

        # Get a list of stopped VMs
        try:
            vm_ids = conn.listDefinedDomains()
            stopped_vm_list = map(conn.lookupByName, vm_ids)
            stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in stopped_vm_list:
            if vm_count > 0 and len(matched_vms) >= vm_count:
                break
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)

        # Get a list of running VMs
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in running_vm_list:
            if vm_count > 0 and len(matched_vms) >= vm_count:
                break
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)

        #power on the vms
        matched_vms = sorted(matched_vms, key=lambda vm: vm.name())
        self._threads = []
        manager = multiprocessing.Manager()
        results = manager.dict()
        for vm in matched_vms:
            [state, maxmem, mem, ncpu, cputime] = vm.info()
            if state == libvirt.VIR_DOMAIN_RUNNING:
                mylog.passed("  " + vm.name() + " is already powered on")
            else:
                results[vm.name()] = False
                th = multiprocessing.Process(target=self._VmThreadPowerOn, args=(vmhost, host_user, host_pass, connection, vm.name(), results))
                th.daemon = True
                self._threads.append(th)

        allgood = libsf.ThreadRunner(self._threads, results, thread_max)
        if allgood:
            mylog.passed("All VMs powered on successfully")
            return True
        else:
            mylog.error("Not all VMs could be powered on")
            return False



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_names", action="list", dest="vm_names", default=None, help="the name of the VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power on")
    parser.add_option("--vm_count", type="string", dest="vm_count", default=None, help="the number of matching VMs to power on")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=1, help="the number of threads to use")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_names, options.connection, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.thread_max, options.debug):
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

