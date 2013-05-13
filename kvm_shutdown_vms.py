#!/usr/bin/python

"""
This action will shutdown VMs on a KVM hypervisor

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
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class KvmShutdownVmsAction(ActionBase):
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

    def Execute(self, vm_name=None, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Shutdown VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Connecting to " + vmhost)
        try:
            conn = libvirt.open("qemu+tcp://" + vmhost + "/system")
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
            return False
        if conn == None:
            mylog.error("Failed to connect")
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE)
            return False

        # Shortcut when only a single VM is specified
        if vm_name:
            try:
                vm = conn.lookupByName(vm_name)
            except libvirt.libvirtError as e:
                mylog.error(str(e))
                super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
                return False
            [state, maxmem, mem, ncpu, cputime] = vm.info()
            if state == libvirt.VIR_DOMAIN_SHUTOFF:
                mylog.passed(vm_name + " is already shutdown")
                return True
            else:
                mylog.info("Shutting down " + vm_name)
                try:
                    vm.shutdown()
                    mylog.passed("Successfully shutdown " + vm.name())
                    return True
                except libvirt.libvirtError as e:
                    mylog.error("Failed to shutdown " + vm.name() + ": " + str(e))
                    super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
                    return False

        mylog.info("Searching for matching VMs")
        matched_vms = []

        # Get a list of running VMs
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
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

        # Get a list of stopped VMs
        try:
            vm_ids = conn.listDefinedDomains()
            stopped_vm_list = map(conn.lookupByName, vm_ids)
            stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, exception=e)
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

        # Shutdown the VMs
        power_count = 0
        matched_vms = sorted(matched_vms, key=lambda vm: vm.name())
        for vm in matched_vms:
            [state, maxmem, mem, ncpu, cputime] = vm.info()
            if state == libvirt.VIR_DOMAIN_SHUTOFF:
                mylog.passed("  " + vm.name() + " is already shutdown")
                power_count += 1
            else:
                mylog.info("  Shutting down " + vm.name())
                try:
                    vm.shutdown()
                    power_count += 1
                    mylog.passed("  Successfully shutdown " + vm.name())
                except libvirt.libvirtError as e:
                    mylog.error("  Failed to shutdown " + vm.name() + ": " + str(e))

        if power_count == len(matched_vms):
            mylog.passed("All VMs shutdown off successfully")
            return True
        else:
            mylog.error("Not all VMs were shutdown off")
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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to shutdown")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to shutdown")
    parser.add_option("--vm_count", type="string", dest="vm_count", default=None, help="the number of matching VMs to shutdown")
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

