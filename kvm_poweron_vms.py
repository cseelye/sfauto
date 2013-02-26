#!/usr/bin/python

# This script will power on a list of VMs

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

host_ip = "172.25.106.000"        # The IP address of the hypervisor
                                # --host_ip

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "password"           # The password for the hypervisor
                                # --client_pass

vm_name = ""                    # The name of the VM to turn on
                                # --vm_name

vm_regex = ""                   # Regex to match to select VMs to turn on
                                # --vm_regex

vm_count = 0                    # The number of matching VMs to turn on
                                # --vm_count

# ----------------------------------------------------------------------------

import sys
from optparse import OptionParser
import json
import time
import re
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import libsf
from libsf import mylog


def main():
    # Parse command line arguments
    parser = OptionParser()
    global host_ip, host_user, host_pass, vm_name, vm_regex, vm_count
    parser.add_option("--host_ip", type="string", dest="host_ip", default=host_ip, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=vm_name, help="the name of the single VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=vm_regex, help="the regex to match names of VMs to power on")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=vm_count, help="the number of matching VMs to power on (0 to use all)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    host_ip = options.host_ip
    host_user = options.host_user
    host_pass = options.host_pass
    vm_name = options.vm_name
    vm_regex = options.vm_regex
    vm_count = options.vm_count
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(host_ip):
        mylog.error("'" + host_ip + "' does not appear to be a valid IP")
        sys.exit(1)

    mylog.info("Connecting to " + host_ip)
    try:
        conn = libvirt.open("qemu+tcp://" + host_ip + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)
    
    # Shortcut when only a single VM is specified
    if vm_name:
        try:
            vm = conn.lookupByName(vm_name)
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            sys.exit(1)
        [state, maxmem, mem, ncpu, cputime] = vm.info()
        if state == libvirt.VIR_DOMAIN_RUNNING:
            mylog.passed(vm_name + " is already powered on")
            sys.exit(0)
        else:
            mylog.info("Powering on " + vm_name)
            try:
                vm.create()
                mylog.passed("Successfully powered on " + vm.name())
                sys.exit(0)
            except libvirt.libvirtError as e:
                mylog.error("Failed to power on " + vm.name() + ": " + str(e))
                sys.exit(1)
    
    
    mylog.info("Searching for matching VMs")
    matched_vms = []
    
    # Get a list of stopped VMs
    try:
        vm_ids = conn.listDefinedDomains()
        stopped_vm_list = map(conn.lookupByName, vm_ids)
        stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    for vm in stopped_vm_list:
        if vm_count > 0 and len(matched_vms) >= vm_count:
            break
        if vm_regex:
            m = re.search(vm_regex, vm.name())
            if m: matched_vms.append(vm)
        else:
            matched_vms.append(vm)
    
    # Get a list of running VMs
    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    for vm in running_vm_list:
        if vm_count > 0 and len(matched_vms) >= vm_count:
            break
        if vm_regex:
            m = re.search(vm_regex, vm.name())
            if m: matched_vms.append(vm)
        else:
            matched_vms.append(vm)
    
    power_count = 0
    matched_vms = sorted(matched_vms, key=lambda vm: vm.name())
    for vm in matched_vms:
        [state, maxmem, mem, ncpu, cputime] = vm.info()
        if state == libvirt.VIR_DOMAIN_RUNNING:
            mylog.passed("  " + vm.name() + " is already powered on")
            power_count += 1
        else:
            mylog.info("  Powering on " + vm.name())
            try:
                vm.create()
                power_count += 1
                mylog.passed("  Successfully powered on " + vm.name())
            except libvirt.libvirtError as e:
                mylog.error("  Failed to power on " + vm.name() + ": " + str(e))
    
    if power_count == len(matched_vms):
        mylog.passed("All VMs powered on successfully")
        sys.exit(0)
    else:
        mylog.error("Not all VMs were powered on")
        sys.exit(1)

    


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
