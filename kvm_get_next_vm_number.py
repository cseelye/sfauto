#!/usr/bin/python

# This script will get the next VM number that matches a prefix

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --host_ip

host_user = "root"              # The username for the hypervisor
                                # --host_user

host_pass = "password"         # The password for the hypervisor
                                # --host_pass

vm_prefix = ""                  # The prefix of the VM names to match
                                # --vm_name

fill = False                    # Find the first gap in the sequence instead of the highest number
                                # --fill

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys, os
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
    global vmhost, host_user, host_pass, vm_prefix, fill, csv, bash
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_prefix", type="string", dest="vm_prefix", default=vm_prefix, help="the prefix of the VM names to match")
    parser.add_option("--fill", action="store_true", dest="fill", help="find the first gap in the sequence instead of the highest number")
    parser.add_option("--csv", action="store_true", dest="csv", help="display minimal output in comma separated format")
    parser.add_option("--bash", action="store_true", dest="bash", help="display minimal output in space separated format")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_prefix = options.vm_prefix
    fill = options.fill
    if options.csv:
        csv = True
        mylog.silence = True
    if options.bash:
        bash = True
        mylog.silence = True
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid hypervisor IP")
        sys.exit(1)

    mylog.info("Connecting to " + vmhost)
    try:
        conn = libvirt.open("qemu+tcp://" + vmhost + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)

    mylog.info("Searching for matching VMs")
    found_numbers = []
    highest_number = 0

    # Get a list of stopped VMs
    try:
        vm_ids = conn.listDefinedDomains()
        stopped_vm_list = map(conn.lookupByName, vm_ids)
        stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    for vm in stopped_vm_list:
        m = re.search("^" + vm_prefix + "0*(\d+)$", vm.name())
        if m:
            vm_number = int(m.group(1))
            mylog.debug("Found " + str(vm_number))
            found_numbers.append(vm_number)
            if vm_number > highest_number:
                highest_number = vm_number

    # Get a list of running VMs
    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    for vm in running_vm_list:
        m = re.search("^" + vm_prefix + "0*(\d+)$", vm.name())
        if m:
            vm_number = int(m.group(1))
            mylog.debug("Found " + str(vm_number))
            found_numbers.append(vm_number)
            if vm_number > highest_number:
                highest_number = vm_number
    found_numbers = sorted(found_numbers)

    # Find the first gap in the sequence
    if fill:
        gap = None
        for i in range(1, len(found_numbers) + 1):
            if found_numbers[i-1] != i:
                gap = i
                break
        if gap:
            if bash or csv:
                sys.stdout.write(str(gap))
                sys.stdout.write("\n")
                sys.stdout.flush()
            else:
                mylog.info("The first gap in " + vm_prefix + " is " + str(gap))
            sys.exit(0)
        else:
            mylog.info("There are no gaps in " + vm_prefix)

    # Show the next number in the sequence
    if bash or csv:
        sys.stdout.write(str(highest_number + 1))
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        mylog.info("The next VM number for " + str(vm_prefix) + " is " + str(highest_number + 1))

    sys.exit(0)






if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = libsf.ScriptTimer()
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
