#!/usr/bin/python

# This script will count the number of VMs that match a prefix

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"        # The IP address of the hypervisor
                                # --host_ip

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "password"           # The password for the hypervisor
                                # --client_pass

vm_prefix = ""                    # The prefix of the VM names to match
                                # --vm_name

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
    global vmhost, host_user, host_pass, vm_prefix, csv, bash
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_prefix", type="string", dest="vm_prefix", default=vm_prefix, help="the prefix of the VM names to match")
    parser.add_option("--csv", action="store_true", dest="csv", help="display minimal output in comma separated format")
    parser.add_option("--bash", action="store_true", dest="bash", help="display minimal output in space separated format")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_prefix = options.vm_prefix
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
    matching_vms = 0

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
            matching_vms += 1

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
            matching_vms += 1

    # Show the number of VMs found
    if bash or csv:
        sys.stdout.write(str(matching_vms))
        sys.stdout.write("\n")
        sys.stdout.flush()
    else:
        mylog.info("There are " + str(matching_vms) + " VMs with prefix " + vm_prefix)

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
