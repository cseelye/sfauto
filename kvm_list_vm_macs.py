#!/usr/bin/python

# This script will list out the VMs and their MAC addresses on a KVM hypervisor

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --vmhost

host_user = "root"              # The username for the hypervisor
                                # --host_user

<<<<<<< HEAD
host_pass = "password"           # The password for the hypervisor
                                # --client_pass
=======
host_pass = "solidfire"         # The password for the hypervisor
                                # --host_pass
>>>>>>> 6611be4... KVM scripts - standardize command line args, add a few new ones

csv = False                     # Display minimal output that is suitable for piping into other programs
                                # --csv

bash = False                    # Display minimal output that is formatted for a bash array/for  loop
                                # --bash

# ----------------------------------------------------------------------------

import sys
from optparse import OptionParser
import json
import time
import re
from xml.etree import ElementTree
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
    global vmhost, host_user, host_pass, csv, bash
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--csv", action="store_true", dest="csv", help="display minimal output in comma separated format")
    parser.add_option("--bash", action="store_true", dest="bash", help="display minimal output in space separated format")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
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
        mylog.error("'" + vmhost + "' does not appear to be a valid IP")
        sys.exit(1)

    mylog.info("Connecting to " + vmhost)
    try:
        conn = libvirt.openReadOnly("qemu+tcp://" + vmhost + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)

    # Get a list of VMs
    vm_list = []
    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        vm_list += running_vm_list
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    try:
        vm_ids = conn.listDefinedDomains()
        stopped_vm_list = map(conn.lookupByName, vm_ids)
        vm_list += stopped_vm_list
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    vm_list = sorted(vm_list, key=lambda vm: vm.name())

    for vm in vm_list:

        # Find the VM alphabetically first MAC address from the XML config
        vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
        mac_list = []
        for node in vm_xml.findall("devices/interface/mac"):
            mac_list.append(node.get("address"))
        mac_list.sort()
        mac = mac_list[0]

        if csv or bash:
            separator = ","
            if bash: separator = " "
            sys.stdout.write(vm.name() + separator + mac + "\n")
            sys.stdout.flush()
        else:
            mylog.info("  " + vm.name() + " - " + mac)



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
