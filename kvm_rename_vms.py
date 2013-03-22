#!/usr/bin/python

# This script will rename KVM hostnames to match VM names

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"      # The IP address of the hypervisor
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

vm_user = "root"                # The username for the clients
                                # --vm_user

<<<<<<< HEAD
client_pass = "password"           # The password for the clients
                                    # --client_pass
=======
vm_pass = "solidfire"           # The password for the clients
                                # --vm_pass
>>>>>>> 6611be4... KVM scripts - standardize command line args, add a few new ones

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
import libclient
from libclient import ClientError, SfClient
import libclientmon
from libclientmon import CftClientMon

def main():
    # Parse command line arguments
    parser = OptionParser()
    global vmhost, host_user, host_pass, vm_user, vm_pass
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_user", type="string", dest="vm_user", default=vm_user, help="the username for the client [%default]")
    parser.add_option("--vm_pass", type="string", dest="vm_pass", default=vm_pass, help="the password for the client [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_user = options.vm_user
    vm_pass = options.vm_pass
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid IP")
        sys.exit(1)

    # Get a list of vm info from the monitor
    monitor = CftClientMon()
    vm_list = monitor.GetGroupVmInfo("KVM")

    mylog.info("Connecting to " + vmhost)
    try:
        conn = libvirt.openReadOnly("qemu+tcp://" + vmhost + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)

    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)

    updated = 0
    for vm in running_vm_list:
        mylog.info("Updating hostname on " + vm.name())
        # Find the VM alphabetically first MAC address from the XML config
        vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
        mac_list = []
        for node in vm_xml.findall("devices/interface/mac"):
            mac_list.append(node.get("address"))
        mac_list.sort()
        mac = mac_list[0]

        # Get the IP of this VM from the monitor info
        ip = ""
        for vm_info in vm_list:
            if vm_info.MacAddress == mac.replace(":", ""):
                ip = vm_info.IpAddress
                break
        if not ip:
            mylog.warning("Could not find IP for " + vm.name())
            continue

        client = SfClient()
        #mylog.info("Connecting to client '" +ip + "'")
        try:
            client.Connect(ip, vm_user, vm_pass)
        except ClientError as e:
            mylog.error(e)
            continue

        if (client.Hostname == vm.name()):
            mylog.passed("  Hostname is correct")
            updated += 1
            continue

        try:
            client.UpdateHostname(vm.name())
        except ClientError as e:
            mylog.error(e.message)
            sys.exit(1)

        mylog.passed("  Successfully set hostname")
        updated += 1

    if updated == len(vm_ids):
        mylog.passed("Successfully updated hostname on all running VMs")
        sys.exit(0)
    else:
        mylog.error("Could not update hostname on all running VMs")
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
