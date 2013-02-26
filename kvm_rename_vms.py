#!/usr/bin/python

# This script will rename KVM hostnames to match VM names

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

host_ip = "172.25.106.000"        # The IP address of the hypervisor
                                # --host_ip

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "password"           # The password for the hypervisor
                                # --client_pass

client_user = "root"                # The username for the clients
                                    # --client_user

client_pass = "password"           # The password for the clients
                                    # --client_pass

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
    global host_ip, host_user, host_pass, client_user, client_pass
    parser.add_option("--host_ip", type="string", dest="host_ip", default=host_ip, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the client [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    host_ip = options.host_ip
    host_user = options.host_user
    host_pass = options.host_pass
    client_user = options.client_user
    client_pass = options.client_pass
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(host_ip):
        mylog.error("'" + host_ip + "' does not appear to be a valid IP")
        sys.exit(1)

    # Get a list of vm info from the monitor
    monitor = CftClientMon()
    vm_list = monitor.GetGroupVmInfo("KVM")
    
    mylog.info("Connecting to " + host_ip)
    try:
        conn = libvirt.openReadOnly("qemu+tcp://" + host_ip + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)
    
    # Get a list of VMs and their MACs from the hypervisor
    try:
        vm_ids = conn.listDomainsID()
    except libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)

    for vid in vm_ids:
        try:
            vm = conn.lookupByID(vid)
        except libvirt.libvirtError as e:
            mylog.error("Failed to get info for VM " + vid)
            continue
        
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

        client = SfClient()
        mylog.info("Connecting to client '" +ip + "'")
        try:
            client.Connect(ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(e)
            continue
    
        mylog.info("Updating hostname on " + client.Hostname)
        try:
            client.UpdateHostname(vm.name())
        except ClientError as e:
            mylog.error(e.message)
            sys.exit(1)
    
        mylog.passed("  Successfully set hostname")



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
