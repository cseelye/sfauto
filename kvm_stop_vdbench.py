#!/usr/bin/python

# This script will stop vdbench on a list of VMs

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"        # The IP address of the hypervisor
                                # --vmhost

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "password"           # The password for the hypervisor
                                # --client_pass

vm_name = ""                    # The name of the VM to shutdown
                                # --vm_name

vm_regex = ""                   # Regex to match to select VMs to stop vdbench
                                # --vm_regex

vm_count = 0                    # The number of matching VMs to stop vdbench
                                # --vm_count

vm_user = "root"                # The username for the VMs
                                # --vm_user

vm_pass = "password"           # The password for the VMs
                                # --vm_pass

# ----------------------------------------------------------------------------

import sys, os
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
import libclientmon
from libclientmon import SfautoClientMon


def main():
    # Parse command line arguments
    parser = OptionParser()
    global vmhost, host_user, host_pass, vm_name, vm_regex, vm_count, vm_user, vm_pass
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=vm_name, help="the name of the single VM to stop vdbench")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=vm_regex, help="the regex to match names of VMs to stop vdbench")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=vm_count, help="the number of matching VMs to stop vdbench (0 to use all)")
    parser.add_option("--vm_user", type="string", dest="vm_user", default=vm_user, help="the username for the VMs [%default]")
    parser.add_option("--vm_pass", type="string", dest="vm_pass", default=vm_pass, help="the password for the VMs [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_name = options.vm_name
    vm_regex = options.vm_regex
    vm_count = options.vm_count
    vm_user = options.vm_user
    vm_pass = options.vm_pass
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid IP")
        sys.exit(1)

    # Get a list of vm info from the monitor
    monitor = SfautoClientMon()
    monitor_list = monitor.GetGroupVmInfo("KVM")

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
    matched_vms = []

    # Get a list of running VMs
    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    for vm in running_vm_list:
        if vm_name and vm.name() == vm_name:
            matched_vms.append(vm)
            break
        if vm_count > 0 and len(matched_vms) >= vm_count:
            break
        if vm_regex:
            m = re.search(vm_regex, vm.name())
            if m: matched_vms.append(vm)
        else:
            matched_vms.append(vm)

    if len(matched_vms) <= 0:
        mylog.warning("Could not find any VMs that match")
        sys.exit(0)

    vdbench_count = 0
    matched_vms = sorted(matched_vms, key=lambda vm: vm.name())
    for vm in matched_vms:
        # Find the VM's alphabetically first MAC address from the XML config
        vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
        mac_list = []
        for node in vm_xml.findall("devices/interface/mac"):
            mac_list.append(node.get("address"))
        mac_list.sort()
        mac = mac_list[0]

        # Get the IP of this VM from the monitor info
        ip = ""
        for vm_info in monitor_list:
            if vm_info.MacAddress == mac.replace(":", ""):
                ip = vm_info.IpAddress
                break
        if not ip:
            mylog.error("Could not find IP address for " + vm.name())
            continue

        # Stop vdbench
        mylog.info("  Stopping vdbench on " + vm.name())
        ssh = libsf.ConnectSsh(ip, vm_user, vm_pass)
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "status vdbench; echo $?")
        stdout_data = stdout.readlines()
        if int(stdout_data.pop()) != 0:
            mylog.error("  Could not get vdbench status on " + vm.name())
            ssh.close()
            continue

        if "stop" in stdout_data[0]:
            mylog.passed("  vdbench is already stopped on " + vm.name())
            ssh.close()
            vdbench_count += 1
            continue

        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "stop vdbench; echo $?")
        if int(stdout.readlines().pop()) != 0:
            mylog.error("  Failed to stop vdbench on " + vm.name())
            ssh.close()
            continue

        vdbench_count += 1
        mylog.passed("  Successfully stopped vdbench on " + vm.name())
        ssh.close()

    if vdbench_count == len(matched_vms):
        mylog.passed("vdbench stopped on all VMs")
        sys.exit(0)
    else:
        mylog.error("Could not stop vdbench on all VMs")
        sys.exit(1)




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
