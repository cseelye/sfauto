#!/usr/bin/python

# This script will change the name and description of the disk on a VM
# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"        # The IP address of the hypervisor
                                # --host_ip

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "solidfire"           # The password for the hypervisor
                                # --client_pass

vm_regex = ""                   # Regex to match to select VMs to renames
                                # --vm_regex

# ----------------------------------------------------------------------------

import sys
from optparse import OptionParser
import json
import time
import re
import libsf
from libsf import mylog
import libxen

def main():
    # Parse command line arguments
    parser = OptionParser()
    global vmhost, host_user, host_pass, vm_regex
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=vm_regex, help="the regex to match names of VMs to power on")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_regex = options.vm_regex
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid hypervisor IP")
        sys.exit(1)

    # Connect to the host/pool
    mylog.info("Connecting to " + vmhost)
    session = None
    try:
        session = libxen.Connect(vmhost, host_user, host_pass)
    except libxen.XenError as e:
        mylog.error(str(e))
        sys.exit(1)

    mylog.info("Searching for matching VMs")
    vm_list = dict()
    try:
        vm_ref_list = session.xenapi.VM.get_all()
    except XenAPI.Failure as e:
        mylog.error("Could not get VM list: " + str(e))
        sys.exit(1)
    for vm_ref in vm_ref_list:
        vm = session.xenapi.VM.get_record(vm_ref)
        if vm["is_a_template"]:
            continue
        if vm["is_control_domain"]:
            continue
        if vm["is_snapshot_from_vmpp"]:
            continue

        vname = vm["name_label"]
        vm_list[vname] = dict()
        vm_list[vname]["ref"] = vm_ref
        vm_list[vname]["vm"] = vm

    matched_vms = dict()
    for vname in sorted(vm_list.keys()):
        vm = vm_list[vname]["vm"]
        vm_ref = vm_list[vname]["ref"]
        if vm_regex:
            m = re.search(vm_regex, vname)
            if m:
                matched_vms[vname] = vm_list[vname]
        else:
            matched_vms[vname] = vm_list[vname]

    for vname in sorted(matched_vms.keys()):
        vm_ref = matched_vms[vname]["ref"]
        vm = matched_vms[vname]["vm"]
        mylog.info("Renaming VDI on " + vname)
        for vbd_ref in vm["VBDs"]:
                vbd = session.xenapi.VBD.get_record(vbd_ref)
                if vbd["type"] != "Disk":
                    continue
                vdi_ref = vbd["VDI"]
                session.xenapi.VDI.set_name_label(vdi_ref, vname + "-disk0")
                session.xenapi.VDI.set_name_description(vdi_ref, "Boot disk for " + vname)

    mylog.passed("Successfully renamed VDI on all VMs")


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
