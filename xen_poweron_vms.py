#!/usr/bin/python

# This script will power on a list of VMs

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --vmhost

host_user = "root"              # The username for the hypervisor
                                # --host_user

host_pass = "solidfire"         # The password for the hypervisor
                                # --host_pass

vm_name = ""                    # The name of the VM to turn on
                                # --vm_name

vm_regex = ""                   # Regex to match to select VMs to turn on
                                # --vm_regex

vm_count = 0                    # The number of matching VMs to turn on
                                # --vm_count

# ----------------------------------------------------------------------------

import sys, os
from optparse import OptionParser
import json
import time
import re
import libsf
from libsf import mylog
import XenAPI
import libxen

def main():
    # Parse command line arguments
    parser = OptionParser()
    global vmhost, host_user, host_pass, vm_name, vm_regex, vm_count
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=vm_name, help="the name of the single VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=vm_regex, help="the regex to match names of VMs to power on")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=vm_count, help="the number of matching VMs to power on (0 to use all)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_name = options.vm_name
    vm_regex = options.vm_regex
    vm_count = options.vm_count
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid IP")
        sys.exit(1)

    # Connect to the host/pool
    mylog.info("Connecting to " + vmhost)
    session = None
    try:
        session = libxen.Connect(vmhost, host_user, host_pass)
    except libxen.XenError as e:
        mylog.error(str(e))
        sys.exit(1)

    if vm_name:
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find VM " + vm_name + " - " + str(e))
            sys.exit(1)
        vm = session.xenapi.VM.get_record(vm_ref)
        if vm["power_state"] == "Running":
            mylog.passed(vm_name + " is already powered on")
            sys.exit(0)
        mylog.info("Powering on " + vm_name)
        try:
            session.xenapi.VM.start(vm_ref, False, False)
        except XenAPI.Failure as e:
            mylog.error("Could not start " + vm_name + " - " + str(e))
            sys.exit(0)

    mylog.info("Searching for matching VMs")

    # Get a list of all VMs
    vm_list = dict()
    try:
        vm_ref_list = session.xenapi.VM.get_all()
    except XenAPI.Failure as e:
        mylog.error("Could not get VM list: " + str(e))
        sys.exit(1)
    for vm_ref in vm_ref_list:
        vm = session.xenapi.VM.get_record(vm_ref)
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

        if vm_count > 0 and len(matched_vms) >= vm_count:
            break

    power_count = 0
    for vname in sorted(matched_vms.keys()):
        vm_ref = matched_vms[vname]["ref"]
        vm = matched_vms[vname]["vm"]
        if vm["power_state"] == "Running":
            mylog.passed("  " + vname + " is already powered on")
            power_count += 1
        else:
            mylog.info("  Powering on " + vm["name_label"])
            try:
                session.xenapi.VM.start(vm_ref, False, False)
                power_count += 1
                mylog.passed("  Successfully powered on " + vname)
            except XenAPI.Failure as e:
                mylog.error("  Failed to power on " + vm["name_label"] + " - " + str(e))

    if power_count == len(matched_vms):
        mylog.passed("All VMs powered on successfully")
        sys.exit(0)
    else:
        mylog.error("Not all VMs were powered on")
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
