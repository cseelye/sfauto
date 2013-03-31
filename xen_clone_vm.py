#!/usr/bin/python

# This script will create clone a VM

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --host_ip

host_user = "root"              # The username for the hypervisor
                                # --client_user

host_pass = "solidfire"         # The password for the hypervisor
                                # --client_pass

vm_name = ""                    # The name of the VM to clone
                                # --vm_name

clone_name = ""                 # The name to give the clone
                                # --clone_name

dest_sr = ""                    # The name of the SR to put the clone in. If not specified, use an SR with the same name as the clone
                                # --dest_sr

# ----------------------------------------------------------------------------

import sys
from optparse import OptionParser
import json
import time
import re
import socket
import libsf
from libsf import mylog
import XenAPI
import libxen
#import xml.dom.minidom
from xml.etree import ElementTree

def main():
    # Parse command line arguments
    parser = OptionParser()
    global vmhost, host_user, host_pass, vm_name, clone_name, dest_sr
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=vm_name, help="the name of the VM to clone")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=clone_name, help="the name to give to the clone")
    parser.add_option("--dest_sr", type="string", dest="dest_sr", default=dest_sr, help="the name of the SR to put the clone in. If not specified, use an SR with the same name as the clone")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_name = options.vm_name
    clone_name = options.clone_name
    dest_sr = options.dest_sr
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

    # Find the source VM
    mylog.info("Searching for source VM")
    vm_ref = None
    try:
        vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
    except XenAPI.Failure as e:
        mylog.error("Could not find source VM '" + vm_name + "' - " + str(e))
        sys.exit(1)
    if not vm_ref or len(vm_ref) <= 0:
        mylog.error("Could not find source VM '" + vm_name + "'")
        sys.exit(1)
    vm_ref = vm_ref[0]

    # Find the destination SR
    mylog.info("Searching for destination SR")
    if not dest_sr:
        dest_sr = clone_name
    dest_sr_ref = None
    try:
        dest_sr_ref = session.xenapi.SR.get_by_name_label(dest_sr)
    except XenAPI.Failure as e:
        mylog.error("Could not find destination SR '" + dest_sr + "' - " + str(e))
        sys.exit(1)
    if dest_sr_ref and len(dest_sr_ref) > 0:
        dest_sr_ref = dest_sr_ref[0]
    else:
        # If no exact match, search for an SR that starts with the specified string
        dest_sr_ref = None
        sr_ref_list = session.xenapi.SR.get_all()
        for sr_ref in sr_ref_list:
            sr = session.xenapi.SR.get_record(sr_ref)
            if sr["type"] != "lvmoiscsi":
                continue
            if sr["name_label"].lower().startswith(dest_sr.lower()):
                dest_sr_ref = sr_ref
                dest_sr = sr["name_label"]
                break
        if not dest_sr_ref:
            mylog.error("Could not find destination SR '" + dest_sr + "'")
            sys.exit(1)

    # Start the clone
    mylog.info("Cloning VM " + vm_name + " to VM " + clone_name + " in SR " + dest_sr + " ...")
    clone_task = None
    try:
        clone_task = session.xenapi.Async.VM.copy(vm_ref, clone_name, dest_sr_ref)
    except XenAPI.Failure as e:
        mylog.error("Could not start clone " + clone_name + ": " + str(e))
        sys.exit(1)

    # Wait for clone to finish
    task_record = None
    progress = 0.0
    while True:
        task_record = session.xenapi.task.get_record(clone_task)
        if task_record["status"] == "pending":
            if task_record["progress"] - progress > 0.1:
                progress = task_record["progress"]
                mylog.info("  " + clone_name + ": %2.1d%%"%(progress*100))
            time.sleep(5)
            continue
        else:
            break
    if task_record["status"] != "success":
        mylog.error("Error cloning " + vm_name + " to " + clone_name + " - " + str(task_record["error_info"]))
        sys.exit(1)

    # Select a host for the clone
    clone_ref = session.xenapi.VM.get_by_name_label(clone_name)
    if not clone_ref or len(clone_ref) <= 0:
        mylog.error("Could not find clone " + clone_name + " after creation")
        sys.exit(1)
    clone_ref = clone_ref[0]
    host_ref_list = session.xenapi.VM.get_possible_hosts(clone_ref)
    min_vms = sys.maxint
    dest_host_ref = None
    dest_host = ""
    for host_ref in host_ref_list:
        h = session.xenapi.host.get_record(host_ref)
        if len(h["resident_VMs"]) < min_vms:
            min_vms = len(h["resident_VMs"])
            dest_host_ref = host_ref
            dest_host = h["name_label"]

    mylog.info("Booting " + clone_name + " on host " + dest_host)
    try:
        session.xenapi.VM.start_on(clone_ref, dest_host_ref, False, False)
    except XenAPI.Failure as e:
        mylog.error("Could not start " + clone_name + " : " + str(e))
        sys.exit(1)

    mylog.passed("Successfully cloned " + vm_name + " to " + clone_name)










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
