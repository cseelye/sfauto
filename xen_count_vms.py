#!/usr/bin/python

# This script will count the number of VMs that match a prefix

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"        # The IP address of the hypervisor
                                # --host_ip

host_user = "root"                # The username for the hypervisor
                                # --client_user

host_pass = "solidfire"           # The password for the hypervisor
                                # --client_pass

vm_prefix = ""                    # The prefix of the VM names to match
                                # --vm_name

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
import libsf
from libsf import mylog
import libxen

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

    # Connect to the host/pool
    mylog.info("Connecting to " + vmhost)
    session = None
    try:
        session = libxen.Connect(vmhost, host_user, host_pass)
    except libxen.XenError as e:
        mylog.error(str(e))
        sys.exit(1)

    mylog.info("Searching for matching VMs")
    # Get a list of all VMs
    try:
        vm_ref_list = session.xenapi.VM.get_all()
    except XenAPI.Failure as e:
        mylog.error("Could not get VM list: " + str(e))
        sys.exit(1)

    # Count the VMs that have the specified prefix and end in a number
    matching_vms = 0
    for vm_ref in vm_ref_list:
        vm = session.xenapi.VM.get_record(vm_ref)
        m = re.search("^" + vm_prefix + "0*(\d+)$", vm["name_label"])
        if m: matching_vms += 1

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
