#!/usr/bin/python

# This script will create SRs from available iSCSI volumes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --host_ip

host_user = "root"              # The username for the hypervisor
                                # --client_user

host_pass = "solidfire"         # The password for the hypervisor
                                # --client_pass

# ----------------------------------------------------------------------------

import sys, os
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
    global vmhost, host_user, host_pass, mvip, username, password, account_name
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
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

    # Make a list of icsi SRs
    sr_list = dict()
    sr_ref_list = session.xenapi.SR.get_all()
    for sr_ref in sr_ref_list:
        sr = session.xenapi.SR.get_record(sr_ref)
        if sr["type"] != "lvmoiscsi":
            continue
        sr_list[sr["name_label"]] = sr_ref

    # For each SR, "plug" each Physical Block Device
    for sr_name in sorted(sr_list.keys()):
        sr_ref = sr_list[sr_name]
        sr = session.xenapi.SR.get_record(sr_ref)
        pbd = session.xenapi.PBD.get_record(sr["PBDs"][0])
        iqn = pbd["device_config"]["targetIQN"]
        mylog.info("Repairing " + sr["name_label"] + " (" + iqn + ")")
        for pbd_ref in sr["PBDs"]:
            pbd = session.xenapi.PBD.get_record(pbd_ref)
            host = session.xenapi.host.get_record(pbd["host"])
            if pbd["currently_attached"]:
                mylog.info("  Already attached to " + host["name_label"])
                continue

            mylog.info("  Scan and attach device on " + host["name_label"] + " ...")
            retry = 3
            wait = 10
            while True:
                try:
                    session.xenapi.PBD.plug(pbd_ref)
                except XenAPI.Failure as e:
                    mylog.error("  Could not plug PBD - " + str(e))
                    retry -= 1
                    if retry <= 0:
                        sys.exit(1)
                    else:
                        mylog.info("    Retrying in " + str(wait) + " sec...")
                        time.sleep(wait)

    mylog.passed("Successfully repaired all SRs")
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
