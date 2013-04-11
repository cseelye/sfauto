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

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

account_name = ""               # SolidFire CHAP account name
                                # --account_name

vag_name = ""                   # SolidFire VAG name
                                # --vag_name

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
    global vmhost, host_user, host_pass, mvip, username, password, account_name, vag_name
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the SolidFire cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the SolidFire cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the SolidFire cluster")
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the SolidFire CHAP account name for the hypervisor")
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the SolidFire VAG name for the hypervisor")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    mvip = options.mvip
    username = options.username
    password = options.password
    account_name = options.account_name
    vag_name = options.vag_name
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid hypervisor IP")
        sys.exit(1)

    chap_user = None
    chap_pass = None
    expected_volumes = 0
    if account_name:
        # Find the account on the SF cluster
        mylog.info("Looking for account '" + account_name + "' on cluster '" + mvip + "'")
        accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        sfaccount = None
        for account in accounts_list["accounts"]:
            if (account["username"].lower() == account_name.lower()):
                sfaccount = account
                break
        if not sfaccount:
            mylog.error("Could not find CHAP account " + account_name)
            sys.exit(1)
        chap_user = account_name
        chap_pass = sfaccount["initiatorSecret"]
        expected_volumes = len(sfaccount["volumes"])
    elif vag_name:
        vag = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=vag_name)
        expected_volumes = len(vag["volumes"])

    # Get the SVIP of the SF cluster
    cluster_info = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
    svip = cluster_info["clusterInfo"]["svip"]

    # Connect to the host/pool
    mylog.info("Connecting to " + vmhost)
    session = None
    try:
        session = libxen.Connect(vmhost, host_user, host_pass)
    except libxen.XenError as e:
        mylog.error(str(e))
        sys.exit(1)

    # Get a list of already existing SRs
    existing_srs = dict()
    sr_ref_list = session.xenapi.SR.get_all()
    for sr_ref in sr_ref_list:
        sr = session.xenapi.SR.get_record(sr_ref)
        if sr["type"] != "lvmoiscsi":
            continue
        pbd = session.xenapi.PBD.get_record(sr["PBDs"][0])
        iqn = pbd["device_config"]["targetIQN"]
        existing_srs[iqn] = sr["name_label"]


    # Find the specified vmhost
    xen_host = None
    host_list = session.xenapi.host.get_all()
    for host_ref in host_list:
        h = session.xenapi.host.get_record(host_ref)
        if h["address"] == vmhost:
            xen_host = host_ref
            break

    # Get the list of targets
    mylog.info("Discovering iSCSI volumes")
    target_iqns = dict()
    try:
        target_iqns = libxen.GetIscsiTargets(session, xen_host, svip, chap_user, chap_pass)
    except libxen.XenError as e:
        mylog.error(str(e))
        sys.exit(1)
    mylog.debug("Found " + str(len(target_iqns)) + " iSCSI targets")
    if len(target_iqns) != expected_volumes:
        mylog.debug("Discovered " + str(len(target_iqns)) + " targets but expected " + str(expected_volumes) + " targets")
        sys.exit(1)

    # Create an SR on each target
    target_list = dict()
    for iqn in sorted(target_iqns):
        if iqn in existing_srs:
            mylog.info(iqn + " is already used by SR " + existing_srs[iqn])
            continue

        mylog.info("Probing SCSI LUN on " + iqn)
        scsi_id = None
        sr_size = None
        retry = 3
        wait = 20
        while True:
            try:
                scsi_id, sr_size = libxen.GetScsiLun(session, xen_host, iqn, svip, chap_user, chap_pass)
                break
            except libxen.XenError as e:
                retry -= 1
                if retry <= 0:
                    mylog.error(str(e))
                    sys.exit(1)
                else:
                    mylog.warning(str(e))
                    mylog.warning("Retrying in " + str(wait) + " sec...")
                    time.sleep(wait)

        desc = "iSCSI SR [" + svip + " (" + iqn + ")]"
        iqn_pieces = iqn.split('.')
        sr_name = iqn_pieces[4] + "." + iqn_pieces[5]

        mylog.info("Creating SR " + sr_name + " (" + str(sr_size/1000/1000/1000) + "GB) SCSIid " + scsi_id)
        sr_args = {
                    "target": svip,
                    "targetIQN": iqn,
                    "SCSIid": scsi_id
        }
        if chap_user:
            sr_args["chapuser"] = chap_user
        if chap_pass:
            sr_args["chappassword"] = chap_pass
        sr_type = "lvmoiscsi"
        retry = 3
        wait = 20
        while True:
            try:
                # The size arg is a string because the Xen XML-RPC implementation chokes on integers that are this large
                session.xenapi.SR.create(xen_host, sr_args, str(sr_size), sr_name, desc, sr_type, "user", True)
                break
            except XenAPI.Failure as e:
                retry -= 1
                if retry <= 0:
                    mylog.error("Could not create SR for target " + iqn + " - " + str(e))
                    sys.exit(1)
                else:
                    mylog.warning("Could not create SR for target " + iqn + " - " + str(e))
                    mylog.warning("Retrying in " + str(wait) + " sec...")
                    time.sleep(wait)
        mylog.passed("  Successfully created SR " + sr_name)

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
