#!/usr/bin/python

# This script will remove clients from a VAG

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                  # The IP addresses of the clients
    "192.168.000.000",          # --client_ips
]

client_user = "root"            # The username for the client
                                # --client_user

client_pass = "solidfire"       # The password for the client
                                # --client_pass

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "solidfire"              # Admin password for the cluster
                                    # --pass

vag_name = ""                       # The name of the group to remove from
                                    # --vag_name

vag_id = 0                          # The ID of the group to remove from
                                    # --vag_id

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog, SfError
import libclient
from libclient import ClientError, SfClient, OsType


def main():
    global mvip, username, password, client_ips, client_user, client_pass, vag_name, vag_id

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "client_ips", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]
    if isinstance(client_ips, basestring):
        client_ips = client_ips.split(",")

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ips", type="string", dest="client_ips", default=",".join(client_ips), help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--vag_name", type="string", dest="vag_name", default=vag_name, help="the name of the group")
    parser.add_option("--vag_id", type="int", dest="vag_id", default=vag_id, help="the ID of the group")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    vag_name = options.vag_name
    vag_id = options.vag_id
    client_user = options.client_user
    client_pass = options.client_pass
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # Find the group
    try:
        vag = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=vag_name, VagId=vag_id)
    except SfError as e:
        mylog.error(str(e))
        sys.exit(1)

    # Get a list of initiator IQNs from the clients
    new_iqn_list = []
    for client_ip in client_ips:
        client = SfClient()
        mylog.info("Connecting to client '" + client_ip + "'")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(e)
            sys.exit(1)
        iqn = client.GetInitiatorName()
        mylog.info(client.Hostname + " has IQN " + iqn)
        if iqn in new_iqn_list:
            mylog.error("Duplicate IQN")
            sys.exit(1)
        new_iqn_list.append(iqn)

    # Append the new IQNs to the list
    full_iqn_list = vag["initiators"]
    for iqn in new_iqn_list:
        if iqn.lower() in vag["initiators"]:
            full_iqn_list.remove(iqn)
        else:
            mylog.debug(iqn + " is alreadynot in group")

    # Modify the VAG on the cluster
    mylog.info("Removing clients from group")
    params = {}
    params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
    params["initiators"] = full_iqn_list
    libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    mylog.passed("Successfully removed clients from group")


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
