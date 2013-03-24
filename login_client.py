#!/usr/bin/python

# This script will log in to all iscsi volumes on a list of clients

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                  # The IP addresses of the clients
    "192.168.000.000",          # --client_ips
]

client_user = "root"            # The username for the client
                                # --client_user

client_pass = "password"       # The password for the client
                                # --client_pass

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

login_order = "serial"          # How to log in to volumes - all at the same time (parallel)
                                # or seqentially (serial) in order by iqn
                                # --login_order

account_name = ""               # Specify an account name instead of using the client hostname
                                # --account_name

target_list = [                 # The list of target IQNs to log in to
                                # Leave empty to log in to all volumes
]                               # --target_list

parallel_thresh = 5             # Do not thread clients unless there are more than this many
                                # --parallel_thresh

parallel_max = 10               # Max number of client threads to use
                                # --parallel_max

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import paramiko
import re
import socket
import platform
import time
import string
import multiprocessing
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient, OsType


def ClientThread(client_ip, client_user, client_pass, account_name, target_list, mvip, svip, login_order, accounts_list, results, index, debug=None):
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    client = SfClient()
    mylog.info(client_ip + ": Connecting to client")
    try:
        client.Connect(client_ip, client_user, client_pass)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    if not account_name:
        account_name = client.Hostname
    mylog.debug(client_ip + ": Using account name " + account_name)

    expected = 0

    # See if there is an sf account with this name already created
    mylog.info(client_ip + ": Looking for account '" + account_name + "' on cluster '" + mvip + "'")
    init_password = None
    account_id = None
    for account in accounts_list["accounts"]:
        if (account["username"].lower() == account_name.lower()):
            account_id = account["accountID"]
            init_password = account["initiatorSecret"]
            expected = len(account["volumes"])
            break

    # If this is a Windows client, make sure the CHAP secret is alphanumeric
    if account_id and client.RemoteOs == OsType.Windows:
        if not init_password.isalnum():
            init_password = libsf.MakeSimpleChapSecret()
            mylog.info(client_ip + ": Resetting CHAP password to '" + init_password + "'")
            params = {}
            params["accountID"] = account_id
            params["initiatorSecret"] = init_password
            params["targetSecret"] = libsf.MakeSimpleChapSecret()
            result = libsf.CallApiMethod(mvip, username, password, "ModifyAccount", params)

    # Create an account if we couldn't find one
    if (account_id == None):
        mylog.info("Creating new account on cluster " + mvip)
        params = {}
        params["username"] = account_name.lower()
        params["initiatorSecret"] = libsf.MakeSimpleChapSecret()
        params["targetSecret"] = libsf.MakeSimpleChapSecret()
        result = libsf.CallApiMethod(mvip, username, password, "AddAccount", params)
        account_id = result["accountID"]
        params = {}
        params["accountID"] = account_id
        result = libsf.CallApiMethod(mvip, username, password, "GetAccountByID", params)
        init_password = result["account"]["initiatorSecret"]

    mylog.info(client_ip + ": Using account " + str(account_id) + " with password '" + init_password + "'")

    # Clean iSCSI if there aren't already volumes logged in
    targets = []
    try:
        targets = client.GetLoggedInTargets()
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    if not targets or len(targets) <= 0:
        try:
            mylog.info(client_ip + ": Cleaning iSCSI on client '" + client.Hostname + "'")
            client.CleanIscsi()
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            return

    # Add the CHAP credentials to the client
    mylog.info(client_ip + ": Setting up iSCSI CHAP on client '" + client.Hostname + "'")
    try:
        client.SetupChap(svip, account_name.lower(), init_password)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    # Do an iSCSI discovery
    mylog.info(client_ip + ": Discovering iSCSI volumes on client '" + client.Hostname + "' at VIP '" + svip + "'")
    try:
        client.RefreshTargets(svip, expected)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    # Log in to all volumes
    mylog.info(client_ip + ": Logging in to iSCSI volumes on client '" + client.Hostname + "'")
    try:
        client.LoginTargets(svip, login_order, target_list)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    # List out the volumes and their info
    mylog.info(client_ip + ": Gathering information about connected volumes...")
    volumes = client.GetVolumeSummary()
    if volumes:
        for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v["iqn"]):
            if "sid" not in volume.keys(): volume["sid"] = "unknown"
            outstr = "   " + volume["iqn"] + " -> " + volume["device"] + ", SID: " + volume["sid"] + ", SectorSize: " + volume["sectors"] + ", Portal: " + volume["portal"]
            if "state" in volume:
                outstr += ", Session: " + volume["state"]
            mylog.info(outstr)

    results[index] = True
    return

def main():
    global client_ips, client_user, client_pass, login_order, account_name, target_list, mvip, username, password, parallel_thresh, parallel_max

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "client_ips", "client_user", "client_pass", "login_order", "parallel_thresh", "parallel_max" ]
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
    parser.add_option("--login_order", type="string", dest="login_order", default=login_order, help="login order for volumes (parallel or serial)")
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the account name to use instead of the client hostname. This will be used for every client!")
    parser.add_option("--target_list", type="string", dest="target_list", default=target_list, help="the list of volume IQNs to log in to, instead of all volumes")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    mvip = options.mvip
    username = options.username
    password = options.password
    login_order = options.login_order
    account_name = options.account_name
    debug = options.debug
    target_list = []
    if options.target_list:
        for t in options.target_list.split(","):
            target_list.append(t.strip())

    parallel_thresh = options.parallel_thresh
    parallel_max = options.parallel_max
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("MVIP '" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)
    if (login_order != "parallel" and login_order != "serial"):
        mylog.warning("Unknown login order specified; assuming serial")
        login_order = "serial"


    # Get the SVIP of the cluster
    cluster_info = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
    svip = cluster_info["clusterInfo"]["svip"]

    # Get a list of accounts from the cluster
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})

    # Run the client operations in parallel if there are enough clients
    if len(client_ips) <= parallel_thresh:
        parallel_clients = 1
    else:
        parallel_clients = parallel_max

    # Start the client threads
    manager = multiprocessing.Manager()
    results = manager.dict()
    current_threads = []
    thread_index = 0
    for client_ip in client_ips:
        results[thread_index] = False
        th = multiprocessing.Process(target=ClientThread, args=(client_ip, client_user, client_pass, account_name, target_list, mvip, svip, login_order, accounts_list, results, thread_index, debug))
        th.start()
        current_threads.append(th)
        thread_index += 1

        # Wait for at least one thread to finish
        while len(current_threads) >= parallel_clients:
            for i in range(len(current_threads)):
                if not current_threads[i].is_alive():
                    del current_threads[i]
                    break

    # Wait for all threads to be done
    for th in current_threads:
        th.join()
    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False

    if all_success:
        mylog.passed("Successfully logged in to volumes on all clients")
        sys.exit(0)
    else:
        mylog.error("Could not log in to all volumes on all clients")
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
