#!/usr/bin/python

# This script will create a CHAP account on the cluster for each specified client and configure the client with the CHAP credentials

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                      # The IP addresses of the clients
    "192.168.000.000",              # --client_ips
]

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

account_name = ""               # Specify an account name instead of using the client hostname
                                # --account_name

parallel_thresh = 5             # Do not thread clients unless there are more than this many
                                # --parallel_thresh

parallel_max = 10               # Max number of client threads to use
                                # --parallel_max

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import multiprocessing
import libsf
from libsf import mylog, SfApiError
import libclient
from libclient import ClientError, SfClient


def ClientThread(client_ip, client_user, client_pass, account_name, svip, accounts_list, results, index, debug):
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    mylog.info(client_ip + ": Connecting to client")
    client = SfClient()
    try:
        client.Connect(client_ip, client_user, client_pass)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    if not account_name:
        account_name = client.Hostname
    mylog.debug(client_ip + ": Using account name " + account_name)

    # See if the account already exists
    init_secret = ""
    found = False
    for account in accounts_list["accounts"]:
        if account["username"].lower() == account_name.lower():
            init_secret = account["initiatorSecret"]
            found = True
            break

    if found:
        mylog.info(client_ip + ": Account " + account_name + " already exists.")
    else:
        # Create the account
        mylog.info("Creating account '" + account_name + "'")
        init_secret = libsf.MakeSimpleChapSecret()
        target_secret = libsf.MakeSimpleChapSecret()
        params = {}
        params["username"] = account_name.lower()
        params["initiatorSecret"] = init_secret
        params["targetSecret"] = target_secret
        try:
            result = libsf.CallApiMethod(mvip, username, password, "AddAccount", params, ExitOnError=False)
        except SfApiError as e:
            # Ignore duplicate error, fail on all others
            if e.name != "xDuplicateUsername":
                mylog.error(client_ip + ": Error " + e.name + ": " + e.message)
                return

    # set the CHAP credentials on the client
    mylog.info(client_ip + ": Setting CHAP credentials on " + client.Hostname)
    try:
        client.SetupChap(svip, account_name.lower(), init_secret)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    results[index] = True
    return

def main():
    global client_ips, client_user, client_pass, mvip, username, password, account_name, parallel_thresh, parallel_max

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "client_ips", "client_user", "client_pass", "parallel_thresh", "parallel_max" ]
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
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster [%default]")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster [%default]")
    parser.add_option("--account_name", type="string", dest="account_name", default=account_name, help="the account name to use instead of the client hostname. This will be used for every client!")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    client_user = options.client_user
    client_pass = options.client_pass
    account_name = options.account_name
    parallel_thresh = options.parallel_thresh
    parallel_max = options.parallel_max
    debug = options.debug
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)
    if debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)


    mylog.info("Getting information from the cluster")
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
        th = multiprocessing.Process(target=ClientThread, args=(client_ip, client_user, client_pass, account_name, svip, accounts_list, results, thread_index, debug))
        th.start()
        current_threads.append(th)
        thread_index += 1

        # Wait for at least one thread to finish
        while len(current_threads) >= parallel_clients:
            for i in range(len(current_threads)):
                if not current_threads[i].is_alive():
                    del current_threads[i]
                    break

    # Wait for all threads to stop
    for th in current_threads:
        th.join()
    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False

    if all_success:
        mylog.passed("Successfully created accounts for all clients")
        sys.exit(0)
    else:
        mylog.error("Could not create accounts for all clients")
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
