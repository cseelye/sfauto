#!/usr/bin/python

# This script will delete CHAP accounts on the cluster that correspond to the specified clients

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

mvip = "192.168.000.000"    # The management VIP of the cluster
                            # --mvip

username = "admin"          # Admin account for the cluster
                            # --user

password = "password"      # Admin password for the cluster
                            # --pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import libsf
from libsf import mylog
import libclient
from libclient import SfClient, ClientError

def main():
    global client_ips, client_user, client_pass, mvip, username, password

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "client_ips", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ips", type="string", dest="client_ips", default=",".join(client_ips), help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
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

    # Find the account to delete
    mylog.info("Getting a list of accounts")
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
    
    for client_ip in client_ips:
        mylog.info(client_ip + ": Connecting to client")
        client = SfClient()
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            return

        account_id = 0
        for account in accounts_list["accounts"]:
            if account["username"].lower() == client.Hostname.lower():
                account_id = account["accountID"]
                break
        if account_id == 0:
            mylog.passed(client_ip + ": Account does not exist or has already been deleted")
            continue
    
        mylog.info(client_ip + ": Deleting account ID " + str(account_id))
        result = libsf.CallApiMethod(mvip, username, password, "RemoveAccount", {"accountID" : account_id})

        mylog.passed(client_ip + ": Successfully deleted account " + client.Hostname)

    mylog.passed("Successfully deleted all accounts")

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







