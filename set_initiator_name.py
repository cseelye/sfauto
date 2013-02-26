#!/usr/bin/python

# This script will set the iSCSI initiator name on a client

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

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import paramiko
import re
import socket
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient


def main():
    global client_ips, client_user, client_pass

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "client_ips", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ips", type="string", dest="client_ips", default=",".join(client_ips), help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
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


    error = False
    for client_ip in client_ips:
        client = SfClient()
        mylog.info("Connecting to client '" + client_ip + "'")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(e)
            sys.exit(1)

        mylog.info("Setting initiator name on " + client.Hostname)
        try:
            client.UpdateInitiatorName()
        except ClientError as e:
            mylog.error(e)
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




