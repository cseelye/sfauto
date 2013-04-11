#!/usr/bin/python

# This script will set the hostname on a client

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ip = "192.168.000.000"       # The IP addresses of the client
                                    # --client_ip

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

new_hostname = ""                   # The new hostname for the client
                                    # --hostname

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
    # Parse command line arguments
    parser = OptionParser()
    global client_ip, client_user, client_pass, new_hostname
    parser.add_option("--client_ip", type="string", dest="client_ip", default=client_ip, help="the IP addresses of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the client [%default]")
    parser.add_option("--hostname", type="string", dest="new_hostname", default=new_hostname, help="the new hostname for the client")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    client_ip = options.client_ip
    new_hostname = options.new_hostname
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not new_hostname:
        mylog.error("Please enter a hostname")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(client_ip):
        mylog.error("'" + client_ip + "' does not appear to be a valid IP address")
        sys.exit(1)

    client = SfClient()
    mylog.info("Connecting to client '" + client_ip + "'")
    try:
        client.Connect(client_ip, client_user, client_pass)
    except ClientError as e:
        mylog.error(e)
        sys.exit(1)

    mylog.info("Setting hostname on " + client.Hostname)
    try:
        client.UpdateHostname(new_hostname)
    except ClientError as e:
        mylog.error(e.message)
        sys.exit(1)

    mylog.passed("Successfully set hostname")


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




