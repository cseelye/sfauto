#!/usr/bin/python

# This script will count all of the connected iSCSI volumes and make sure the expected number is present

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

expected = 0                        # The nuber of drives to expect
                                    # --expected
# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import paramiko
import re
import socket
import platform
import time
import libsf
from libsf import mylog

def main():
    global client_ips, client_user, client_pass, expected

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
    parser.add_option("--expected", type="int", dest="expected", default=expected, help="the number of drives to expect")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    expected = options.expected
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


    exit_code = 0
    for client_ip in client_ips:
        # Connect to client
        ssh = libsf.ConnectSsh(client_ip, client_user, client_pass)

        # Get the hostname of the client
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "hostname")
        client_hostname = stdout.readlines()[0].strip()

        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "iscsiadm -m session -P 3 | egrep \"Target|Current Portal|iSCSI Session State|Attached scsi\"")
        data = stdout.readlines()
        new_volume = None
        volumes = dict()
        for line in data:
            m = re.search("Target:\s+(.+)", line)
            if(m):
                new_volume = dict()
                new_volume["iqn"] = m.group(1)

            m = re.search("Current Portal:\s+(.+):", line)
            if(m):
                new_volume["portal"] = m.group(1)

            m = re.search("Session State:\s+(.+)", line)
            if(m):
                new_volume["state"] = m.group(1)

            m = re.search("disk\s+(\S+)\s", line)
            if(m):
                new_volume["device"] = m.group(1)
                # we don't add it to the dict until we find a valid device
                volumes[m.group(1)] = new_volume

        volume_count = len(volumes)
        if (volume_count == expected):
            mylog.passed("Found " + str(volume_count) + " iSCSI volumes on client '" + client_hostname + "'")
        else:
            mylog.error("Expected " + str(expected) + " but found " + str(volume_count) + " iSCSI volumes on client '" + client_hostname + "'")
            exit_code = 1

    exit(exit_code)


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







