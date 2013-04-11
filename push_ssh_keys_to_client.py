#!/usr/bin/python

# This script will push your local SSH RSA key to a list of clients, to enable password-less SSH login

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


import sys, os
from optparse import OptionParser
import paramiko
import re
import socket
import platform
import time
import getpass
import os
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
    if isinstance(client_ips, basestring):
        client_ips = client_ips.split(",")


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

    # Get local hostname
    local_hostname = platform.node()

    # Get current user
    local_username = getpass.getuser()
    home = os.path.expanduser("~")

    # Look for or create local RSA id
    key_text = ""
    key_path = home + "/.ssh/id_rsa.pub"
    if not os.path.exists(key_path):
        mylog.info("Creating SSH key for " + local_hostname)
        libsf.RunCommand("ssh-keygen -q -f ~/.ssh/id_rsa -N \"\"")
    with open(key_path) as f: key_text = f.read()
    m = re.search(local_username + "\@" + local_hostname, key_text)
    if not m:
        mylog.warning("The SSH key in " + key_path + " doesn't use the current username/hostname")
    key_text = key_text.rstrip()

    # Send the key over to each client
    for client_ip in client_ips:
        client = SfClient()
        mylog.info("Connecting to client '" + client_ip + "'")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(e)
            sys.exit(1)

        if client.RemoteOs == libclient.OsType.Windows:
            mylog.passed("Skipping Windows client")
            continue
        
        # Make sure the .ssh directory exists
        retcode, stdout, stderr = client.ExecuteCommand("find ~ -maxdepth 1 -name \".ssh\" -type d | wc -l")
        if not int(stdout):
            client.ExecuteCommand("mkdir ~/.ssh")
        client.ExecuteCommand("chmod 700 ~/.ssh")

        # See if the key is already on the client and add it if it isn't
        found = False
        retcode, stdout, stderr = client.ExecuteCommand("cat ~/.ssh/authorized_keys")
        for line in stdout.split("\n"):
            #print line
            if line == key_text:
                found = True
                break
        if found:
            mylog.info("Key is already on client " + client.Hostname)
        else:
            mylog.info("Adding key to " + client.Hostname)
            client.ExecuteCommand("echo \"" + key_text + "\" >> ~/.ssh/authorized_keys")
        client.ExecuteCommand("chmod 600 ~/.ssh/authorized_keys")
        mylog.passed("Pushed key to " + client.Hostname)

    mylog.passed("Successfully pushed SSH keys to all clients")


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


