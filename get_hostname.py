#!/usr/bin/python

# This script will print the hostname of a client

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ip = "192.168.000.000"       # The IP address of the client
                                    # --client_ips

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

# ----------------------------------------------------------------------------


from optparse import OptionParser
import paramiko
import sys,os
import libsf
from libsf import mylog
import libclient
from libclient import ClientError, SfClient

def main():
    global client_ip, client_user, client_pass

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "client_ip", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ip", type="string", dest="client_ip", default=client_ip, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the client [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    client_ip = options.client_ip
    if not libsf.IsValidIpv4Address(client_ip):
        mylog.error("'" + client_ip + "' does not appear to be a valid IP address")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    try:
        client = SfClient()
        client.Connect(client_ip, client_user, client_pass)
        sys.stdout.write(client.Hostname)
        sys.stdout.write("\n")
        sys.stdout.flush()
    except:
        exit(1)


if __name__ == '__main__':
    #mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        #mylog.warning("Aborted by user")
        exit(1)
    except:
        #mylog.exception("Unhandled exception")
        exit(1)
    exit(0)


