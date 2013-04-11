#!/usr/bin/python

# This script will list out all of the connected iSCSI volumes

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                      # The IP addresses of the clients
    "192.168.154.000",              # --client_ips
]

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

sort = "iqn"                        # The sorting order for the results
                                    # --sort
# ----------------------------------------------------------------------------


from optparse import OptionParser
#import paramiko
#import re
#import socket
#import platform
#import time
#import subprocess
import sys,os
import libsf
from libsf import mylog
#import libclient
from libclient import SfClient, ClientError, OsType

#def ExecCommand(pCommand, pSshConnection=None):
#    if pSshConnection != None:
#        stdin, stdout, stderr = libsf.ExecSshCommand(pSshConnection, pCommand)
#        return stdout.readlines()
#    else:
#        p = subprocess.Popen(pCommand, shell=True, stdout=subprocess.PIPE)
#        stdout = p.communicate()[0]
#        return stdout.split("\n")

def main():
    global client_ips, client_user, client_pass, sort

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
    parser.add_option("--sort", type="string", dest="sort", default=sort, help="the sorting order for the results (device, iqn, portal, state)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    sort = options.sort
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    
    
    for client_ip in client_ips:
        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            return
        if client.RemoteOs != OsType.Linux:
            mylog.error("Sorry, this currently only works on Linux")
            exit(1)
        
        mylog.info(client_ip + ": Gathering information about connected volumes...")
        volumes = client.GetVolumeSummary()
        
        if (sort != "device" and sort != "portal" and sort != "iqn" and sort != "state"):
            #mylog.warning("Invalid sort; assuming 'iqn'")
            sort = "iqn"
        
        mylog.info(client_ip + ": Found " + str(len(volumes.keys())) + " iSCSI volumes on " + client.Hostname + ":")
        for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v[sort]):
            if "sid" not in volume.keys(): volume["sid"] = "unknown"
            outstr = "   " + volume["iqn"] + " -> " + volume["device"] + ", SID: " + volume["sid"] + ", SectorSize: " + volume["sectors"] + ", Portal: " + volume["portal"]
            if "state" in volume:
                outstr += ", Session: " + volume["state"]
            mylog.info(outstr)


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






