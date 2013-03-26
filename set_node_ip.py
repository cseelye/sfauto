#!/usr/bin/python

# This script will set the network configuration of a node

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

node_ip = "192.168.000.000"     # The current IP addresse of the node
                                # --node_ip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

oneg_ip = ""                    # The new IP address for the node
                                # --oneg_ip

oneg_netmask = ""               # The new netmask for the node
                                # --oneg_netmask

oneg_gateway = ""               # The new gateway for the node
                                # --oneg_gateway

teng_ip = ""                    # The new IP address for the node
                                # --teng_ip

teng_netmask = ""               # The new netmask for the node
                                # --teng_netmask

dns_ip = ""                     # The DNS IP address for the node
                                # --dns_search

dns_search = ""                 # The DNS search path for the node
                                # --dns_search

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
    global node_ip, username, password, oneg_ip, oneg_netmask, oneg_gateway, teng_ip, teng_netmask, dns_ip, dns_search

    # Parse command line arguments
    parser = OptionParser(description="Set the network information of a node")
    parser.add_option("--node_ip", type="string", dest="node_ip", default=node_ip, help="the current IP address of the node")
    parser.add_option("--username", type="string", dest="username", default=username, help="the username for the node [%default]")
    parser.add_option("--password", type="string", dest="password", default=password, help="the password for the node [%default]")
    parser.add_option("--oneg_ip", type="string", dest="oneg_ip", default=oneg_ip, help="the new 1G IP address for the node")
    parser.add_option("--oneg_netmask", type="string", dest="oneg_netmask", default=oneg_netmask, help="the new 1G netmask for the node")
    parser.add_option("--oneg_gateway", type="string", dest="oneg_gateway", default=oneg_gateway, help="the new 1G gateway for the node")
    parser.add_option("--teng_ip", type="string", dest="teng_ip", default=teng_ip, help="the new 10G IP address for the node")
    parser.add_option("--teng_netmask", type="string", dest="teng_netmask", default=teng_netmask, help="the new 10G netmask for the node")
    parser.add_option("--dns_ip", type="string", dest="dns_ip", default=dns_ip, help="the new DNS IP address for the node")
    parser.add_option("--dns_search", type="string", dest="dns_search", default=dns_search, help="the new DNS search path for the node")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    node_ip = options.node_ip
    username = options.username
    passowrd = options.password
    oneg_ip = options.oneg_ip
    oneg_netmask = options.oneg_netmask
    oneg_gateway = options.oneg_gateway
    teng_ip = options.teng_ip
    teng_netmask = options.teng_netmask
    dns_ip = options.dns_ip
    dns_search = options.dns_search
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    params = {}
    params["network"] = {}
    params["network"]["Bond1G"] = {}
    params["network"]["Bond1G"]["address"] = oneg_ip
    params["network"]["Bond1G"]["netmask"] = oneg_netmask
    params["network"]["Bond1G"]["gateway"] = oneg_gateway
    params["network"]["Bond1G"]["dns-nameservers"] = dns_ip
    params["network"]["Bond1G"]["dns-search"] = dns_search
    params["network"]["Bond10G"] = {}
    params["network"]["Bond10G"]["address"] = teng_ip
    params["network"]["Bond10G"]["netmask"] = teng_netmask
    result = libsf.CallNodeApiMethod(node_ip, username, password, "SetConfig", params)

    mylog.passed("Successfully set network info")


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




