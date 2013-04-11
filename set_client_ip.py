#!/usr/bin/python

# This script will set the IP address of a client

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ip = "192.168.000.000"       # The IP addresses of the client
                                    # --client_ip

client_user = "root"                # The username for the client
                                    # --client_user

client_pass = "password"           # The password for the client
                                    # --client_pass

interface_name = ""                 # The name of the interface to set the IP on
                                    # Either interface_name or interface_mac are required
                                    # --interface_name

interface_mac  = ""                 # The MAC address of the interface to set the IP on
                                    # Either interface_name or interface_mac are required
                                    # --interface_mac

new_ip = ""                         # The new IP address for the client
                                    # --new_ip

new_netmask = ""                    # The new netmask for the client
                                    # --new_ip

new_gateway = ""                    # The new gateway for the client
                                    # --new_gateway

update_hosts = True                 # Use this IP as the /etc/hosts entry
                                    # --update_hosts

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
    parser = OptionParser(description="Set the IP address of a client on a specified interface")
    global client_ip, client_user, client_pass, new_ip, new_netmask, new_gateway, interface_name, interface_mac, update_hosts
    parser.add_option("--client_ip", type="string", dest="client_ip", default=client_ip, help="the IP addresses of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the client [%default]")
    parser.add_option("--interface_name", type="string", dest="interface_name", default=interface_name, help="the name of the interface to set the IP on. Specify name or MAC but not both")
    parser.add_option("--interface_mac", type="string", dest="interface_mac", default=interface_mac, help="the MAC address of the interface to set the IP on. Specify name or MAC but not both")
    parser.add_option("--new_ip", type="string", dest="new_ip", default=new_ip, help="the new IP address for the client")
    parser.add_option("--new_netmask", type="string", dest="new_netmask", default=new_netmask, help="the new netmask for the client")
    parser.add_option("--new_gateway", type="string", dest="new_gateway", default=new_gateway, help="the new gateway for the client")
    parser.add_option("--noupdate_hosts", action="store_true", dest="noupdate_hosts", help="do not update the hosts file with this IP")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    client_ip = options.client_ip
    new_ip = options.new_ip
    new_netmask = options.new_netmask
    new_gateway = options.new_gateway
    interface_mac = options.interface_mac
    interface_name = options.interface_name
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if options.noupdate_hosts:
        update_hosts = False
    if interface_mac and interface_name:
        mylog.error("Please specify interface_mac or interface_name but not both")
        sys.exit(1)
    if not interface_mac and not interface_name:
        mylog.error("Please specify interface_mac or interface_name")
        sys.exit(1)
    if not new_ip:
        mylog.error("Please specify a new IP address")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(new_ip):
        mylog.error("'" + new_ip + "' does not appear to be a valid IP address")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(client_ip):
        mylog.error("'" + client_ip + "' does not appear to be a valid IP address")
        sys.exit(1)

    client = SfClient()
    mylog.info("Connecting to client '" + client_ip + "'")
    try:
        client.Connect(client_ip, client_user, client_pass)
    except ClientError as e:
        mylog.error(e.message)
        sys.exit(1)

    if interface_name:
        mylog.info("Setting IP to " + new_ip + " on interaface " + interface_name)
        try:
            client.ChangeIpAddress(NewIp=new_ip, NewMask=new_netmask, NewGateway=new_gateway, InterfaceName=interface_name, UpdateHosts=update_hosts)
        except ClientError as e:
            mylog.error(e.message)
            sys.exit(1)
    elif interface_mac:
        mylog.info("Setting IP to " + new_ip + " on interaface with MAC " + interface_mac)
        try:
            client.ChangeIpAddress(NewIp=new_ip, NewMask=new_netmask, NewGateway=new_gateway, InterfaceMac=interface_mac, UpdateHosts=update_hosts)
        except ClientError as e:
            mylog.error(e.message)
            sys.exit(1)

    mylog.passed("Successfully set IP address")


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




