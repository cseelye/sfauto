#!/usr/bin/python

"""
This action will set the IP address of a client

When run as a script, the following options/env variables apply:
    --client_ip        The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --new_ip            The new IP address

    --new_netmask       The new netmask

    --new _gateway      The new gateway

    --interface_name    The name of the interface to set the IP on

    --interface_mac     The MAC address of the interface to set the IP on

    --noupdate_hosts    Do not update the hosts file with the new IP
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SetClientIpAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_CLIENT_IP = "BEFORE_SET_CLIENT_IP"
        AFTER_SET_CLIENT_IP = "AFTER_SET_CLIENT_IP"
        SET_CLIENT_IP_FAILED = "SET_CLIENT_IP_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIP" : libsf.IsValidIpv4Address,
                            "newIP" : libsf.IsValidIpv4Address},
            args)
        if "interfaceName" not in args and "interfaceMac" not in args:
            raise libsf.SfArgumentError("Please specify interfaceName or interfaceMac")
        if not args["interfaceName"] and not args["interfaceMac"]:
            raise libsf.SfArgumentError("Please specify interfaceName or interfaceMac")

    def Execute(self, clientIP, newIP, newNetmask, newGateway=None, interfaceName=None, interfaceMac=None, updateHosts=True, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, debug=False):
        """
        Change the IP address of a client
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        client = SfClient()
        mylog.info("Connecting to client '" + clientIP + "'")
        try:
            client.Connect(clientIP, clientUser, clientPass)
        except ClientError as e:
            mylog.error(e.message)
            return False

        self._RaiseEvent(self.Events.BEFORE_SET_CLIENT_IP, clientIP=clientIP)
        if interfaceName:
            mylog.info("Setting IP to " + newIP + " on interface " + interfaceName)
            try:
                client.ChangeIpAddress(NewIp=newIP, NewMask=newNetmask, NewGateway=newGateway, InterfaceName=interfaceName, UpdateHosts=updateHosts)
            except ClientError as e:
                mylog.error(e.message)
                self._RaiseEvent(self.Events.SET_CLIENT_IP_FAILED)
                return False
        elif interfaceMac:
            mylog.info("Setting IP to " + newIP + " on interaface with MAC " + interfaceMac)
            try:
                client.ChangeIpAddress(NewIp=newIP, NewMask=newNetmask, NewGateway=newGateway, InterfaceMac=interfaceMac, UpdateHosts=updateHosts)
            except ClientError as e:
                mylog.error(e.message)
                self._RaiseEvent(self.Events.SET_CLIENT_IP_FAILED, clientIP=clientIP, exception=e)
                return False

        mylog.passed("Successfully set IP address")
        self._RaiseEvent(self.Events.AFTER_SET_CLIENT_IP, clientIP=clientIP)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(description="Set the IP address of a client on a specified interface")
    parser.add_option("--client_ip", type="string", dest="client_ip", default=None, help="the IP addresses of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--interface_name", type="string", dest="interface_name", default=None, help="the name of the interface to set the IP on. Specify name or MAC but not both")
    parser.add_option("--interface_mac", type="string", dest="interface_mac", default=None, help="the MAC address of the interface to set the IP on. Specify name or MAC but not both")
    parser.add_option("--new_ip", type="string", dest="new_ip", default=None, help="the new IP address for the client")
    parser.add_option("--new_netmask", type="string", dest="new_netmask", default=None, help="the new netmask for the client")
    parser.add_option("--new_gateway", type="string", dest="new_gateway", default=None, help="the new gateway for the client")
    parser.add_option("--noupdate_hosts", action="store_false", dest="update_hosts", default=True, help="do not update the hosts file with this IP")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.new_ip, options.new_netmask, options.new_gateway, options.interface_name, options.interface_mac, options.update_hosts, options.client_user, options.client_pass, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

