#!/usr/bin/python

"""
This action will check if DHCP is enabled on a client

When run as a script, the following options/env variables apply:
    --client_ip        The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --interface_name    The name of the interface to check for DHCP

    --interface_mac     The MAC address of the interface to check for DHCP

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

from optparse import OptionParser
import sys
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetClientDhcpEnabledAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ip" : libsf.IsValidIpv4Address},
            args)
        if "interface_name" not in args and "interface_mac" not in args:
            raise libsf.SfArgumentError("Please specify interface_name or interface_mac")
        if not args["interface_name"] and not args["interface_mac"]:
            raise libsf.SfArgumentError("Please specify interface_name or interface_mac")

    def Get(self, client_ip, interface_name=None, interface_mac=None, csv=False, bash=False, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Check if DHCP is enabled on a client
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        client = SfClient()
        try:
            mylog.info("Connecting to " + client_ip)
            client.Connect(client_ip, client_user, client_pass)
            mylog.info("Checking for DHCP")
            dhcp = client.GetDhcpEnabled(interface_name, interface_mac)
        except ClientError as e:
            mylog.error(e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return False

        self.SetSharedValue(SharedValues.clientDHCPEnabled, dhcp)
        self.SetSharedValue(client_ip + "-clientDHCPEnabled", hostname)
        return dhcp

    def Execute(self, client_ip, interface_name=None, interface_mac=None, csv=False, bash=False, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Show if DHCP is enabled on a client
        """
        del self
        dhcp = Get(**locals())

        if dhcp:
            if bash or csv:
                sys.stdout.write("true")
                sys.stdout.write("\n")
                sys.stdout.flush()
            else:
                mylog.info("DHCP is enabled")
        else:
            if bash or csv:
                sys.stdout.write("false")
                sys.stdout.write("\n")
                sys.stdout.flush()
            else:
                mylog.info("DHCP is not enabled")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--client_ip", type="string", dest="client_ip", default=None, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--interface_name", type="string", dest="interface_name", default=None, help="the name of the interface to check for DHCP")
    parser.add_option("--interface_mac", type="string", dest="interface_mac", default=None, help="the MAC address of the interface to check for DHCP")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.interface_name, options.interface_mac, options.csv, options.bash, options.client_user, options.client_pass, options.debug):
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

