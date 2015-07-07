#!/usr/bin/env python2.7

"""
This action will kernel panic a list of clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KernelPanicClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, client_ips=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Kernel panic clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        allgood = True
        for client_ip in client_ips:
            mylog.info(client_ip + ": Connecting")
            client = SfClient()
            try:
                client.Connect(client_ip, client_user, client_pass)
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False
                continue

            mylog.info(client_ip + ": Panicking")
            try:
                client.KernelPanic()
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False
                continue

            mylog.passed(client_ip + ": Generated kernel panic")

        if allgood:
            mylog.passed("Successfully panic all clients")
            return True
        else:
            mylog.error("Could not panic all clients")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP address of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.client_user, options.client_pass, options.debug):
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

