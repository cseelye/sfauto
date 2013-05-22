#!/usr/bin/python

"""
This action will set the iSCSI initiator name of a client

The initiator name is basedon the standard for that OS and is made unique by including the hostname

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SetClientInitiatorNameAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_CLIENT_INITIATOR_NAME = "BEFORE_SET_CLIENT_INITIATOR_NAME"
        AFTER_SET_CLIENT_INITIATOR_NAME = "AFTER_SET_CLIENT_INITIATOR_NAME"
        SET_CLIENT_INITIATOR_NAME_FAILED = "SET_CLIENT_INITIATOR_NAME_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIPs" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, clientIPs, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, debug=False):
        """
        Set a client initiator name
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        error = False
        for client_ip in clientIPs:
            client = SfClient()
            mylog.info("Connecting to client '" + client_ip + "'")
            try:
                client.Connect(client_ip, clientUser, clientPass)
            except ClientError as e:
                mylog.error(str(e))
                error = True
                continue

            mylog.info("Setting initiator name on " + client.Hostname)
            self._RaiseEvent(self.Events.BEFORE_SET_CLIENT_INITIATOR_NAME, clientIP=client_ip)
            try:
                client.UpdateInitiatorName()
            except ClientError as e:
                mylog.error(str(e))
                error = True
                self._RaiseEvent(self.Events.SET_CLIENT_INITIATOR_NAME_FAILED, clientIP=client_ip, exception=e)
                continue

            mylog.passed("  Successfully set initiator name on " + client.Hostname)
            self._RaiseEvent(self.Events.AFTER_SET_CLIENT_INITIATOR_NAME, clientIP=client_ip)

        if error:
            mylog.error("Failed to set initiator name on all clients")
            return False
        else:
            mylog.passed("Successfully set initiator name on all clients")
            return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description="Set the iSCSI initiator name on the clients")
    parser.add_option("--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
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
