#!/usr/bin/python

"""
This action will set the hostname of a client

When run as a script, the following options/env variables apply:
    --client_ip        The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --hostname          The new hostname
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

class SetClientHostnameAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_CLIENT_HOSTNAME = "BEFORE_SET_CLIENT_HOSTNAME"
        AFTER_SET_CLIENT_HOSTNAME = "AFTER_SET_CLIENT_HOSTNAME"
        SET_CLIENT_HOSTNAME_FAILED = "SET_CLIENT_HOSTNAME_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIP" : libsf.IsValidIpv4Address,
                            "hostname" : None},
            args)

    def Execute(self, clientIP, hostname, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, debug=False):
        """
        Change the hostname of a client
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        client = SfClient()
        mylog.info("Connecting to client '" + clientIP + "'")
        try:
            client.Connect(clientIP, clientUser, clientPass)
        except ClientError as e:
            mylog.error(e)
            return False

        mylog.info("Setting hostname from " + client.Hostname + " to " + hostname)
        self._RaiseEvent(self.Events.BEFORE_SET_CLIENT_HOSTNAME, clientIP=clientIP)
        try:
            client.UpdateHostname(hostname)
        except ClientError as e:
            mylog.error(e.message)
            self._RaiseEvent(self.Events.SET_CLIENT_HOSTNAME_FAILED, clientIP=clientIP, exception=e)
            return False

        mylog.passed("Successfully set hostname")
        self._RaiseEvent(self.Events.AFTER_SET_CLIENT_HOSTNAME, clientIP=clientIP)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(description="Set the hostname of a client")
    parser.add_option("--client_ip", type="string", dest="client_ip", default=None, help="the IP addresses of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--hostname", type="string", dest="hostname", default=None, help="the new hostname for the client")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.hostname, options.client_user, options.client_pass, options.debug):
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

