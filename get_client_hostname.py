#!/usr/bin/python

"""
This action will get the hostname of a client

When run as a script, the following options/env variables apply:
    --client_ip        The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

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

class GetClientHostnameAction(ActionBase):
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

    def Get(self, client_ip, csv=False, bash=False, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Get the hostname of a client
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        mylog.info("Connecting to client '" + client_ip + "'")
        try:
            client = SfClient()
            client.Connect(client_ip, client_user, client_pass)
            hostname = client.Hostname
        except ClientError as e:
            mylog.error(e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return False

        self.SetSharedValue(SharedValues.clientHostname, hostname)
        self.SetSharedValue(client_ip + "-clientHostname", hostname)
        return hostname

    def Execute(self, client_ip, csv=False, bash=False, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Show the hostname of a client
        """
        del self
        client_name = Get(**locals())
        if client_name is False:
            return False

        if csv or bash:
            sys.stdout.write(str(client_name))
            sys.stdout.write("\n")
            sys.stdout.flush()
        else:
            mylog.info(client_ip + " has hostname " + str(client_name))
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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.csv, options.bash, options.client_user, options.client_pass, options.debug):
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

