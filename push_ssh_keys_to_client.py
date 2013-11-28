#!/usr/bin/python

"""
This action will push the local SSH RSA key to a list of clients, to enable password-less SSH

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import re
import platform
import getpass
import os
import lib.libsf as libsf
from lib.libsf import mylog
import lib.libclient as libclient
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class PushSshKeysToClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_PUSH = "BEFORE_PUSH"
        AFTER_PUSH = "AFTER_PUSH"
        ALL_PUSHED = "ALL_PUSHED"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            },
            args)

    def Execute(self, client_ips=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Push SSH keys to clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Look for or create local RSA id
        local_hostname = platform.node()
        home = os.path.expanduser("~")
        key_text = ""
        if "win" in platform.system().lower():
            key_path = home + "\\ssh\\id_rsa.pub"
            if not os.path.exists(key_path):
                mylog.error("Please place your RSA id in " + key_path)
                return False
        else:
            key_path = home + "/.ssh/id_rsa.pub"
            if not os.path.exists(key_path):
                mylog.info("Creating SSH key for " + local_hostname)
                libsf.RunCommand("ssh-keygen -q -f ~/.ssh/id_rsa -N \"\"")
        with open(key_path) as f:
            key_text = f.read()
        key_text = key_text.rstrip()

        # Send the key over to each client
        allgood = True
        for client_ip in client_ips:
            self._RaiseEvent(self.Events.BEFORE_PUSH, clientIP=client_ip)
            client = SfClient()
            mylog.info("Connecting to client '" + client_ip + "'")
            try:
                client.Connect(client_ip, client_user, client_pass)
            except ClientError as e:
                mylog.error("Failed to connect to " + client_ip + ": " + str(e))
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False
                continue

            if client.RemoteOs == libclient.OsType.Windows:
                mylog.passed("Skipping Windows client")
                continue

            try:
                # Make sure the .ssh directory exists
                retcode, stdout, stderr = client.ExecuteCommand("find ~ -maxdepth 1 -name \".ssh\" -type d | wc -l")
                if not int(stdout):
                    client.ExecuteCommand("mkdir ~/.ssh")
                client.ExecuteCommand("chmod 700 ~/.ssh")

                # See if the key is already on the client and add it if it isn't
                found = False
                retcode, stdout, stderr = client.ExecuteCommand("cat ~/.ssh/authorized_keys")
                for line in stdout.split("\n"):
                    #print line
                    if line == key_text:
                        found = True
                        break
                if found:
                    mylog.info("Key is already on client " + client.Hostname)
                else:
                    mylog.info("Adding key to " + client.Hostname)
                    client.ExecuteCommand("echo \"" + key_text + "\" >> ~/.ssh/authorized_keys")
                client.ExecuteCommand("chmod 600 ~/.ssh/authorized_keys")
                mylog.passed("Pushed key to " + client.Hostname)
            except ClientError as e:
                mylog.error("Failed to push key to client " + client_ip)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False
                continue
            self._RaiseEvent(self.Events.AFTER_PUSH, clientIP=client_ip)

        self._RaiseEvent(self.Events.ALL_PUSHED)
        if allgood:
            mylog.passed("Successfully pushed SSH keys to all clients")
            return True
        else:
            mylog.error("Could not push SSH keys to all clients")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
