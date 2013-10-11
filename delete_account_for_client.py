#!/usr/bin/python

"""
This action will delete the CHAP account that corresponds to the specified clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
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

class DeleteAccountForClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "client_ips" : libsf.IsValidIpv4AddressList
                            },
                    args)

    def Execute(self, mvip=sfdefaults.mvip, client_ips=None, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Delete CHAP accounts for a list of clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of accounts from the cluster
        mylog.info("Getting a list of accounts")
        try:
            accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        except libsf.SfError as e:
            mylog.error("Failed to get account list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        allgood = True
        for client_ip in client_ips:
            mylog.info(client_ip + ": Connecting to client")
            client = SfClient()
            try:
                client.Connect(client_ip, client_user, client_pass)
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return False

            account_id = 0
            for account in accounts_list["accounts"]:
                if account["username"].lower() == client.Hostname.lower():
                    account_id = account["accountID"]
                    break
            if account_id == 0:
                mylog.passed(client_ip + ": Account does not exist or has already been deleted")
                continue

            mylog.info(client_ip + ": Deleting account ID " + str(account_id))
            try:
                libsf.CallApiMethod(mvip, username, password, "RemoveAccount", {"accountID" : account_id})
                mylog.passed(client_ip + ": Successfully deleted account " + client.Hostname)
            except libsf.SfError as e:
                mylog.error(client_ip + ": Failed to delete account: " + str(e))
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                allgood = False

        if allgood:
            mylog.passed("Successfully deleted all accounts")
            return True
        else:
            mylog.error("Could not delete all accounts")
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
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.client_ips, options.username, options.password, options.client_user, options.client_pass, options.debug):
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
