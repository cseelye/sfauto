#!/usr/bin/python

"""
This action will delete a CHAP account on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --account_name      The name of the account to delete
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class DeleteAccountAction(ActionBase):
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
                            "account_name" : None},
            args)

    def Execute(self, account_name, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Delete an account
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Find the account to delete
        mylog.info("Searching for account " + account_name)
        try:
            accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        except libsf.SfApiError as e:
            mylog.error("Failed to get account list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        account_id = 0
        for account in accounts_list["accounts"]:
            if account["username"].lower() == account_name.lower():
                account_id = account["accountID"]
                break
        if account_id == 0:
            mylog.error("Could not find account " + account_name + " on cluster " + mvip)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("Deleting account ID " + str(account_id))
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveAccount", {"accountID" : account_id})
        except libsf.SfApiError as e:
            mylog.error("Failed to delete account: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully deleted account " + account_name)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the name for the account")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.account_name, options.mvip, options.username, options.password, options.debug):
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

