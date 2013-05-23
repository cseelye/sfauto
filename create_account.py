#!/usr/bin/python

"""
This action will create a CHAP account on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --account_name      The name of the account to create

    --initiator_secret  The initiator secret to use. Leave blank to auto-create

    --target_secret     The target secret to use. Leave blank to auto-create

    --strict            Fail if the account already exists
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateAccountAction(ActionBase):
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

    def Execute(self, account_name, mvip=sfdefaults.mvip, initiator_secret=None, target_secret=None, strict=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create an account
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Creating account '" + str(account_name) + "'")
        params = {}
        params["username"] = account_name
        if initiator_secret != None and len(initiator_secret) > 0:
            params["initiatorSecret"] = initiator_secret
        else:
            params["initiatorSecret"] = libsf.MakeSimpleChapSecret()
        if target_secret != None and len(target_secret) > 0:
            params["targetSecret"] = target_secret
        else:
            params["targetSecret"] = libsf.MakeSimpleChapSecret()
        try:
            libsf.CallApiMethod(mvip, username, password, "AddAccount", params)
        except libsf.SfApiError as e:
            if (e.name == "xDuplicateUsername" and not strict):
                mylog.passed("Account already exists")
                self.SetSharedValue(SharedValues.accountName, account_name)
                return True
            else:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        self.SetSharedValue(SharedValues.accountName, account_name)
        mylog.passed("Account created successfully")
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
    parser.add_option("--initiator_secret", type="string", dest="initiator_secret", default=None, help="the initiator secret for the account")
    parser.add_option("--target_secret", type="string", dest="target_secret", default=None, help="the target secret for the account")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the account already exists")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.account_name, options.mvip, options.initiator_secret, options.target_secret, options.strict, options.username, options.password, options.debug):
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

