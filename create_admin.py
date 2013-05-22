#!/usr/bin/python

"""
This action will create an admin user on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --admin_name        The name for the new admin

    --admin_pass        The password for the new admin
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateAdminAction(ActionBase):
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
                            "admin_name" : None},
            args)

    def Execute(self, admin_name, mvip=sfdefaults.mvip, admin_pass=sfdefaults.password, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create a cluster admin
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        params = {}
        params["username"] = admin_name
        params["password"] = admin_pass
        params["access"] = ["administrator"]
        try:
            libsf.CallApiMethod(mvip, username, password, "AddClusterAdmin", params)
        except libsf.SfError as e:
            if e.name == "xDuplicateUsername":
                mylog.passed("Admin already exists")
                return True
            else:
                mylog.error("Failed to create admin: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

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
    parser.add_option("--admin_name", type="string", dest="admin_name", default=None, help="the name for the new admin")
    parser.add_option("--admin_pass", type="string", dest="admin_pass", default=sfdefaults.password, help="the password for the new admin")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.admin_name, options.admin_pass, options.username, options.password, options.debug):
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
