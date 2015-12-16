#!/usr/bin/env python2.7

"""
This action will count the number of drives in the given state and compare to the expected number

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --expected          The expected number of drives

    --compare           Comparison method to use (lt, le, gt, ge, eq)
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CountDrivesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "state" : lambda x: x in sfdefaults.all_drive_states,
                            "compare" : lambda x: x in sfdefaults.all_compare_ops.keys(),
                            "expected" : libsf.IsInteger},
            args)

    def Get(self, state="any", mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Count drives
        """
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Searching for drives...")
        drive_count = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("Failed to get drive list: {}".format(e))
            return -1
        for drive in result["drives"]:
            if state == "any" or  drive["status"] == state:
                drive_count += 1

        mylog.info("Found {} drives".format(drive_count))
        return drive_count

    def Execute(self, expected, state="any", compare="eq", mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Count drives
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        if compare == "eq":
            op = "=="
        elif compare == "ne":
            op = "!="
        elif compare == "lt":
            op = "<"
        elif compare == "le":
            op = "<="
        elif compare == "gt":
            op = ">"
        elif compare == "ge":
            op = ">="

        mylog.info("Searching for drives...")
        drive_count = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("Failed to get drive list: " + str(e))
            return False
        if state == "any":
            drive_count = len(result["drives"])
        else:
            for drive in result["drives"]:
                if drive["status"] == state:
                    drive_count += 1

        expression = "{} {} {}".format(drive_count, op, expected)
        mylog.debug("Testing {}".format(expression))
        result = eval(expression)

        if result:
            mylog.passed("Found {} drives".format(drive_count))
            return True
        else:
            mylog.error("Found {} drives but expected {} {}".format(drive_count, op, expected))
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line options
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--state", type="choice", dest="state", choices=sfdefaults.all_drive_states, default="any", help="the state of the drives [%default]")
    parser.add_option("--expected", type="string", dest="expected", default=0, help="the expected number of drives")
    parser.add_option("--compare", type="choice", dest="compare", choices=sfdefaults.all_compare_ops.keys(), default="eq", help="the comparison operator to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(expected=options.expected, state=options.state, compare=options.compare, mvip=options.mvip, username=options.username, password=options.password, debug=options.debug):
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

