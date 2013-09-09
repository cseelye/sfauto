#!/usr/bin/python

"""
This action will set the status of a testcase in Qmetry

When run as a script, the following options/env variables apply:

"""

import sys
import time
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.sfdefaults as sfdefaults
from lib.libqmetry import QmetryClient, QmetryError

class ExecuteQmetryTestAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"suite_path" : None,
                            "platform_name" : None,
                            "tc_name" : None,
                            "qmetry_url" : None,
                            "qmetry_user" : None,
                            "qmetry_user" : None
                            },
            args)

    def Execute(self, suite_path, platform_name, tc_name, tc_status, tc_comment=None, qmetry_project=sfdefaults.qmetry_project, qmetry_release=sfdefaults.qmetry_release, qmetry_build=sfdefaults.qmetry_build, qmetry_url=sfdefaults.qmetry_soap_url, qmetry_user=sfdefaults.qmetry_username, qmetry_pass=sfdefaults.qmetry_password, debug=False):
        """
        Set the status of a test
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Connecting to Qmetry...")
        with QmetryClient(qmetry_url, qmetry_user, qmetry_pass) as qmetry:
            qmetry.SetScope(qmetry_project, qmetry_release, qmetry_build)

            mylog.info("Setting test case '" + tc_name + "' in suite '" + suite_path + "' and platform '" + platform_name + "' to status '" + tc_status +"'")
            try:
                qmetry.SetTestCaseExecutionStatus(suite_path, platform_name, tc_name, tc_status, tc_comment)
            except QmetryError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--suite_path", type="string", dest="suite_path", default=None, help="the path to the test suite in Qmetry")
    parser.add_option("--platform_name", type="string", dest="platform_name", default=None, help="the name of the platform in Qmetry")
    parser.add_option("--tc_name", type="string", dest="tc_name", default=None, help="the name of the test case in Qmetry")
    parser.add_option("--tc_status", type="string", dest="tc_status", default=None, help="the status to set on the test case in Qmetry (Passed, Failed, Not Run, Blocked, Not Applicable)")
    parser.add_option("--tc_comment", type="string", dest="tc_comment", default=None, help="the comment to set on the test case in Qmetry")
    parser.add_option("--qmetry_project", type="string", dest="qmetry_project", default=sfdefaults.qmetry_project, help="the project scope in Qmetry [%default]")
    parser.add_option("--qmetry_release", type="string", dest="qmetry_release", default=sfdefaults.qmetry_release, help="the release scope in Qmetry [%default]")
    parser.add_option("--qmetry_build", type="string", dest="qmetry_build", default=sfdefaults.qmetry_build, help="the build scope in Qmetry [%default]")
    parser.add_option("--qmetry_url", type="string", dest="qmetry_url", default=sfdefaults.qmetry_soap_url, help="the SOAP URL for Qmetry [%default]")
    parser.add_option("--qmetry_user", type="string", dest="qmetry_user", default=sfdefaults.qmetry_username, help="the username for Qmetry [%default]")
    parser.add_option("--qmetry_pass", type="string", dest="qmetry_pass", default=sfdefaults.qmetry_password, help="the password for Qmetry [default hidden]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.suite_path, options.platform_name, options.tc_name, options.tc_status, options.tc_comment, options.qmetry_project, options.qmetry_release, options.qmetry_build, options.qmetry_url, options.qmetry_user, options.qmetry_pass, options.debug):
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

