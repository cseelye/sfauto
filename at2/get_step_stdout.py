#!/usr/bin/python

"""
This action will show the stdout of a step from AT2

It will loop and wait for the nodes to be available and then check them out

When run as a script, the following options/env variables apply:
    --user              AT2 username

    --pass              AT2 password

    --task_instance_step_id      AT2 taskInstanceStepID of the step
"""

import sys
from optparse import OptionParser
import multiprocessing
import time
sys.path.append("..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import logging
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetStepStdoutAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({
                            "username" : None,
                            "password" : None,
                            "task_instance_step_id" : libsf.IsPositiveInteger
                            },
            args)

    def Execute(self, task_instance_step_id, username=None, password=None, debug=False):
        """
        Show the stdout of a step
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDeug()
        else:
            mylog.hideDebug()

        try:
            result = libsf.CallApiMethod("autotest2.solidfire.net", username, password, "GetTaskInstanceStepStdout", {"taskInstanceStepID" : task_instance_step_id}, ApiVersion=1.0)
        except libsf.SfApiError as e:
            mylog.error("GetTaskInstanceStepStdout failed: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        stdout = result["stdout"].replace("\\n", "\n")
        print stdout

        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-u", "--user", type="string", dest="username", default=None, help="the username of the AT2 account to check out the nodes to")
    parser.add_option("-p", "--pass", type="string", dest="password", default=None, help="the password of the AT account")
    parser.add_option("-i", "--task_instance_step_id", type="int", dest="task_instance_step_id", default=None, help="the AT2 taskInstanceStepID")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        if Execute(task_instance_step_id=options.task_instance_step_id, username=options.username, password=options.password, debug=options.debug):
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
    sys.exit(0)
