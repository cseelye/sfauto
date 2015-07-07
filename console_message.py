#!/usr/bin/env python2.7

"""
This action will display/log a message

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --cluster_name      The name of the cluster
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ConsoleMessageAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_CLUSTER_NAME = "BEFORE_SET_CLUSTER_NAME"
        AFTER_SET_CLUSTER_NAME = "AFTER_SET_CLUSTER_NAME"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"message" : None,
                            "level" : lambda x: x in [f.severity for f in mylog.GetLoggerMethods()]},
            args)

    def Execute(self, message, level="info", debug=False):
        """
        Display a message
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # See if the log has a function with the same name as the severity level, and call that function with the message to log
        log = getattr(mylog, level, None)
        if not log:
            return False
        log(message)
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    severities = sorted([f.severity for f in mylog.GetLoggerMethods()])
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--message", type="string", dest="message", default=None, help="the message to display")
    parser.add_option("-l", "--level", type="choice", choices=severities, dest="level", default="info", help="the severity level: one of ({}) [%default]".format(",".join(severities)))
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        if Execute(message=options.message, level=options.level, debug=options.debug):
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

