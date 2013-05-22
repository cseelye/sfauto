#!/usr/bin/python

"""
This action will display the cluster capacity stats

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --stat              Which capacity stat to get (default is all)

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetClusterCapacityAction(ActionBase):
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
                            "password" : None},
            args)

    def Get(self, mvip=sfdefaults.mvip, stat=None, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Get cluster capacity stats
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        try:
            result = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
        except libsf.SfError as e:
            mylog.error("Failed to get capacity stats: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        if stat:
            if stat not in result["clusterCapacity"]:
                mylog.error(stat + " is not in clusterCapacity")
                return False
            self.SetSharedValue(stat, result["clusterCapacity"][stat])
        else:
            for key, value in result["clusterCapacity"].iteritems():
                self.SetSharedValue(str(key), str(value))

        return result

    def Execute(self, mvip=sfdefaults.mvip, stat=None, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show cluster capacity stats
        """
        del self
        result = Get(**locals())
        if result is False:
            return False

        if stat:
            if csv or bash:
                sys.stdout.write(str(result["clusterCapacity"][stat]) + "\n")
                sys.stdout.flush()
            else:
                mylog.info(stat + " = " + str(result["clusterCapacity"][stat]))
        else:
            if csv or bash:
                separator = ","
                if bash:
                    separator = " "
                stats = []
                for key, value in result["clusterCapacity"].iteritems():
                    stats.append(str(key) + "=" + str(value))
                sys.stdout.write(separator.join(stats))
                sys.stdout.flush()
            else:
                for key, value in result["clusterCapacity"].iteritems():
                    mylog.info(str(key) + " = " + str(value))

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
    parser.add_option("--stat", type="string", dest="stat", default=None, help="the capacity stat to get")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.stat, options.csv, options.bash, options.username, options.password, options.debug):
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

