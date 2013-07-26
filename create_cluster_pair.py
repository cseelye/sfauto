#!/usr/bin/python

"""
This action will pair two clusters

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the first cluster
    SFMVIP env var

    --mvip2             The managementVIP of the second cluster

    --user              The cluster admin username for the first cluster
    SFUSER env var

    --pass              The cluster admin password for the first cluster
    SFPASS env var

    --user2             The cluster admin username for the second cluster

    --pass2             The cluster admin password for the second cluster

    --strict            Fail if the pair already exists
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateClusterPairAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "mvip2" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "username2" : None,
                            "password2" : None,
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, mvip2=None, strict=False, username=sfdefaults.username, password=sfdefaults.password, username2=sfdefaults.username, password2=sfdefaults.password, debug=False):
        """
        Pair two clusters
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster1 = libsfcluster.SFCluster(mvip, username, password)
        cluster2 = libsfcluster.SFCluster(mvip2, username2, password2)

        try:
            pair = cluster1.StartClusterPairing()
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        try:
            cluster2.CompleteClusterPairing(pair.key)
        except libsf.SfApiError as e:
            if not strict and e.name == "xPairingAlreadyExists":
                mylog.debug("Removing unneeded start pairing")
                cluster1.RemoveClusterPairing(pair.ID)
                mylog.passed("Clusters are already paired")
                return True
            else:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
        mylog.passed("Successfully paired clusters")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line options
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the first cluster")
    parser.add_option("--mvip2", type="string", dest="mvip2", default=None, help="the management VIP for the second cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the first cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the first cluster [%default]")
    parser.add_option("--user2", type="string", dest="username2", default=sfdefaults.username, help="the username for the second cluster [%default]")
    parser.add_option("--pass2", type="string", dest="password2", default=sfdefaults.password, help="the password for the second cluster [%default]")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the pair already exists")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + ",".join(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.mvip2, options.strict, options.username, options.password, options.username2, options.password2, options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

