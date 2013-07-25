#!/usr/bin/python

"""
This action will complete pairing between two clusters

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --key               The pairing key from the other cluster

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

class CompleteClusterPairingAction(ActionBase):
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
                            "key" : None},
            args)

    def Execute(self, key, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Complete pairing between two clusters
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster2 = libsfcluster.SFCluster(mvip, username, password)

        try:
            cluster2.CompleteClusterPairing(key)
        except libsf.SfApiError as e:
            if e.name == "xPairingAlreadyExists":
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
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the second cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the second cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the second cluster [%default]")
    parser.add_option("--key", type="string", dest="key", default=None, help="the pairing key from the other cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + ",".join(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.key, options.mvip, options.username, options.password, options.debug):
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

