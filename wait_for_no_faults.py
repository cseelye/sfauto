#!/usr/bin/python

"""
This action will wait until there are no active cluster faults

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --fault_whitelist   Ignore these faults if they are present
    SFFAULT_WHITELIST env var

    --fault_blacklist   Immediately fail if any of these faults are present
"""

import sys
from optparse import OptionParser
import time
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class WaitForNoFaultsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_WAIT = "BEFORE_WAIT"
        AFTER_WAIT = "AFTER_WAIT"
        FAULT_LIST_CHANGED = "FAULT_LIST_CHANGED"
        BLACKLISTED_FAULT_FOUND = "BLACKLISTED_FAULT_FOUND"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, faultWhitelist=None, faultBlacklist=None, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait until there are no active cluster faults
        """
        if faultWhitelist == None:
            faultWhitelist = sfdefaults.fault_whitelist
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Waiting for no unresolved cluster faults on cluster " + mvip)
        self._RaiseEvent(self.Events.BEFORE_WAIT)

        if faultWhitelist == None:
            faultWhitelist = set()
        else:
            faultWhitelist = set(faultWhitelist)
        if len(faultWhitelist) > 0:
            mylog.info("If these faults are present, they will be ignored: " + ", ".join(faultWhitelist))
        if faultBlacklist == None:
            faultBlacklist = set()
        else:
            faultBlacklist = set(faultBlacklist)
        if len(faultBlacklist) > 0:
            mylog.info("If these faults are present, they will cause an immediately fail: " + ", ".join(faultBlacklist))

        cluster = libsfcluster.SFCluster(mvip, username, password)
        previous_faults = set()
        while True:
            # Get a list of current faults
            try:
                current_faults = cluster.GetCurrentFaultSet(forceUpdate = True)
            except libsf.SfError as e:
                mylog.error("Failed to get list of faults: " + str(e))
                self.RaiseFailureEvent(message="Failed to get list of faults: " + str(e))
                return False

            # Break if there are no faults
            if len(current_faults) <= 0:
                break

            # Break if the only current faults are ignored faults
            if current_faults & faultWhitelist == current_faults:
                break

            # Print the list of faults if it is the first time or if it has changed
            if previous_faults == set() or current_faults & previous_faults != previous_faults:
                mylog.warning("Current faults: " + ",".join(current_faults))
                self._RaiseEvent(self.Events.FAULT_LIST_CHANGED)

            # Abort if there are any blacklisted faults
            if len(current_faults & faultBlacklist) > 0:
                mylog.error("Blacklisted fault found")
                self._RaiseEvent(self.Events.BLACKLISTED_FAULT_FOUND)
                return False

            previous_faults = current_faults
            time.sleep(60)

        mylog.passed("There are no current cluster faults on " + mvip)
        self._RaiseEvent(self.Events.AFTER_WAIT)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--fault_whitelist", action="list", dest="fault_whitelist", default=None, help="ignore these faults and do not wait for them to clear")
    parser.add_option("--fault_blacklist", action="list", dest="fault_blacklist", default=None, help="immediately fail if any of these faults are present")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.fault_whitelist, options.fault_blacklist, options.username, options.password, options.debug):
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

