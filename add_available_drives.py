#!/usr/bin/env python

"""
This action will find all drives in the available pool and add them to the cluster.

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --no-sync           Do not wait for syncing after adding drives
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class AddAvailableDrivesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ADD = "BEFORE_ADD"
        AFTER_ADD = "AFTER_ADD"
        BEFORE_SYNC = "BEFORE_SYNC"
        AFTER_SYNC = "AFTER_SYNC"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, waitForSync=True, debug=False):
        """
        Find all of the drives in the available pool and add them to the cluster, optionally waiting for syncing to complete.
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        self._RaiseEvent(self.Events.BEFORE_ADD)
        try:
            added = cluster.AddAvailableDrives()
        except libsf.SfError as e:
            mylog.error("Failed to get drive list: " + str(e))
            self.RaiseFailureEvent(exception=e)
            return False
        if added <= 0:
            return True

        if waitForSync:
            self._RaiseEvent(self.Events.BEFORE_SYNC)
            mylog.info("Waiting a minute to make sure syncing has started")
            time.sleep(60)

            try:
                mylog.info("Waiting for slice syncing")
                while cluster.IsSliceSyncing():
                    time.sleep(20)

                mylog.info("Waiting for bin syncing")
                while cluster.IsBinSyncing():
                    time.sleep(20)
            except libsf.SfError as e:
                mylog.error("Failed wait for syncing: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            self._RaiseEvent(self.Events.AFTER_SYNC)

        mylog.passed("Successfully added drives to the cluster")
        self._RaiseEvent(self.Events.AFTER_ADD)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line options
    parser = OptionParser(description="Add all of the available drives to the cluster and wait for syncing to complete.")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--no_sync", action="store_false", dest="wait_for_sync", default=True, help="do not wait for syncing after adding the drives")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + str(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.wait_for_sync, options.debug):
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
