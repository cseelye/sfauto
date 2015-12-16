#!/usr/bin/env python2.7

"""
This action will wait for all drives from all nodes to be present and active or available state

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import time
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.libsfcluster import SFCluster
from lib.libsfnode import SFNode
from lib.action_base import ActionBase

class WaitForHealthyDrivesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Wait for all drives
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Waiting for all drives to be healthy...")
        cluster = SFCluster(mvip, username, password)
        active_nodes = cluster.ListActiveNodes()
        expected_count = 0
        for n in active_nodes:
            node = SFNode(n["mip"], clusterUsername=username, clusterPassword=password)
            expected_count += node.GetExpectedDriveCount()

        # Wait for all drives to be present
        previous_found = -1
        while True:
            drives = cluster.ListDrives()
            found_count = len(drives)
            if found_count >= expected_count:
                break

            if found_count != previous_found:
                mylog.info("Found {} drives".format(found_count))
                previous_found = found_count

            time.sleep(10)

        # Wait for all drives to be available or active
        previous_found = -1
        while True:
            drives = cluster.ListDrives()
            bad_status = 0
            for drive in drives:
                if drive["status"] != "active" and drive["status"] != "available":
                    bad_status += 1
            if bad_status <= 0:
                break
            
            if bad_status != previous_found:
                mylog.info("Found {} drives with unhealthy status")
                previous_found = bad_status

            time.sleep(10)

        mylog.passed("{} drives are present and healthy".format(expected_count))
        return True

# Instantiate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(mvip=options.mvip, username=options.username, password=options.password, debug=options.debug):
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

