#!/usr/bin/python

"""
This action will remove a node from the cluster.

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ip           The mIP of the node to remove

    --remove_drives     Remove all of the drives in the node before removing the node
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class RemoveNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_REMOVE_DRIVES = "BEFORE_REMOVE_DRIVES"
        AFTER_REMOVE_DRIVES = "AFTER_REMOVE_DRIVES"
        BEFORE_REMOVE_NODE = "BEFORE_REMOVE_NODE"
        AFTER_REMOVE_NODE = "AFTER_REMOVE_NODE"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "node_ip" : libsf.IsValidIpv4Address},
            args)

    def Execute(self, mvip, node_ip, remove_drives=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Remove a node from the cluster
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Find the nodeID of the requested node
        mylog.info("Searching for nodes")
        node_id = 0
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["nodes"]:
            if node["mip"] == node_ip:
                node_id = node["nodeID"]
                break
        if node_id <= 0:
            mylog.error("Could not find node " + node_ip)
            self.RaiseFailureEvent(message="Could not find node " + node_ip)
            return False
        mylog.info("Found node " + node_ip + " is nodeID " + str(node_id))

        # Remove all active and failed drives from the node
        if remove_drives:
            self._RaiseEvent(self.Events.BEFORE_REMOVE_DRIVES)
            mylog.info("Searching for drives in node " + node_ip)
            drives_to_remove = []
            try:
                result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
            except libsf.SfError as e:
                mylog.error("Failed to get drive list: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for drive in result["drives"]:
                if (drive["status"].lower() == "active" or drive["status"].lower() == "active") and node_id == drive["nodeID"]:
                    drives_to_remove.append(drive["driveID"])
            if len(drives_to_remove) > 0:
                mylog.info("Removing " + str(len(drives_to_remove)) + " drives " + str(drives_to_remove))
                time.sleep(2)
                try:
                    libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': drives_to_remove})
                except libsf.SfError as e:
                    mylog.error("Failed to remove drives: " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

                mylog.info("Waiting for syncing")
                time.sleep(60)
                try:
                    # Wait for bin syncing
                    while libsf.ClusterIsBinSyncing(mvip, username, password):
                        time.sleep(30)
                    # Wait for slice syncing
                    while libsf.ClusterIsSliceSyncing(mvip, username, password):
                        time.sleep(30)
                except libsf.SfError as e:
                    mylog.error("Failed to wait for syncing: " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
            self._RaiseEvent(self.Events.AFTER_REMOVE_DRIVES)

        # Remove the node
        self._RaiseEvent(self.Events.BEFORE_REMOVE_NODE)
        while True:
            mylog.info("Removing " + node_ip + " from cluster")
            try:
                libsf.CallApiMethod(mvip, username, password, "RemoveNodes", {"nodes" : [node_id]}, ExitOnError=False)
                break
            except libsf.SfApiError as e:
                if e.name == "xDBConnectionLoss":
                    # Often happens when removing ensemble members
                    mylog.warning("xDBConnectionLoss - making sure node was actually removed")
                    # Just retry and catch xNodeIDDoesNotExist fault if it was already removed
                    time.sleep(5)
                    continue
                elif e.name == "xNodeIDDoesNotExist":
                    # Node was actually removed, but an error happened responding to the API call and the retry logic triggered this
                    break
                else:
                    mylog.error("Error " + e.name + " - " + e.message)
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

        mylog.passed("Successfully removed " + node_ip + " from cluster")
        self._RaiseEvent(self.Events.AFTER_REMOVE_NODE)
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
    parser.add_option("--node_ip", type="string", dest="node_ip", default=None, help="the mIP of the node to remove")
    parser.add_option("--remove_drives", action="store_true", dest="remove_drives", default=False, help="display more verbose messages")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.node_ip, options.remove_drives, options.username, options.password, options.debug):
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

