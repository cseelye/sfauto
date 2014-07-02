#!/usr/bin/python

"""
This action will add an IQN to a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --iqn               The IQN to add

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.libsfcluster import SFCluster

class AddFcnodeToVolgroupsAction(ActionBase):
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
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Add the FC node(s) to a volume access group
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Looking for FC volume access groups and nodes")

        # Get the cluster version so we know which endpoint to use
        cluster = SFCluster(mvip, username, password)
        try:
            api_version = cluster.GetAPIVersion()
        except libsf.SfError as e:
            mylog.error("Failed to get cluster version: " + str(e))
            mylog.info("Assuming API version 7.0")
            api_version = 7.0

        # Find all the volume groups with FC initiators in them
        fc_groups = []
        try:
            all_groups = libsf.CallApiMethod(mvip, username, password, "ListVolumeAccessGroups", {}, ApiVersion=api_version)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        for vag in all_groups["volumeAccessGroups"]:
            fc = False
            if "fibreChannelInitiators" in vag and len(vag["fibreChannelInitiators"]) > 0:
                fc = True
            elif "initiators" in vag:
                fc = False
                for init in vag['initiators']:
                    if init.startswith("iqn"):
                        continue
                    else:
                        fc = True
                        break
            if fc:
                fc_groups.append(vag)

        if len(fc_groups) <= 0:
            mylog.warning("There are no volume access groups with FC initiators in them")
            return True

        # Separate initiators into FC and iSCSI
        for group in fc_groups:
            if "iscsiInitiators" not in group:
                group["iscsiInitiators"] = []
            if "fibreChannelInitiators" not in group:
                group["fibreChannelInitiators"] = []
            if "initiators" in group:
                for init in group["initiators"]:
                    if init.startswith("iqn") and init not in group["iscsiInitiators"]:
                        group["iscsiInitiators"].append(init)
                    elif init not in group["fibreChannelInitiators"]:
                        group["fibreChannelInitiators"].append(init)

        # Find the FC nodes in the cluster
        fc_node_ids = []
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListAllNodes", {})
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for node in result["nodes"]:
            if node["platformInfo"]["nodeType"] == "SFFC":
                fc_node_ids.append(node["nodeID"])
        if len(fc_node_ids) <= 0:
            mylog.error("This cluster has no FC nodes in it")
            self.RaiseFailureEvent(message="This cluster has no FC nodes in it")
            return False

        # Get the cluster ID
        try:
            result = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        cluster_id = result["clusterInfo"]["uniqueID"]

        # First remove all FC node IQNs from all groups
        mylog.info("Removing FC node IQNs from all groups")
        for vag in fc_groups:
            iscsi_initiators = []
            for init in vag["iscsiInitiators"]:
                if cluster_id + ".fc" in init:
                    continue
                iscsi_initiators.append(init)
            all_initiators = iscsi_initiators + vag["fibreChannelInitiators"]
            params = {}
            params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
            params["iscsiInitiators"] = iscsi_initiators
            params["initiators"] = all_initiators
            try:
                libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=api_version)
            except libsf.SfApiError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        # For each group, create the new list of IQNs and modify the group
        mylog.info("Adding FC node IQNs to all FC volume access groups")
        for vag in fc_groups:
            mylog.info("  Adding IQNs to " + vag["name"])
            # get the current list of iscsi initators minus all FC iqns
            iscsi_initiators = []
            for init in vag["iscsiInitiators"]:
                if cluster_id + ".fc" in init:
                    continue
                iscsi_initiators.append(init)
            all_initiators = iscsi_initiators + vag["fibreChannelInitiators"]
            for node_id in fc_node_ids:
                for fc_initiator in vag["fibreChannelInitiators"]:
                    iqn = "iqn.2010-01.com.solidfire:{0}.fc{1}.{2}".format(cluster_id, node_id, fc_initiator.replace(':', '').lower())
                    iscsi_initiators.append(iqn)
                all_initiators = iscsi_initiators + vag["fibreChannelInitiators"]

                params = {}
                params["volumeAccessGroupID"] = vag["volumeAccessGroupID"]
                params["iscsiInitiators"] = iscsi_initiators
                params["initiators"] = all_initiators
                try:
                    libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=api_version)
                except libsf.SfApiError as e:
                    mylog.error(str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

        mylog.passed("Successfully added FC nodes to groups")
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
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.debug):
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
