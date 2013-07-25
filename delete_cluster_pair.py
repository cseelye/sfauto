#!/usr/bin/python

"""
This action will delete a cluster pairing

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the first cluster
    SFMVIP env var

    --mvip2             The managementVIP of the second cluster

    --cluster_pair_id   The ID of the cluster pair on the first cluster

    --cluster_pair_uuid The UUID of the cluster pair

    --cluster_name      The name of the second cluster

    --user              The cluster admin username for the first cluster
    SFUSER env var

    --pass              The cluster admin password for the first cluster
    SFPASS env var

    --user2             The cluster admin username for the second cluster

    --pass2             The cluster admin password for the second cluster

    --strict            Fail if the pair does not exist

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

class DeleteClusterPairAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "clusterPairID": libsf.IsPositiveInteger,
                            "username" : None,
                            "password" : None},
            args)
        if args["mvip2"]:
            libsf.ValidateArgs({"mvip2" : libsf.IsValidIpv4Address}, args)


    def Execute(self, clusterPairID=0, clusterPairUUID=None, remoteClusterName=None, mvip=sfdefaults.mvip, mvip2=None, strict=False, username=sfdefaults.username, password=sfdefaults.password, username2=sfdefaults.username, password2=sfdefaults.password, debug=False):
        """
        Pair two clusters
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Find the pair on the first cluster
        mylog.info("Looking for pair on " + mvip)
        cluster1 = libsfcluster.SFCluster(mvip, username, password)
        pair = None
        try:
            pair = cluster1.FindClusterPair(clusterPairID=clusterPairID, clusterPairUUID=clusterPairUUID, remoteClusterMVIP=mvip2, remoteClusterName=remoteClusterName)
        except libsf.SfUnknownObjectError as e:
            if strict:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            else:
                mylog.passed("Pairing does not exist on " + mvip)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Remove the pair on the first cluster
        if pair:
            try:
                cluster1.RemoveClusterPairing(pair.ID)
                mylog.passed("Removed cluster pair on " + mvip)
            except libsf.SfApiError as e:
                if e.name == "xPairingDoesNotExist":
                    if strict:
                        mylog.passed("Pairing does not exist on " + mvip)
                    else:
                        mylog.error("Pairing does not exist")
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False
                else:
                    mylog.error(str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

        if mvip2:
            # Find the pair on the second cluster
            mylog.info("Looking for pair on " + mvip2)
            cluster2 = libsfcluster.SFCluster(mvip2, username2, password2)
            pair2 = None
            try:
                if pair:
                    # If we found the pair on the first cluster, look for the corresponding pair on the second cluster via UUID
                    pair2 = cluster2.FindClusterPair(clusterPairUUID=pair.UUID)
                else:
                    # Otherwise search for it using whatever the user provided
                    pair2 = cluster2.FindClusterPair(clusterPairID=clusterPairID, clusterPairUUID=clusterPairUUID, remoteClusterMVIP=mvip, remoteClusterName=remoteClusterName)
            except libsf.SfUnknownObjectError as e:
                if strict:
                    mylog.error(str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                else:
                    mylog.passed("Pairing does not exist on " + mvip)

            if pair2:
                # Remove the pair on the second cluster
                try:
                    cluster2.RemoveClusterPairing(pair2.ID)
                    mylog.passed("Removed cluster pair on " + mvip2)
                except libsf.SfApiError as e:
                    if e.name == "xPairingDoesNotExist":
                        if strict:
                            mylog.passed("Pairing does not exist on " + mvip)
                        else:
                            mylog.error("Pairing does not exist")
                            self.RaiseFailureEvent(message=str(e), exception=e)
                            return False
                    else:
                        mylog.error(str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False

        mylog.passed("Successfully deleted cluster pair")
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
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=None, help="the name of the cluster to delete the pairing for")
    parser.add_option("--cluster_pair_uuid", type="string", dest="cluster_pair_uuid", default=None, help="the UUID of the cluster pair to delete")
    parser.add_option("--cluster_pair_id", type="int", dest="cluster_pair_id", default=0, help="the ID of the cluster pair to delete")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the pair does not exist")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()
    if extra_args and len(extra_args) > 0:
        mylog.error("Unknown arguments: " + ",".join(extra_args))
        sys.exit(1)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.cluster_pair_id, options.cluster_pair_uuid, options.cluster_name, options.mvip, options.mvip2, options.strict, options.username, options.password, options.username2, options.password2, options.debug):
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

