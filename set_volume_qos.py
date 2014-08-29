#!/usr/bin/python

"""
This action will set QoS on a list of volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --min_iops          The minIOPS to set

    --max_iops          The maxIOPS to set

    --burstIOPS         The burstIOPS to set

    --volumeName       The name of the volume to select

    --volume_id         The ID of the volume to select

    --volume_prefix     Prefix for the volumes to select

    --volume_regex      Regex to search for volumes to select

    --volume_count      The max number of volumes to select

    --source_account    Name of the account to select volumes from

    --paralell_thresh   Do not thread calls unless there are more than this many
    SFPARALLEL_THRESH env var

    --parallel_max       Max number of threads to use
    SFPARALLEL_MAX env var
"""

import sys
from optparse import OptionParser
import multiprocessing
import logging
import signal
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SetVolumeQosAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_UPDATE_VOLUMES = "BEFORE_UPDATE_VOLUMES"
        AFTER_UPDATE_VOLUMES = "AFTER_UPDATE_VOLUMES"
        VOLUME_UPDATE_FAILED = "VOLUME_UPDATE_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            },
            args)

    def _ApiCallThread(self, mvip, username, password, volumeName, volumeID, maxIOPS, minIOPS, burstIOPS, results):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        myname = multiprocessing.current_process().name
        results[myname] = False

        mylog.info("Updating QoS on " + volumeName)
        params = {}
        params["volumeID"] = volumeID
        qos = {}
        if maxIOPS > 0:
            qos["maxIOPS"] = maxIOPS
        if minIOPS > 0:
            qos["minIOPS"] = minIOPS
        if burstIOPS > 0:
            qos["burstIOPS"] = burstIOPS
        params["qos"] = qos
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolume", params)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self._RaiseEvent(self.Events.VOLUME_UPDATE_FAILED, volumeName=volumeName, exception=e)
            return

        results[myname] = True
        return

    def Execute(self, mvip=sfdefaults.mvip, minIOPS=sfdefaults.min_iops, maxIOPS=sfdefaults.max_iops, burstIOPS=sfdefaults.burst_iops, volumeName=None, volumeID=0, volumePrefix=None, volumeRegex=None, volumeCount=0, sourceAccount=None, sourceAccountID=0, test=False, username=sfdefaults.username, password=sfdefaults.password, parallelThresh=sfdefaults.parallel_calls_thresh, parallelMax=sfdefaults.parallel_calls_max, debug=False):
        """
        Set QoS on volumes
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Get a list of volumes to modify
        mylog.info("Searching for volumes")
        try:
            volumes = cluster.SearchForVolumes(volumeID=volumeID, volumeName=volumeName, volumeRegex=volumeRegex, volumePrefix=volumePrefix, accountName=sourceAccount, accountID=sourceAccountID, volumeCount=volumeCount)
        except libsf.SfError as e:
            mylog.error(e.message)
            return False

        names = []
        for vol in volumes.values():
            names.append(vol["name"])
        mylog.info("Setting QoS " + str(len(names)) + " volumes: " + ",".join(sorted(names)))

        if test:
            mylog.info("Test option set; volumes will not be modified")
            return True

        # Run the API operations in parallel if there are enough
        if len(volumes.keys()) <= parallelThresh:
            parallel_calls = 1
        else:
            parallel_calls = parallelMax

        # Start the client threads
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for volumeID, volumeInfo in volumes.items():
            volume_name = volumeInfo["name"]
            thread_name = "volume-" + str(volumeID)
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ApiCallThread, name=thread_name, args=(mvip, username, password, volume_name, volumeID, maxIOPS, minIOPS, burstIOPS, results))
            th.deamon = True
            self._threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_UPDATE_VOLUMES)
        allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)

        if allgood:
            mylog.passed("Successfully updated Qos on all volumes")
            self._RaiseEvent(self.Events.AFTER_UPDATE_VOLUMES)
            return True
        else:
            mylog.error("Could not update QoS on all volumes")
            return False

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
    parser.add_option("--min_iops", type="int", dest="min_iops", default=sfdefaults.min_iops, help="the min sustained IOPS to guarantee on this volume")
    parser.add_option("--max_iops", type="int", dest="max_iops", default=sfdefaults.max_iops, help="the max sustained IOPS to allow on this volume")
    parser.add_option("--burst_iops", type="int", dest="burst_iops", default=sfdefaults.burst_iops, help="the burst IOPS to allow on this volume")
    parser.add_option("--volume_name", type="string", dest="volume_name", default=None, help="the volume to modify")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to modify")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to modify")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to modify")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to modify")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=0, help="the ID of the account to select volumes from")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be modified but don't actually modify them")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_calls_thresh, help="do not thread calls unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.min_iops, options.max_iops, options.burst_iops, options.volume_name, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.source_account_id, options.test, options.username, options.password, options.parallel_thresh, options.parallel_max, options.debug):
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
