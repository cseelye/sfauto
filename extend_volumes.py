#!/usr/bin/python

"""
This action will extend a list of volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_name       The name of the volume to extend

    --volume_id         The ID of the volume to extend

    --volume_prefix     Prefix for the volumes to extend

    --volume_regex      Regex to search for volumes to extend

    --volume_count      The max number of volumes to extend

    --source_account    Name of the account to extend volumes from

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
import lib.libsfcluster as libsfcluster
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ExtendVolumesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_UPDATE_VOLUMES = "BEFORE_UPDATE_VOLUMES"
        AFTER_UPDATE_VOLUMES = "AFTER_UPDATE_VOLUMES"
        VOLUME_UPDATE_FAILED = "VOLUME_UPDATE_FAILED"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "new_size" : None,
                            },
                    args)
        if args["new_size"] <= 0:
            raise libsf.SfArgumentError("new_size must be a positive, non-zero integer")

    def _ApiCallThread(self, mvip, username, password, volumeName, volumeID, newSizeGB, results):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        myname = multiprocessing.current_process().name
        results[myname] = False

        mylog.info("Changing size on " + volumeName)
        params = {}
        params["volumeID"] = volumeID
        params["totalSize"] = newSizeGB * 1000 * 1000 * 1000
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolume", params)
        except libsf.SfApiError as e:
            mylog.error("Failed to modify " + volumeName + ": " + str(e))
            self._RaiseEvent(self.Events.VOLUME_UPDATE_FAILED, volumeName=volumeName, exception=e)
            return

        results[myname] = True
        return

    def Execute(self, new_size, mvip=sfdefaults.mvip, volume_name=None, volume_id=0, volume_prefix=None, volume_regex=None, volume_count=0, source_account=None, source_account_id=0, test=False, username=sfdefaults.username, password=sfdefaults.password, parallel_thresh=sfdefaults.parallel_calls_thresh, parallel_max=sfdefaults.parallel_calls_max, debug=False):
        """
        Extend volumes
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Get a list of volumes to modify
        mylog.info("Searching for volumes")
        try:
            volumes = cluster.SearchForVolumes(volumeID=volume_id, volumeName=volume_name, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        count = len(volumes.keys())
        names = ", ".join(map(str, sorted(volumes.keys())))
        mylog.info("Extending " + str(count) + " volumes: " + names)

        if test:
            mylog.info("Test option set; volumes will not be extended")
            return True

        # Run the API operations in parallel if there are enough
        if len(volumes.keys()) <= parallel_thresh:
            parallel_calls = 1
        else:
            parallel_calls = parallel_max

        # Start the client threads
        manager = multiprocessing.Manager()
        results = manager.dict()
        _threads = []
        for volume_id, volume in volumes.items():
            thread_name = "volume-" + str(volume_id)
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ApiCallThread, name=thread_name, args=(mvip, username, password, volume["name"], volume_id, new_size, results))
            th.daemon = True
            _threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_UPDATE_VOLUMES)
        allgood = libsf.ThreadRunner(_threads, results, parallel_calls)
        self._RaiseEvent(self.Events.AFTER_UPDATE_VOLUMES)

        if allgood:
            mylog.passed("Successfully extended all volumes")
            return True
        else:
            mylog.error("Could not extend all volumes")
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
    parser.add_option("--new_size", type="int", dest="new_size", default=0, help="the new size of the volume, in GB")
    parser.add_option("--volume_name", type="string", dest="volume_name", default=None, help="the volume to extend")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to extend")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to extend")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to extend")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to extend")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=0, help="the ID of the account to select volumes from")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be extended but don't actually extend them")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_calls_thresh, help="do not thread calls unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.new_size, options.mvip, options.volume_name, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.source_account_id, options.test, options.username, options.password, options.parallel_thresh, options.parallel_max, options.debug):
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

