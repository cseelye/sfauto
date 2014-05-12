#!/usr/bin/python

"""
This action will move a list of volumes to an account

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_name       The name of the volume to move

    --volume_id         The ID of the volume to move

    --volume_count      The max number of volumes to move

    --dest_account      Name of the account to move volumes to

    --paralell_thresh   Do not thread calls unless there are more than this many
    SFPARALLEL_THRESH env var

    --parallel_max       Max number of threads to use
    SFPARALLEL_MAX env var
"""

import sys
from optparse import OptionParser
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class PauseVolumeRrAction(ActionBase):
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

    def _ApiCallThread(self, mvip, username, password, volume_id, pause, results):
        myname = multiprocessing.current_process().name
        results[myname] = False

        params = {}
        params["volumeID"] = volume_id
        params["pausedManual"] = False
        if pause:
            params["pausedManual"] = True
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolumePair", params, ApiVersion=6.0)
        except libsf.SfApiError as e:
            mylog.error("Failed to move " + volume_id + ": " + str(e))
            self._RaiseEvent(self.Events.VOLUME_UPDATE_FAILED, exception=e)
            return

        results[myname] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
                    args)

    def Execute(self, mvip, volume_id=0, volume_count=1, pause=False, test=False, username=sfdefaults.username, password=sfdefaults.password, parallel_thresh=sfdefaults.parallel_calls_thresh, parallel_max=sfdefaults.parallel_calls_max, debug=False):
        """
        pause remote rep on volumes
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if test:
            mylog.info("Test option set; volumes will not be moved")
            return True

        # Run the API operations in parallel if there are enough
        if volume_count <= parallel_thresh:
            parallel_calls = 1
        else:
            parallel_calls = parallel_max

        # move the requested volumes
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for vol_id in range(volume_id, volume_id+volume_count):
            thread_name = "volume-" + str(vol_id)
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ApiCallThread, name=thread_name, args=(mvip, username, password, vol_id, pause, results))
            th.daemon = True
            self._threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_UPDATE_VOLUMES)
        allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)
        if allgood:
            mylog.passed("Successfully moved all volumes")
            self._RaiseEvent(self.Events.AFTER_UPDATE_VOLUMES)
            return True
        else:
            mylog.error("Could not move all volumes")
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
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to lock")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to lock")
    parser.add_option("--pause", action="store_true", dest="pause", default=False, help="pause remote rep")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be moved but don't actually move them")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_calls_thresh, help="do not thread calls unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volume_id, options.volume_count, options.pause, options.test, options.username, options.password, options.parallel_thresh, options.parallel_max, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
