#!/usr/bin/python

"""
This action will set an attribute on a list of volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_id         The ID of the volume to select

    --volume_count      The max number of volumes to select

    --attr              The attribute to set on the volumes

    --attrVAlue         The value of the attribute to set on the volumes

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

class SetVolumeAttrAction(ActionBase):
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

    def _ApiCallThread(self, mvip, username, password, volumeID, attr, attrValue, debug, results):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        myname = multiprocessing.current_process().name
        results[myname] = False

        if debug:
            mylog.info("Updating " + str(volumeID) + " " + attr +"=" + attrValue)
        params = {}
        params["volumeID"] = volumeID
        params[attr] = attrValue
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolume", params)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self._RaiseEvent(self.Events.VOLUME_UPDATE_FAILED, volumeID=volumeID, exception=e)
            return

        results[myname] = True
        return

    def Execute(self, mvip=sfdefaults.mvip, volumeID=0, volumeCount=0, attr="", attrValue="", test=False, username=sfdefaults.username, password=sfdefaults.password, parallelThresh=sfdefaults.parallel_calls_thresh, parallelMax=sfdefaults.parallel_calls_max, debug=False):
        """
        Set attribute on volumes
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        mylog.info("Setting attribute " + attr + "=" + attrValue)

        if test:
            mylog.info("Test option set; volumes will not be modified")
            return True

        # Run the API operations in parallel if there are enough
        if volumeCount <= parallelThresh:
            parallel_calls = 1
        else:
            parallel_calls = parallelMax

        # Start the client threads
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for volID in range(volumeID, volumeID+volumeCount):
            thread_name = "volume-" + str(volID)
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ApiCallThread, name=thread_name, args=(mvip, username, password, volID, attr, attrValue, debug, results))
            th.deamon = True
            self._threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_UPDATE_VOLUMES)
        allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)

        if allgood:
            mylog.passed("Successfully updated attributes on all volumes")
            self._RaiseEvent(self.Events.AFTER_UPDATE_VOLUMES)
            return True
        else:
            mylog.error("Could not update attributes on all volumes")
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
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to modify")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to modify")
    parser.add_option("--attr", type="string", dest="attr", help="the attribute to set")
    parser.add_option("--attr_value", type="string", dest="attr_value", help="the value of the attribute to set")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be modified but don't actually modify them")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_calls_thresh, help="do not thread calls unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volume_id, options.volume_count, options.attr, options.attr_value, options.test, options.username, options.password, options.parallel_thresh, options.parallel_max, options.debug):
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
