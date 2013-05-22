#!/usr/bin/python

"""
This action will add volumes to a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_name       The name(s) of the volume(s) to add

    --volume_id         The volumeID(s) of the volume(s) to add

    --volume_prefix     Prefix for the volumes to add

    --volume_regex      Regex to search for volumes to add

    --volume_count      Add at most this many volumes (0 to add all matches)

    --source_account    Account name to use to search for volumes to add

    --source_account_id Account ID to use to search for volumes to add

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class AddVolumesToVolgroupAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ADD = "BEFORE_ADD"
        AFTER_ADD = "AFTER_ADD"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, mvip=sfdefaults.mvip, volgroup_name=None, volgroup_id=0, volume_name=None, volume_id=0, volume_prefix=None, volume_regex=None, volume_count=0, source_account=None, source_account_id=0, test=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Add volumes to a volume access group
        """
        if volume_name == None and volume_id == None or volgroup_id <= 0:
            volume_name = self.GetSharedValue(SharedValues.volumeList)
            volgroup_id = self.GetSharedValue(SharedValues.volumeIDList)
        ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Find the group
        try:
            volgroup = cluster.FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
        except SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Get a list of volumes to add
        mylog.info("Searching for volumes")
        try:
            volumes_to_add = cluster.SearchForVolumes(volumeID=volume_id, volumeName=volume_name, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
        except SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        names = []
        for vol in volumes_to_add.values():
            names.append(vol["name"])
        mylog.info(str(len(names)) + " volumes wil be added: " + ",".join(sorted(names)))

        if test:
            mylog.info("Test option set; volumes will not be added")
            return True

        mylog.info("Adding volumes to group")
        self._RaiseEvent(self.Events.BEFORE_ADD)
        try:
            volgroup.AddVolumes(volumes_to_add.keys())
        except SfError as e:
            mylog.error("Failed to add volumes to group: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully added volumes to group")
        self._RaiseEvent(self.Events.AFTER_ADD)
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
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the VAG to add volumes to")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the VAG to add volumes to")
    parser.add_option("--volume_name", type="string", dest="volume_name", default=None, help="the volume to add")
    parser.add_option("--volume_id", type="string", dest="volume_id", default=0, help="the volume to add")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to add")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to add")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to add")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=0, help="the ID of the account to select volumes from")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be added but don't actually add them")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volgroup_name, options.volgroup_id, options.volume_name, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.source_account_id, options.test, options.username, options.password, options.debug):
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

