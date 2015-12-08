#!/usr/bin/env python2.7

"""
This action will create a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volgroup_name     The name of the group to create

    --strict            Fail if the group already exists
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase

class CreateVolgroupAction(ActionBase):
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
                            "volgroup_name" : None},
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, volgroup_name, mvip=sfdefaults.mvip, strict=False, iqns=None, volume_names=None, volume_ids=0, volume_prefix=None, volume_regex=None, volume_count=0, source_account=None, source_account_id=0, test=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create a volume access group
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Find the requested volumes
        add_volume_ids = None
        if volume_names or volume_ids or volume_prefix or volume_regex or source_account or source_account_id:
            mylog.info("Searching for volumes")
            try:
                found_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
            except SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            add_volume_ids = found_volumes.keys()
            add_volume_names = [found_volumes[i]["name"] for i in add_volume_ids]

        # See if the group already exists
        mylog.info("Searching for volume groups")
        try:
            cluster.FindVolumeAccessGroup(volgroupName=volgroup_name)
            if strict or iqns or add_volume_ids:
                mylog.error("Group already exists")
                self.RaiseFailureEvent(message="Group already exists")
                return False
            else:
                mylog.passed("Group already exists")
                return True
        except libsf.SfApiError as e:
            mylog.error("Could not search for volume groups: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        except SfError:
            # Group does not exist
            pass

        mylog.info("Creating volume access group '{}'".format(volgroup_name))
        if iqns:
            mylog.info("  IQNs: {}".format(",".join(iqns)))
        if add_volume_ids:
            mylog.info("  Volumes: {}".format(",".join(add_volume_names)))

        if test:
            mylog.info("Test option set; group will not be created")
            return True

        # Create the group
        try:
            cluster.CreateVolumeGroup(volgroup_name, iqns, add_volume_ids)
        except libsf.SfError as e:
            mylog.error("Failed to create group: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Group created successfully")
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
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name for the group")
    parser.add_option("--iqns", action="list", dest="iqns", default=None, help="list of initiator IQNs to add to the group")
    parser.add_option("--volume_names", action="list", dest="volume_names", default=None, help="list volume(s) to add to the group")
    parser.add_option("--volume_id", action="list", dest="volume_id", default=0, help="the volume(s) to add")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to add")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to add")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to add")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select volumes from")
    parser.add_option("--source_account_id", type="int", dest="source_account_id", default=0, help="the ID of the account to select volumes from")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the account already exists")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volume group that would be created but don't actually create it")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.volgroup_name, options.mvip, options.strict, options.iqns, options.volume_names, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.source_account_id, options.test, options.username, options.password, options.debug):
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
