#!/usr/bin/python

"""
This action will delete a list of volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_name       The name of the volume to delete

    --volume_id         The ID of the volume to delete

    --volume_prefix     Prefix for the volumes to delete

    --volume_regex      Regex to search for volumes to delete

    --volume_count      The max number of volumes to delete

    --source_account    Name of the account to delete volumes from

    --purge             Purge the volumes afer deleting them

    --test
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class DeleteVolumesAction(ActionBase):
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

    def Execute(self, mvip=sfdefaults.mvip, volume_name=None, volume_id=0, volume_prefix=None, volume_regex=None, volume_count=0, source_account=None, purge=False, test=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Delete volumes
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of volumes to delete
        mylog.info("Searching for volumes")
        try:
            volumes_to_delete = libsf.SearchForVolumes(mvip, username, password, VolumeId=volume_id, VolumeName=volume_name, VolumeRegex=volume_regex, VolumePrefix=volume_prefix, AccountName=source_account, VolumeCount=volume_count)
        except SfError as e:
            mylog.error(e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        count = len(volumes_to_delete.keys())
        names = ", ".join(sorted(volumes_to_delete.keys()))
        mylog.info("Deleting " + str(count) + " volumes: " + names)

        if test:
            mylog.info("Test option set; volumes will not be deleted")
            return True

        # Delete the requested volumes
        for vol_name in sorted(volumes_to_delete.keys()):
            vol_id = volumes_to_delete[vol_name]
            mylog.debug("Deleting " + vol_name)
            params = {}
            params["volumeID"] = vol_id
            try:
                libsf.CallApiMethod(mvip, username, password, "DeleteVolume", params)
            except SfError as e:
                mylog.error(e.message)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            if purge:
                mylog.debug("Purging " + vol_name)
                try:
                    libsf.CallApiMethod(mvip, username, password, "PurgeDeletedVolume", params)
                except SfError as e:
                    mylog.error(e.message)
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
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
    parser.add_option("--volume_name", type="string", dest="volume_name", default=None, help="the volume to delete")
    parser.add_option("--volume_id", type="int", dest="volume_id", default=0, help="the volume to delete")
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix of volumes to delete")
    parser.add_option("--volume_regex", type="string", dest="volume_regex", default=None, help="regex to search for volumes to delete")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=0, help="the number of volumes to delete")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to select volumes from")
    parser.add_option("--purge", action="store_true", dest="purge", default=False, help="purge the volumes after deleting them")
    parser.add_option("--test", action="store_true", dest="test", default=False, help="show the volumes that would be deleted but don't actually delete them")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volume_name, options.volume_id, options.volume_prefix, options.volume_regex, options.volume_count, options.source_account, options.purge, options.test, options.username, options.password, options.debug):
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
