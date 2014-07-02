#!/usr/bin/python

"""
This action will change the LUN assignments for volumes in a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group

    --method            The method to use for renumbering. Valid options are
                        [seq, rev, rand, vol] for sequential, reverse, random, volumeID

    --min               Smallest LUN number to use

    --max               Largest LUN number to use
"""

import sys
from optparse import OptionParser
import random
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.libsfcluster import SFCluster

class ModifyVolgroupLunAssignmentsAction(ActionBase):
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
                            "method" : lambda m: m in ['seq', 'rev', 'rand', 'vol'],
                            },
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")
        if args['method'] == 'seq' and ('lun_min' not in args or args['lun_min'] < 0):
            raise libsf.SfArgumentError("Please specify a valid min")
        if args['method'] == 'rev' and ('lun_max' not in args or args['lun_max'] < 0 or args['lun_max'] > 16383):
            raise libsf.SfArgumentError("Please specify a valid max")
        if args['method'] == 'rand' and (('lun_min' not in args or args['lun_min'] < 0) or ('lun_max' not in args or args['lun_max'] < 0 or args['lun_max'] > 16383)):
            raise libsf.SfArgumentError("Please specify a valid min and max")
        if 'lun_min' in args and 'lun_max' in args and args['lun_min'] > args['lun_max']:
            raise libsf.SfArgumentError("Please specify a min < max")

    def Execute(self, method='seq', lun_min=0, lun_max=16383, volgroup_name=None, volgroup_id=0, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Renumber LUNS in the specified volume access group
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Get the cluster version so we know which endpoint to use
        cluster = SFCluster(mvip, username, password)
        try:
            api_version = cluster.GetAPIVersion()
        except libsf.SfError as e:
            mylog.error("Failed to get cluster version: " + str(e))
            mylog.info("Assuming API version 7.0")
            api_version = 7.0

        # Find the group
        mylog.info("Finding the volume group on the cluster")
        try:
            volgroup = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=volgroup_name, VagId=volgroup_id, ApiVersion=api_version)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        group_volume_ids = []
        if 'volumes' in volgroup:
            group_volume_ids = [v for v in volgroup['volumes']]
        elif 'volumeLunAssignments' in volgroup:
            group_volume_ids = [v['volumeID'] for v in volgroup['volumeLunAssignments']]
        group_volume_ids = sorted(group_volume_ids)

        # Check that the min/max will work for this volume group
        if method == 'seq':
            if lun_min + len(group_volume_ids) > 16383:
                mylog.error("min LUN value is too high - volumes will exceed LUN 16383")
                self.RaiseFailureEvent(message="min LUN value is too high - volumes will exceed LUN 16383")
                return False
        elif method == 'rev':
            if lun_min + len(group_volume_ids) > 16383:
                mylog.error("min LUN value is too high - volumes will exceed LUN 16383")
                self.RaiseFailureEvent(message="min LUN value is too high - volumes will exceed LUN 16383")
                return False
        elif method == 'rand':
            if lun_max - lun_min + 1 < len(group_volume_ids):
                mylog.error("min to max range is too small to fit all volumes in this group")
                self.RaiseFailureEvent(message="min to max range is too small to fit all volumes in this group")
                return False
        elif method == 'vol':
            if max(group_volume_ids) > 16383:
                mylog.error("Max volume ID is too large to use as a LUN number")
                self.RaiseFailureEvent(message="Max volume ID is too large to use as a LUN number")
                return False

        # Create the new LUN assignments
        lun_assignments = []
        if method == 'seq':
            lun = lun_min
            for volume_id in group_volume_ids:
                lun_assignments.append({'volumeID' : volume_id, 'logicalUnitNumber' : lun, 'lun' : lun}) # specify both 'lun' and 'logicalUnitNumber' to cover changes in the API
                lun += 1
        elif method == 'rev':
            lun = lun_max
            for volume_id in group_volume_ids:
                lun_assignments.append({'volumeID' : volume_id, 'logicalUnitNumber' : lun, 'lun' : lun})
                lun -= 1
        elif method == 'rand':
            luns = range(lun_min, lun_max+1)
            random.shuffle(luns)
            for i, volume_id in enumerate(group_volume_ids):
                lun_assignments.append({'volumeID' : volume_id, 'logicalUnitNumber' : luns[i], 'lun' : luns[i]})
        elif method == 'vol':
            for volume_id in group_volume_ids:
                lun_assignments.append({'volumeID' : volume_id, 'logicalUnitNumber' : volume_id, 'lun' : volume_id})

        # Modify the group
        mylog.info("Modifying group " + volgroup["name"])
        params = {}
        params["volumeAccessGroupID"] = volgroup["volumeAccessGroupID"]
        params["volumeLunAssignments"] = lun_assignments
        params["lunAssignments"] = lun_assignments
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroupLunAssignments", params, ApiVersion=api_version)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully modified LUN assignments")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__),
                          epilog="""The --method flag sets how renumbering is done.
                          'seq' means number sequentially, starting from --min.
                          'rev' means number seqentially in reverse, starting from --max.
                          'rand' means number using values randomly selected between --min and --max.
                          'vol' means to use the volumeID as the LUN number.""")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the group")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the group")
    parser.add_option("--method", type="choice", dest="method", choices=['seq', 'rev', 'rand', 'vol'], default='seq', help="The method to use for renumbering")
    parser.add_option("--min", type="int", dest="min", default=0, help="the smallest LUN number to use")
    parser.add_option("--max", type="int", dest="max", default=16383, help="the largest LUN number to use")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(method=options.method, lun_min=options.min, lun_max=options.max, volgroup_name=options.volgroup_name, volgroup_id=options.volgroup_id, mvip=options.mvip, username=options.username, password=options.password, debug=options.debug):
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
