#!/usr/bin/env python2.7

"""
This action will delete all volume access groups

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog, SfError
import lib.sfdefaults as sfdefaults
import logging
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class DeleteAllVolgroupsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Delete all volume access groups
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        
        mylog.info("Getting a list of volume groups")
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListVolumeAccessGroups", {})
        except libsf.SfApiError as e:
            mylog.error(str(e))
            return False

        allgood = True
        for group in result['volumeAccessGroups']:
            mylog.info("Deleting volume group {}".format(group['name']))
            params = {}
            params['volumeAccessGroupID'] = group['volumeAccessGroupID']
            try:
                libsf.CallApiMethod(mvip, username, password, "DeleteVolumeAccessGroup", params)
            except libsf.SfApiError as e:
                mylog.error(str(e))
                allgood = False

        if allgood:
            mylog.passed("Groups deleted successfully")
            return True
        else:
            mylog.error("Failed to delete all groups")
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
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the VAG to delete")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the VAG to delete")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the account has already been deleted")
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

