#!/usr/bin/python

"""
This action will delete a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volgroup_name     The name of the group to create

    --volgroup_id       The ID of the group to delete

    --strict            Fail if the group has already been deleted
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

class DeleteVolgroupAction(ActionBase):
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
                            "password" : None},
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, mvip=sfdefaults.mvip, volgroup_name=None, volgroup_id=0, strict=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Delete a volume access group
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if volgroup_name:
            mylog.info("Deleting volume access group '" + str(volgroup_name) + "'")
        else:
            mylog.info("Deleting volume access group " + str(volgroup_id))

        cluster = libsfcluster.SFCluster(mvip, username, password)

        # Find the group on the cluster
        try:
            volgroup = cluster.FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
        except libsf.SfApiError as e:
            mylog.error("Could not find volume group: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        except SfError:
            if strict:
                mylog.error("Group does not exist")
                self.RaiseFailureEvent(message="Group already exists")
                return False
            else:
                mylog.passed("Group does not exist")
                return True

        # Delete the group
        try:
            volgroup.Delete()
        except libsf.SfError as e:
            mylog.error("Failed to delete group: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Group deleted successfully")
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
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the VAG to delete")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the VAG to delete")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the account has already been deleted")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.volgroup_name, options.volgroup_id, options.strict, options.username, options.password, options.debug):
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

