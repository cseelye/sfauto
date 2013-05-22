#!/usr/bin/python

"""
This action will add an IQN to a volume access group

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --iqn               The IQN to add

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group
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

class AddIqnToVolgroupAction(ActionBase):
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
                            "password" : None,
                            "iqn" : None},
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, iqn=None, mvip=sfdefaults.mvip, volgroup_name=None, volgroup_id=0, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Add list of IQNs to volume access group
        """
        if iqn == None:
            iqn = self.GetSharedValue(SharedValues.clientIQN)

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if volgroup_name:
            mylog.info("Adding " + iqn + " to group " + str(volgroup_name))
        else:
            mylog.info("Adding " + iqn + " to group " + str(volgroup_id))

        # Find the group
        try:
            volgroup = libsfcluster.SFCluster(mvip, username, password).FindVolumeAccessGroup(volgroupName=volgroup_name, volgroupID=volgroup_id)
        except SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Add the IQN
        self._RaiseEvent(self.Events.BEFORE_ADD)
        try:
            volgroup.AddInitiators([iqn])
        except SfError as e:
            mylog.error("Failed to modify group: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Added IQN to group")
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
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the group")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the group")
    parser.add_option("--iqn", type="string", dest="iqn", default=None, help="the initiator IQN to add to the group")
    parser.add_option("--debug", action="store_true", dest="debug", default=True, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.iqn, options.mvip, options.volgroup_name, options.volgroup_id, options.username, options.password, options.debug):
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
