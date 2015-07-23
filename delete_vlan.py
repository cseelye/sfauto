#!/usr/bin/env python2.7

"""
This action will delete a VLAN on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --tag               The tag for the VLAN
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class DeleteVlanAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "tag" : libsf.IsValidVLANTag},
            args)

    def Execute(self, tag, mvip=sfdefaults.mvip, strict=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Delete a VLAN
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Deleting VLAN {}".format(tag))
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListVirtualNetworks", {}, ApiVersion=8.0)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            return False
        
        vlan_id = None
        for vlan in result['virtualNetworks']:
            if vlan['virtualNetworkTag'] == tag:
                vlan_id = vlan['virtualNetworkID']
                break
        if not vlan_id:
            if strict:
                mylog.error("VLAN {} not found".format(tag))
                return False
            else:
                mylog.passed("VLAN {} is already deleted".format(tag))
                return True

        params = {}
        params['virtualNetworkTag'] = tag
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveVirtualNetwork", params, ApiVersion=8.0)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            return False

        mylog.passed("VLAN deleted successfully")
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
    parser.add_option("--tag", type="int", dest="tag", default=None, help="the tag for the VLAN")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the VLAN is already gone")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(tag=options.tag, mvip=options.mvip, strict=options.strict, username=options.username, password=options.password, debug=options.debug):
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

