#!/usr/bin/env python2.7

"""
This action will display a list of the VLAN tags in the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ListVlanTagsAction(ActionBase):
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

    def Get(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Get a list of VLAN tags in the cluster
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        # Get a list of VLANs in the cluster
        mylog.info("Getting a list of VLANs in cluster " + mvip)
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListVirtualNetworks', {}, ApiVersion=8.0)
        except libsf.SfError as e:
            mylog.error("Failed to get VLAN list: " + e.message)
            return False

        tag_list = []
        for vlan in result["virtualNetworks"]:
            tag_list.append(vlan["virtualNetworkTag"])

        return tag_list

    def Execute(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Show the list of VLAN tags in the cluster
        """
        del self
        tag_list = Get(**locals())
        if tag_list is False:
            return False

        # Display the list in the requested format
        if csv or bash:
            separator = ","
            if bash:
                separator = " "
            sys.stdout.write(separator.join([str(i) for i in tag_list]) + "\n")
            sys.stdout.flush()
        else:
            mylog.info(str(len(tag_list)) + " VLANs in cluster " + mvip)
            for tag in tag_list:
                mylog.info("  {}".format(tag))
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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.csv, options.bash, options.username, options.password, options.debug):
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

