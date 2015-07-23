#!/usr/bin/env python2.7

"""
This action will create a VLAN on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --tag               The tag for the VLAN

    --address_start     The beginning of the address block for the VLAN

    --address_size      The size of the address block for the VLAN

    --netmask           The netmask for the VLAN

    --svip              The SVIP for the VLAN

    --namespace         Put this VLAN into a namespace

    --strict            Fail if the VLAN already exists
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateVlanAction(ActionBase):
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
                            "tag" : libsf.IsValidVLANTag,
                            "address_start" : libsf.IsValidIpv4Address,
                            "address_size" : libsf.IsPositiveInteger,
                            "netmask" : libsf.IsValidIpv4Address,
                            "svip" : libsf.IsValidIpv4Address},
            args)

    def Execute(self, tag, address_start, address_size, netmask, svip, namespace=False, mvip=sfdefaults.mvip, strict=False, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create a VLAN
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Creating VLAN {} SVIP {} addresses {}/{}".format(tag, svip, address_start, netmask))
        params = {}
        params['virtualNetworkTag'] = tag
        params['name'] = "vlan-{}".format(tag)
        params['addressBlocks'] = []
        params['addressBlocks'].append({'start' : address_start, 'size' : address_size})
        params['netmask'] = netmask
        params['svip'] = svip
        params['namespace'] = namespace
        try:
            libsf.CallApiMethod(mvip, username, password, "AddVirtualNetwork", params, ApiVersion=8.0)
        except libsf.SfApiError as e:
            if (e.name == "xVirtualNetworkAlreadyExists" and not strict):
                mylog.passed("VLAN already exists")
                return True
            else:
                mylog.error(str(e))
                return False

        mylog.passed("VLAN created successfully")
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
    parser.add_option("--address_start", type="string", dest="address_start", default=None, help="the starting address for nodes on the VLAN")
    parser.add_option("--address_size", type="int", dest="address_size", default=None, help="the number of addresses for nodes on the VLAN")
    parser.add_option("--netmask", type="string", dest="netmask", default=None, help="the netmask for nodes on the VLAN")
    parser.add_option("--svip", type="string", dest="svip", default=None, help="the SVIP for the VLAN")
    parser.add_option("--namespaces", action="store_true", dest="namespace", default=False, help="put this VLAN into a namespace")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if the VLAN already exists")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(tag=options.tag, address_start=options.address_start, address_size=options.address_size, netmask=options.netmask, svip=options.svip, namespace=options.namespace, mvip=options.mvip, strict=options.strict, username=options.username, password=options.password, debug=options.debug):
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

