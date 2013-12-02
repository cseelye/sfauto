#!/usr/bin/python

"""
This action will add a network route to a node

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --network           The network to route to

    --netmask           The subnetmask of the network

    --gateway           The gateway to the network

"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libsfnode as libsfnode
import lib.sfdefaults as sfdefaults
import time

class Add10grouteToNodeAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address,
                            #"network" : libsf.IsValidIpv4Address,
                            #"netmask" : libsf.IsValidIpv4Address,
                            #"gateway" : libsf.IsValidIpv4Address
                            },
            args)

    def Execute(self, node_ip, username, password, network, netmask, gateway, debug=False):
        """
        Set the network info of an available node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info(node_ip + ": Adding route to " + network + "/" + netmask + " via " + gateway)

        node = libsfnode.SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
        try:
            node.AddNetworkRoute(network, netmask, gateway)
        except libsf.SfError as e:
            mylog.error("Failed to add route: " + str(e))
            self.RaiseFailureEvent("Failed to add route: " + str(e), nodeIP=node_ip, exception=e)
            return False

        mylog.passed("Successfully added route")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(description="Set the network information of a node")
    parser.add_option("-n", "--node_ip", type="string", dest="node_ip", default=None, help="the current IP address of the node")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="The username for the node")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="The password for the node")
    parser.add_option("--network", type="string", dest="network", default=None, help="the network to route to")
    parser.add_option("--netmask", type="string", dest="netmask", default=None, help="the subnet mask for the network")
    parser.add_option("--gateway", type="string", dest="gateway", default=None, help="the gateway to the network")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(node_ip=options.node_ip, username=options.username, password=options.password, network=options.network, netmask=options.netmask, gateway=options.gateway, debug=options.debug):
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
