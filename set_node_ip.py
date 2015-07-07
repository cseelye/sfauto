#!/usr/bin/env python2.7

"""
This action will set the network info of an available node

When run as a script, the following options/env variables apply:
    --node_ip           The node management IP

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --oneg_ip           The new 1G (management) IP address for the node

    --oneg_netmask      The new 1G netmask for the node

    --oneg_gateway      The new 1G gateway for the node

    --teng_ip           The new 10G (storage, cluster) IP address for the node

    --teng_netmask      The new 10G netmask for the node

    --dns_ip            The new DNS server for the node

    --dns_search        The new DNS search path for the node
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

class SetNodeIpAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_SET_NETINFO = "BEFORE_SET_NETINFO"
        AFTER_SET_NETINFO = "AFTER_SET_NETINFO"
        SET_NETINFO_FAILED = "SET_NETINFO_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ip" : libsf.IsValidIpv4Address,
                            "oneg_ip" : libsf.IsValidIpv4Address},
            args)

    def Execute(self, node_ip, username, password, oneg_ip, oneg_netmask, oneg_gateway, teng_ip, teng_netmask, dns_ip, dns_search, oneg_nic="Bond1G", teng_nic="Bond10G", debug=False):
        """
        Set the network info of an available node
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Setting the following network config on " + node_ip)
        mylog.info("  1G IP     : " + str(oneg_ip))
        mylog.info("  1G mask   : " + str(oneg_netmask))
        mylog.info("  1G gw     : " + str(oneg_gateway))
        if teng_ip:
            mylog.info("  10G IP    : " + str(teng_ip))
            mylog.info("  10G mask  : " + str(teng_netmask))
        mylog.info("  DNS server: " + str(dns_ip))
        mylog.info("  DNS search: " + str(dns_search))
        mylog.info("This may take up to 90 seconds")
        self._RaiseEvent(self.Events.BEFORE_SET_NETINFO, node_ip=node_ip)

        node = libsfnode.SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
        start = time.time()
        try:
            node.SetNetworkInfo(oneg_ip, oneg_netmask, oneg_gateway, dns_ip, dns_search, teng_ip, teng_netmask, oneg_nic, teng_nic)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self._RaiseEvent(self.Events.SET_NETINFO_FAILED, node_ip=node_ip)
            return False

        # The network config API doesn't like to get new requests too soon after changing the IP
        if time.time() - start < 70:
            time.sleep(70 - (time.time() - start))

        mylog.passed("Successfully set network info")
        self._RaiseEvent(self.Events.AFTER_SET_NETINFO, node_ip=node_ip)
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
    parser.add_option("--oneg_ip", type="string", dest="oneg_ip", default=None, help="the new 1G IP address for the node")
    parser.add_option("--oneg_netmask", type="string", dest="oneg_netmask", default=None, help="the new 1G netmask for the node")
    parser.add_option("--oneg_gateway", type="string", dest="oneg_gateway", default=None, help="the new 1G gateway for the node")
    parser.add_option("--oneg_nic", type="string", dest="oneg_nic", default="Bond1G", help="the name of the 1G network interface")
    parser.add_option("--teng_ip", type="string", dest="teng_ip", default=None, help="the new 10G IP address for the node")
    parser.add_option("--teng_netmask", type="string", dest="teng_netmask", default=None, help="the new 10G netmask for the node")
    parser.add_option("--teng_nic", type="string", dest="teng_nic", default="Bond10G", help="the name of the 10G network interface")
    parser.add_option("--dns_ip", type="string", dest="dns_ip", default=None, help="the new DNS IP address for the node")
    parser.add_option("--dns_search", type="string", dest="dns_search", default=None, help="the new DNS search path for the node")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(node_ip=options.node_ip, username=options.username, password=options.password, oneg_ip=options.oneg_ip, oneg_netmask=options.oneg_netmask, oneg_gateway=options.oneg_gateway, teng_ip=options.teng_ip, teng_netmask=options.teng_netmask, dns_ip=options.dns_ip, dns_search=options.dns_search, oneg_nic=options.oneg_nic, teng_nic=options.teng_nic, debug=options.debug):
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

