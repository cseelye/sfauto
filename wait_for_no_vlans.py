#!/usr/bin/env python2.7

"""
This action will wait for there to be no VLANs on the list of nodes

When run as a script, the following options/env variables apply:
    --node_ips          List of node IP addresses
    SFNODE_IPS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS env var

    --timeout           How long to wait (sec) before giving up
"""

import sys
from optparse import OptionParser
import time
import logging
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.libsfnode import SFNode, NetworkInterfaceType
from lib.action_base import ActionBase

class WaitForNoVlansAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ips" : libsf.IsValidIpv4AddressList},
            args)

    def _NodeThread(self, node_ip, username, password, results):
        myname = multiprocessing.current_process().name
        results[myname] = False

        mylog.info("  {}: Waiting for no VLANs".format(node_ip))
        node = SFNode(ip=node_ip, clusterUsername=username, clusterPassword=password)
        while True:
            try:
                ifaces = node.ListNetworkInterfaces([NetworkInterfaceType.Vlan, NetworkInterfaceType.VirtualVlan])
            except libsf.SfError as e:
                results[myname] = False
                mylog.error("  {}: {}".format(node_ip, e))
                return

            # Wait until there are no VLAN interfaces
            if len(ifaces) > 0:
                time.sleep(5)
                continue

            if node.GetHighestVersion() >= 9.0:
                try:
                    namespaces = node.ListNetworkNamespaceInfo()
                except libsf.SfError as e:
                    results[myname] = False
                    mylog.error("  {}: {}".format(node_ip, e))
                    return
    
                # Wait until there is only a single namespace (base)
                if len(namespaces) > 1:
                    time.sleep(5)
                    continue

            mylog.info("  {}: No VLANs on node".format(node_ip))
            results[myname] = True
            return


    def Execute(self, node_ips=None, username=sfdefaults.username, password=sfdefaults.password, timeout=sfdefaults.vlan_healthy_timeout, debug=False):
        """
        Wait for no VLANs on the nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Waiting for there to be no VLANs on nodes")
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        start_time = time.time()
        for node_ip in node_ips:
            thread_name = "node-" + node_ip
            results[thread_name] = False
            th = multiprocessing.Process(target=self._NodeThread, name=thread_name, args=(node_ip, username, password, results))
            th.start()
            self._threads.append(th)

        while True:
            time.sleep(1)
            alldone = True
            for th in self._threads:
                if th.is_alive():
                    alldone = False
                    break
            if alldone:
                break

            if time.time() - start_time > timeout:
                mylog.error("Timeout waiting for VLANs")
                self.Abort()
                return False

        allgood = True
        for thread_name, result in results.items():
            if not result:
                allgood = False
                break

        if not allgood:
            mylog.error("Failed to wait for VLANs on nodes")
            return False

        end_time = time.time()
        duration = end_time - start_time

        mylog.info("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.passed("There are no VLANs on nodes")
        return True

# Instantiate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--timeout", type="int", dest="timeout", default=sfdefaults.vlan_healthy_timeout, help="how long to wait (sec) before giving up [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(node_ips=options.node_ips, username=options.username, password=options.password, timeout=options.timeout, debug=options.debug):
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

