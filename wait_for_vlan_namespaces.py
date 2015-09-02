#!/usr/bin/env python2.7

"""
This action will wait for all VLAN namespaces, interfaces up, iSCSI listening

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --timeout           How long to wait (sec) before giving up
"""

import sys
from optparse import OptionParser
import re
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase

class WaitForVlanNamespacesAction(ActionBase):
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
                            "timeout" : libsf.IsPositiveInteger},
            args)

    def Execute(self, timeout=sfdefaults.vlan_healthy_timeout, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Wait for VLAN namespaces to be ready
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Waiting for all VLAN namespaces to be ready on {}".format(mvip))

        cluster = libsfcluster.SFCluster(mvip, username, password)
        node_ips = cluster.ListActiveNodeIPs()
        cluster_vlans = cluster.ListVLANs()

        start_time = time.time()
        previous_sample = {}
        while True:
            
            allgood = True
            for node_ip in node_ips:
                nodegood = True
                ssh = libsf.ConnectSsh(node_ip, ssh_user, ssh_pass)

                # Check for all namespaces created
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "ns-list")
                namespace_list = [line.strip() for line in stdout.readlines()]
                mylog.debug("Found {} namespaces {}".format(len(namespace_list),",".join(namespace_list)))
                if len(namespace_list) != len(cluster_vlans):
                    nodegood = False
                    mylog.warning("{}: {} of {} namespaces on the node".format(node_ip, len(namespace_list), len(cluster_vlans)))
                else:
                    mylog.info("{}: All namespaces present".format(node_ip))
                
                # Check for interfaces in all namespaces
                if nodegood:
                    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo ns-ifconfig 2>&1 | grep Bond10G | awk '{print $1}'")
                    interfaces = [line.strip() for line in stdout.readlines()]
                    mylog.debug("Found {} interfaces {}".format(len(interfaces), ",".join(interfaces)))
                    missing = []
                    for ns_name in namespace_list:
                        tag = int(ns_name.split("_")[1])
                        ifname = "Bond10G.{}".format(tag)
                        if ifname not in interfaces:
                            missing.append(ifname)
                    if len(missing) > 0:
                        nodegood = False
                        mylog.warning("{}: Missing interfaces {}".format(node_ip, ",".join(missing)))
                    else:
                        mylog.info("{}: All interfaces present".format(node_ip))
                
                # Check for iSCSI listening in the namespace
                if nodegood:
                    stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo ns-listen 2>&1 | egrep 'namespace|3260'")
                    listeners = {}
                    for line in stdout.readlines():
                        if "default namespace" in line:
                            current_namespace = "base"
                            listeners[current_namespace] = None
                            continue;
                        m = re.search("(namespace_\d+)", line)
                        if m:
                            current_namespace = m.group(1)
                            listeners[current_namespace] = None
                            continue
                        m = re.search("0\.0\.0\.0:3260.+LISTEN\s+(\S+)", line)
                        if m and current_namespace:
                            listeners[current_namespace] = m.group(1)
                    missing = []
                    for namespace, listener in listeners.iteritems():
                        if not listener:
                            missing.append(namespace)
                    if len(missing) > 0:
                        nodegood = False
                        mylog.warning("{}: Not listening for iSCSI in {} namespaces {}".format(node_ip, len(missing), ",".join(sorted(missing))))
                    else:
                        mylog.info("{}: Listening for iSCSI in all namespaces".format(node_ip))

                if nodegood:
                    mylog.passed("{} all namespaces are healthy".format(node_ip))
                else:
                    allgood = False

            if allgood:
                break
            time.sleep(30)
            if time.time() - start_time > timeout:
                mylog.error("Timeout waiting for namespaces")
                return False

        end_time = time.time()
        duration = end_time - start_time

        mylog.info("Duration " + libsf.SecondsToElapsedStr(duration))
        mylog.passed("All namespaces on all nodes are ready")
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
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--timeout", type="int", dest="timeout", default=sfdefaults.vlan_healthy_timeout, help="how long to wait (sec) before giving up [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(timeout=options.timeout, mvip=options.mvip, username=options.username, password=options.password, ssh_user=options.ssh_user, ssh_pass=options.ssh_pass, debug=options.debug):
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

