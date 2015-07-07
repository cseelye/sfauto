"""
This script will take a cluster that failed during a test and return it to a "normal" state

  When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --node_ips          Node IP address list
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --ipmi_user          The nodes ipmi username
    SFIPMI_USER env var

    --ipmi_pass          The nodes ipmi password
    SFIPMI_PASS

"""

import sys
import time
from optparse import OptionParser
import logging
import platform
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libclient as libclient
import lib.libsfnode as libsfnode
from lib.action_base import ActionBase
import get_node_ipmi_ip
import get_active_nodes
import wait_syncing
import add_available_drives
import check_cluster_health

class RestoreClusterDefaultsAction(ActionBase):
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



    def ipmiGuess(self, nodeIP, ipmiUser, ipmiPass):
        brokenIP = nodeIP.split(".")
        index = brokenIP.index("133")
        brokenIP.pop(index)
        brokenIP.insert(index, "134")
        ipmiIP = ".".join(brokenIP)
        if libsf.Ping(ipmiIP):
            return ipmiIP
        return False


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, nodeIPs=None, sshUser=sfdefaults.ssh_user, sshPass=sfdefaults.ssh_pass, ipmiUser=sfdefaults.ipmi_user, ipmiPass=sfdefaults.ipmi_pass, debug=False):
        
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if nodeIPs is None:
            nodeIPs = get_active_nodes.Get(mvip=mvip, username=username, password=password)
            if nodeIPs == False:
                mylog.error("There was an error trying to get the list of active nodes on: " + mvip)
                return False

        #checks to see if each node is alive
        #if node is not on will try to power it on and wait
        for node in nodeIPs:
            if libsf.Ping(node):
                mylog.info(node + " is alive")
            else:    
                ipmiIP = get_node_ipmi_ip.Get(node_ip=node, ssh_user=sshUser, ssh_pass=sshPass)
                if ipmiIP == False:
                    mylog.warning("Error trying to get ipmi IP address")
                    ipmiIP = self.ipmiGuess(node, ipmiUser, ipmiPass)
                    mylog.info("Going to use " + ipmiIP + " as the IPMI IP for node " + node)
                    if ipmiIP == False:
                        mylog.error("Unable to find an IPMI IP for the node: " + node)
                    else:
                        temp_node = libsfnode.SFNode(ip=node, sshUsername=sshUser, sshPassword=sshPass, clusterMvip=mvip, clusterUsername=username, clusterPassword=password, ipmiIP=ipmiIP, ipmiUsername=ipmiUser, ipmiPassword=ipmiPass)
                        mylog.info(node + " Trying to power on")
                        temp_node.PowerOn()

        mylog.info("Waiting for 1 minute")
        time.sleep(60)
        #make sure nothing is syncing right now
        mylog.step("Wait for everything to finish syncing")
        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Timed out waiting for the cluster to finish syncing")
            return False

        #look for drives to add
        mylog.step("Check for available drive")
        if add_available_drives.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("There was an error trying to add the drives back")
            return False

        mylog.info("Waiting for 1 minute")
        time.sleep(60)

        mylog.step("Checking the cluster health")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The cluster at: " + mvip + " is not healthy")
            return False

        mylog.passed("The cluster appears to be back to normal")
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes to add drives from")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--ipmi_user", type="string", dest="ipmi_user", default=sfdefaults.ipmi_user, help="the ipmi username for the nodes")
    parser.add_option("--ipmi_pass", type="string", dest="ipmi_pass", default=sfdefaults.ipmi_pass, help="the ipmi password for the nodes")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.node_ips, options.ssh_user, options.ssh_pass, options.ipmi_user, options.ipmi_pass, options.debug):
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
