"""
This script will remove all the BS drives from a n number of nodes
Wait for syncing and make sure the VMs stay healthy the entire time
Once cluster is healthy again it will add those drives back to the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS
    
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --node_count        The number of nodes to remove all BS drives from

"""


import sys
from optparse import OptionParser
from xml.etree import ElementTree
import logging
import multiprocessing
import platform
import random
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import kvm_check_vm_health
import check_cluster_health
import kvm_list_vm_names
import list_active_nodes
import wait_syncing
import remove_drives
import add_drives

class KvmLargeScaleBlockDriveRemovalAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmHost" : libsf.IsValidIpv4Address,
                            "hostUser" : None,
                            "hostPass" : None},
            args)

    def _checkVMHealthThread(self, vmHost, hostUser, hostPass, vmNames, waitTime=30):
        mylog.info("Started VM Health Thread")
        while True:
            if kvm_check_vm_health.Execute(vmHost, hostUser, hostPass, vmNames, True) == False:
                mylog.error("The VMs are not healthy. Bad News")
            time.sleep(waitTime)



    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, numberOfNodes=2, nomaster=True, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.step("Checking the Cluster health")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy to begin with")
            return False

        vmNames = kvm_list_vm_names.Get(vmhost=vmHost, host_user=hostUser, host_pass=hostPass)
        if vmNames == False:
            mylog.error("Failed getting list of VM names")
            return False


        mylog.step("Checking the VMs health")
        if kvm_check_vm_health.Execute(vmHost, hostUser, hostPass, vmNames) == False:
            mylog.error("The VMs are not healthy to begin with")
            return False

        waitTime = 30
        healthThread = multiprocessing.Process(target=self._checkVMHealthThread, args=(vmHost, hostUser, hostPass, vmNames, waitTime))
        healthThread.daemon = True
        healthThread.start()


        #get a random list of node IPs and node IDs
        try:
            node_list = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("There wasn an error getting a list of active nodes. Message " + str(e))
            healthThread.terminate()
            return False

        random_node_list = []
        if len(node_list["nodes"]) - numberOfNodes >=3:
            for i in xrange(0, numberOfNodes):
                random_index = random.randint(0, len(node_list["nodes"]) - 1)
                temp = node_list["nodes"][random_index]
                temp = temp["mip"], temp["nodeID"]
                node_list["nodes"].pop(random_index)
                random_node_list.append(temp)
        else:
            mylog.error("You can't remove that many drives from nodes. Need at least 3 nodes left")
            healthThread.terminate()
            return False
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("There was an error getting a list of drives")
            healthThread.terminate()
            return False

        #loop over each node
        for node in random_node_list:

            bs_drive_slot_list = []
            for drive in result["drives"]:
                if drive["nodeID"] == node[1]:
                    if drive["type"] == "block":
                        bs_drive_slot_list.append(drive["slot"])


            node_ips = []
            node_ips.append(node[0])

            if remove_drives.Execute(mvip=mvip, node_ips=node_ips, drive_slots=bs_drive_slot_list, username=username, password=password) == False:
                mylog.error("There was an error trying to remove half the BS drives")
                healthThread.terminate()
                return False

            #once syncing has finished do a health check on the cluster
            mylog.step("Health check")
            if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
                mylog.error("The Cluster is not healthy")
                healthThread.terminate()
                return False

            #if all is good add the BS drives back to the cluster
            if add_drives.Execute(mvip=mvip, node_ips=node_ips, drive_slots=bs_drive_slot_list, username=username, password=password) == False:
                mylog.error("There was an error trying to remove half the BS drives")
                healthThread.terminate()
                return False

        #make sure cluster is healthy
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            healthThread.terminate()
            return False
        #done
        mylog.passed("The cluster passed the test")
        healthThread.terminate()
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))

    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--node_count", type="int", dest="node_count", default=2, help="The number of nodes to remove all BS drives from")
    parser.add_option("--nomaster", action="store_true", dest="nomaster", default=False, help="do not select the cluster master")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.ssh_user, options.ssh_pass, options.node_count, options.nomaster, options.debug):
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