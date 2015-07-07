"""
This script will all the Slice drives from a n number of nodes
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

    --node_count        The number of nodes to remove all Slice drives from

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
import check_vm_health_clientmon
import check_cluster_health
import wait_syncing
import add_available_drives

class LargeScaleSliceDriveRemovalAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)
        self.Healthy = multiprocessing.Value('b', True)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientUser" : None,
                            "clientPass" : None,
                            "threadMax" : libsf.IsInteger},
            args)



    def _checkVMHealthThread(self, vmType, clientUser, clientPass, threadMax, waitTime=30):
        mylog.info("Started VM Health Thread")
        while True:
            if check_vm_health_clientmon.Execute(vmType=vmType, clientUser=clientUser, clientPass=clientPass, noLogs=True, threadMax=threadMax) == False:
                self.Healthy.value = False
            time.sleep(waitTime)



    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmType=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, numberOfNodes=2, threadMax=sfdefaults.parallel_max, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.step("Checking the Cluster health")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy to begin with")
            return False

        mylog.step("Checking the VMs health")
        if check_vm_health_clientmon.Execute(vmType=vmType, clientUser=clientUser, clientPass=clientPass, threadMax=threadMax) == False:
            mylog.error("The VMs are not healthy to begin with")
            return False

        waitTime = 60
        healthThread = multiprocessing.Process(target=self._checkVMHealthThread, args=(vmType, clientUser, clientPass, threadMax, waitTime))
        healthThread.start()


        #get a random list of node IPs and node IDs
        try:
            node_list = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("There was an error trying to get a list of active nodes. Message: " + str(e))
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

        #loop over each node


        slice_drive_id_list = []
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("There was an error trying to get a list of drives. Message: " + str(e))
            healthThread.terminate()
            return False

        for node in random_node_list:
            for drive in result["drives"]:
                if drive["nodeID"] == node[1]:
                    if drive["type"] == "volume" and drive["status"] == "active":
                        slice_drive_id_list.append(int(drive["driveID"]))

        mylog.step("Trying to remove slice drives")
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': slice_drive_id_list})
            mylog.info("Slice drives have been removed")
        except libsf.SfError as e:
            mylog.error("Failed to remove drives: " + str(e))
            healthThread.terminate()
            return False

        mylog.step("Waiting for 1 minute")
        time.sleep(60)

        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Error waiting for cluster to sync")
            healthThread.terminate()
            return False

        #once syncing has finished do a health check on the cluster
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            healthThread.terminate()
            return False

        #if all is good add the slice drives back to the cluster
        if add_available_drives.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("There was an error trying to add the slice drives back")
            healthThread.terminate()
            return False

        #make sure cluster is healthy
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            healthThread.terminate()
            return False

        #a check to report if any of the health threads failed
        if self.Healthy.value == False:
            mylog.warning("The VMs were not healthy thoughout the entire test")
            healthThread.terminate()
            return False

        #done
        healthThread.terminate()
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))

    parser.add_option("--vm_type", type="string", dest="vm_type", default=sfdefaults.vmhost_kvm, help="The type of VM to check, ex: KVM")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--node_count", type="int", dest="node_count", default=2, help="The number of nodes to remove all BS drives from")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=sfdefaults.parallel_max, help="The number of threads to use when checking a client's health")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vm_type, options.client_user, options.client_pass, options.node_count, options.thread_max, options.debug):
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