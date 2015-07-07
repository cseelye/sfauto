"""
This script will remove half the the BS drives from a single random node
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

"""


import sys
from optparse import OptionParser
import logging
import multiprocessing
import random
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import check_vm_health_clientmon
import check_cluster_health
import get_random_node
import wait_syncing
import add_available_drives
import clusterbscheck


class SmallScaleBlockDriveRemovalAction(ActionBase):
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



    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmType=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, nomaster=True, threadMax=sfdefaults.parallel_max, bsCheck=True, skipVM=False, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)


        mylog.step("Checking the Cluster health")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy to begin with")
            return False

        mylog.step("Preforming a Cluster BS Check")
        if clusterbscheck.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Cluster BS Check Failed")
            return False

        if skipVM == False:
            mylog.step("Checking the VMs health")
            if check_vm_health_clientmon.Execute(vmType=vmType, clientUser=clientUser, clientPass=clientPass, threadMax=threadMax) == False:
                mylog.error("The VMs are not healthy to begin with")
                return False

            waitTime = 60
            healthThread = multiprocessing.Process(target=self._checkVMHealthThread, args=(vmType, clientUser, clientPass, threadMax, waitTime))
            healthThread.start()


        #get a random node
        random_node_ip = get_random_node.Get(mvip=mvip, username=username, password=password, nomaster=nomaster)
        if random_node_ip == False:
            mylog.error("There was an error getting a random node")
            if skipVM == False:
                healthThread.terminate()
            return False

        #get a list of block service drives

        try:
            node_list = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("Unable to get a list of active nodes. Message: " + str(e))
            if skipVM == False:
                healthThread.terminate()
            return False

        random_node_id = None
        for node in node_list["nodes"]:
            if node["mip"] == random_node_ip:
                random_node_id = node["nodeID"]

        bs_drive_id_list = []
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("Unable to get list of drives. Message: " + str(e))
            if skipVM == False:
                healthThread.terminate()
            return False

        for drive in result["drives"]:
            if drive["nodeID"] == random_node_id:
                if drive["type"] == "block" and drive["status"] == "active":
                    bs_drive_id_list.append(drive["driveID"])


        #remove half the drives from the list
        for i in xrange(0, len(bs_drive_id_list)/2):
            random_index = random.randint(0, len(bs_drive_id_list) - 1)
            bs_drive_id_list.pop(random_index)
        
        mylog.step("Trying to remove block drives")
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': bs_drive_id_list})
            mylog.info("The block drives have been removed.")
        except libsf.SfError as e:
            mylog.error("Failed to remove drives: " + str(e))
            if skipVM == False:
                healthThread.terminate()
            return False

        #wait for one minute
        mylog.step("Waiting for 1 minute")
        time.sleep(60)

        #wait for syncing to finish
        mylog.step("Waiting for everything to sync on cluster: " + mvip)
        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Timed out waiting for syncing")
            if skipVM == False:
                healthThread.terminate()
            return False

        #once syncing has finished do a health check on the cluster
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            if skipVM == False:
                healthThread.terminate()
            return False

        mylog.step("Preforming a Cluster BS Check")
        if clusterbscheck.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Cluster BS Check Failed")
            if skipVM == False:
                healthThread.terminate()    
            return False


        #if all is good add the BS drives back to the cluster
        if add_available_drives.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("There was an error trying to add the block drives back to the cluster")
            if skipVM == False:
                healthThread.terminate()
            return False

        #make sure cluster is healthy
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            if skipVM == False:
                healthThread.terminate()
            return False

        #a check to report if any of the health threads failed
        if self.Healthy.value == False:
            mylog.warning("The VMs were not healthy thoughout the entire test")
            if skipVM == False:
                healthThread.terminate()
            return False

        #done
        if skipVM == False:
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
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--nomaster", action="store_true", dest="nomaster", default=False, help="do not select the cluster master")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=sfdefaults.parallel_max, help="The number of threads to use when checking a client's health")
    parser.add_option("--skip_vms", action="store_true", dest="skip_vms", default=False, help="Skip checking the health of the VMs. ClientMonitor")
    parser.add_option("--bs_check", action="store_true", dest="bs_check", default=False, help="Do a BS check when checking the cluster health")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vm_type, options.client_user, options.client_pass, options.ssh_user, options.ssh_pass, options.nomaster, options.thread_max, options.bs_check, options.skip_vms, options.debug):
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