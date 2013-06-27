"""
This script will remove half the Slice drives from a single random node
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
import get_random_node
import wait_syncing
import add_available_drives

class KvmSmallScaleSliceDriveRemovalAction(ActionBase):
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
        mylog.step("Starting Health Monitoring Thread")
        while True:
            if kvm_check_vm_health.Execute(vmHost, hostUser, hostPass, vmNames, True) == False:
                mylog.error("The VMs are not healthy. Bad News")
            time.sleep(waitTime)



    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, debug=False):

        mylog.step("Checking the Cluster health")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy to begin with")
            return False

        vmNames = kvm_list_vm_names.Get(vmhost=vmHost, host_user=hostUser, host_pass=hostPass, vm_regex="clone")
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


        #get a random node
        random_node_ip = get_random_node.Get(mvip=mvip, username=username, password=password)

        if random_node_ip == False:
            mylog.error("There was an error getting a random node from " + mvip)
            healthThread.terminate()
            return False
        #get a list of block service drives
        try:
            node_list = libsf.CallApiMethod(mvip, username, password, "ListActiveNodes", {})
        except libsf.SfError as e:
            mylog.error("There was an error trying to get a list of active nodes. Message: " + str(e))
            healthThread.terminate()
            return False

        random_node_id = None
        for node in node_list["nodes"]:
            if node["mip"] == random_node_ip:
                random_node_id = node["nodeID"]

        slice_drive_id_list = []
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListDrives", {})
        except libsf.SfError as e:
            mylog.error("There was an error trying to get the list of drives. Message: " + str(e))
            healthThread.terminate()
            return False

        for drive in result["drives"]:
            if drive["nodeID"] == random_node_id:
                if drive["type"] == "volume" and drive["status"] == "active":
                    slice_drive_id_list.append(int(drive["driveID"]))


        #remove half the drives from the list
        if len(slice_drive_id_list) > 1:
            for i in xrange(0, len(slice_drive_id_list)/2):
                random_index = random.randint(0, len(slice_drive_id_list) - 1)
                slice_drive_id_list.pop(random_index)

        mylog.step("Removing 1 Slice Drive")
        try:
            libsf.CallApiMethod(mvip, username, password, "RemoveDrives", {'drives': slice_drive_id_list})
            mylog.info("1 slice drive has been removed")
        except libsf.SfError as e:
            mylog.error("Failed to remove drives: " + str(e))
            healthThread.terminate()
            return False

        mylog.step("Waiting for 1 minute")
        time.sleep(60)

        #wait for syncing to finish
        mylog.step("Waiting for everything to sync on cluster: " + mvip)
        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Timed out waiting for syncing")
            healthThread.terminate()
            return False

        #once syncing has finished do a health check on the cluster
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            healthThread.terminate()
            return False

        #if all is good add the BS drives back to the cluster
        if add_available_drives.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Failed added drives back to cluster")
            healthThread.terminate()
            return False

        #make sure cluster is healthy
        mylog.step("Health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
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

    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--nomaster", action="store_true", dest="nomaster", default=False, help="do not select the cluster master")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.ssh_user, options.ssh_pass, options.debug):
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