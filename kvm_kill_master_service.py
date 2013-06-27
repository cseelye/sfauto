"""
This script will kill the master service on the master node.
Wait for the master node to switch over 
Wait for the cluster to become healthy again  
repeat untill number of iterations is reached

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

    --iterations        The number of times to kill the master service
"""


import sys
from optparse import OptionParser
from xml.etree import ElementTree
import logging
import multiprocessing
import platform
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libsfnode as libsfnode
import get_cluster_master
import check_cluster_health
import list_active_nodes
import kvm_check_vm_health
import kvm_list_vm_names
import wait_for_cluster_healthy


class KvmKillMasterServiceAction(ActionBase):
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


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, iterations=None, debug=False):

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


        if iterations == None:
            #get active nodes
            active_nodes = list_active_nodes.Get(mvip=mvip, username=username, password=password)
            if active_nodes == False:
                mylog.error("Was unable to get a list of active nodes from " + mvip)
                healthThread.terminate()
                return False
            iterations = len(active_nodes) * 2
            mylog.info("No value for iteration given. Setting it to the number of nodes * 2. Iterations = " + str(iterations))
        #get cluster master
        cluster_master = get_cluster_master.Get(mvip=mvip, username=username, password=password)
        if cluster_master == False:
            mylog.error("Unable to get get cluster master")
            healthThread.terminate()
            return False
        cluster_master = cluster_master[0]
        mylog.info("The cluster master is: " + cluster_master)



        #loop over iterations
        for i in xrange(0, iterations):

            mylog.banner("Iteration " + str(i + 1) + " of " + str(iterations))

            #create sfnode
            master_node = libsfnode.SFNode(ip=cluster_master, sshUsername=nodeSshUser, sshPassword=nodeSshPass, clusterUsername=username, clusterPassword=password)
            
            #kill master service on that node
            master_node.KillMasterService()

            #wait for a little bit
            mylog.step("Waiting for 30 seconds")
            time.sleep(30)

            got_new_master = False

            while not got_new_master:
                #get the new cluster master
                new_cluster_master = get_cluster_master.Get(mvip=mvip, username=username, password=password)
                if new_cluster_master == False:
                    mylog.error("Unable to get get cluster master.")
                    healthThread.terminate()
                    return False
                #if the cluster master is the same then wait longer and check again
                if cluster_master != new_cluster_master[0]:
                    mylog.info("The master node has switched. The new master is: " + new_cluster_master[0])
                    cluster_master = new_cluster_master[0]
                    got_new_master = True

                    if wait_for_cluster_healthy.Execute(mvip=mvip, username=username, password=password) == False:
                        mylog.error("There wasn an error waiting for the cluster to become healthy")
                        healthThread.terminate()
                        return False
                else:
                    time.sleep(10)

        #make sure cluster is healthy
        mylog.step("Final health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy")
            healthThread.terminate()
            return False
        
        mylog.passed("The cluster is healthy over " + str(iterations) + " iterations of killing the master service")
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
    parser.add_option("--iterations", type="int", dest="iterations", default=None, help="the number of times to kill the master service")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.ssh_user, options.ssh_pass, options.vmhost, options.host_user, options.host_pass, options.iterations, options.debug):
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