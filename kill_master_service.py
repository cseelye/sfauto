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

    --iterations        The number of times to kill the master service default is nodes*2
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
import check_vm_health_clientmon
import wait_for_cluster_healthy


class KillMasterServiceAction(ActionBase):
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


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, vmType=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, iterations=None, threadMax=sfdefaults.parallel_max, debug=False):

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
            mylog.info(cluster_master + " Killing the master service")
            master_node.KillMasterService()

            #wait for a little bit
            mylog.step("Waiting for 60 seconds")
            time.sleep(60)

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

        #a check to report if any of the health threads failed
        if self.Healthy.value == False:
            mylog.warning("The VMs were not healthy thoughout the entire test")
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

    parser.add_option("--vm_type", type="string", dest="vm_type", default=sfdefaults.vmhost_kvm, help="The type of VM to check, ex: KVM")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--iterations", type="int", dest="iterations", default=None, help="the number of times to kill the master service")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=sfdefaults.parallel_max, help="The number of threads to use when checking a client's health")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()


    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.ssh_user, options.ssh_pass, options.vm_type, options.client_user, options.client_pass, options.iterations, options.thread_max, options.debug):
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