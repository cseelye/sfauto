"""
This script will power off one node
Wait for syncing to start and then finish
And make sure the VMs stay healthy the entire time

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
import multiprocessing
import logging
import time
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import lib.libsfnode as libsfnode
from lib.libsf import mylog
import lib.libsf as libsf
from clientmon.libclientmon import ClientMon
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import kvm_check_vm_health
import check_cluster_health
import kvm_list_vm_names
import get_random_node
import wait_syncing
import get_node_ipmi_ip
import count_available_drives
import add_available_drives

class KvmEntireNodeFailureAction(ActionBase):
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
                mylog.silence = False
                mylog.error("The VMs are not healthy. Bad News")
            time.sleep(waitTime)




    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, nodeSshUser=sfdefaults.ssh_user, nodeSshPass=sfdefaults.ssh_pass, nomaster=True, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #make sure cluster and VMs are healthy prior to starting
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

        #Pick 1 random node and power it off
        #wait for down
        random_node_ip = get_random_node.Get(mvip=mvip, username=username, password=password, nomaster=nomaster)
        ipmiIP = get_node_ipmi_ip.Get(node_ip=random_node_ip, ssh_user=nodeSshUser, ssh_pass=nodeSshPass)
        random_node = libsfnode.SFNode(ip=random_node_ip, sshUsername=nodeSshUser, sshPassword=nodeSshPass, clusterMvip=mvip, clusterUsername=username, ipmiIP=ipmiIP, ipmiUsername=sfdefaults.ipmi_user, ipmiPassword=sfdefaults.ipmi_pass)

        mylog.step("Powering Down node: " + random_node_ip)
        random_node.PowerOff()
        random_node.WaitForDown()
        mylog.info("Node: " + random_node_ip + " is down")

        mylog.step("Waiting for 3 minutes")
        time.sleep(180)

        #wait for syncing to start
        #when syncing finishes bring node back up
        mylog.step("Waiting for everything to sync on cluster: " + mvip)
        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Timed out waiting for syncing")
            healthThread.terminate()
            return False

        mylog.step("Syncing finished. Now powering on node: " + random_node_ip)
        random_node.PowerOn()
        random_node.WaitForUp()
        mylog.info("Node: " + random_node_ip + " is now powered on")

        mylog.step("Waiting for 3 minutes")
        time.sleep(180)

        #check for Available drives and if so add them to the cluster
        mylog.step("Look for available drives")
        if(count_available_drives.Execute(expected=1, compare="ge", mvip=mvip) == True):

            #add the drives back to the culster and wait for sync
            if(add_available_drives.Execute(mvip=mvip, username=username, password=password) == True):
                mylog.info("Available drives were added to the cluster")
            else:
                mylog.error("Available drives were not added to the cluster")
                healthThread.terminate()
                return False

        #wait for syncing to start again
        #wait for syncing to finish again
        mylog.step("Waiting for everything to sync on cluster: " + mvip)
        if wait_syncing.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("Timed out waiting for syncing")
            healthThread.terminate()
            return False

        #make sure cluster is healthy
        mylog.step("Final health check")
        if check_cluster_health.Execute(mvip=mvip, username=username, password=password) == False:
            mylog.error("The Cluster is not healthy to begin with")
            healthThread.terminate()
            return False

        #end
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
        if Execute(options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.ssh_user, options.ssh_pass, options.nomaster, options.debug):
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
