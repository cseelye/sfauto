"""
This script will to a complete kvm setup
    1. it will take in a list of nodes 
    2. turn them into a cluster
    3. mount nfs datastore
    4. create volume on cluster and login on vmhost
    5. clone qcow2 image to volume (raw)
    6. import VM 
    7. clone template VM n number of times 
    8. boot all VMs except template
    8. preform health check on VMs 

        When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
    --vm_host        The IP address of the vmHost

    --host_user       The username for the vmHost
    SFCLIENT_USER env var

    --host_pass       The password for the vmHost

    --cpu_count       How many virt CPUs to have in the VM

    --memory_size     How much virt memory to have in the VM

    --vm_name         Name of the VM

    --qcow2_path      path to the qcow2 image

    --raw_path        path to the raw volume - iscsi volume

    --os_type         the type of os to be on the VM: linux

    --nfs_ip            The IP address of the nfs datastore

    --nfs_path          The path on the nfs datastore you want to mount

    --mount_point       The location on the client where you want to mount the nfs datastore

    --skip_cluster      Skips making a cluster, mvip still needed but not node_ips, cluster_name, node_count
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
from lib.action_base import ActionBase
import kvm_complete_creation_test
import kvm_hypervisor_setup
import make_cluster
import check_vm_health_clientmon
import get_volume_iqn
import create_volumes
import login_client
import mount_volumes_test

class KvmCompleteClusterSetupAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgsCluster(self, args):
        libsf.ValidateArgs({"vmHost" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
                            "svip" : libsf.IsValidIpv4Address,
                            "nodeIPs" : libsf.IsValidIpv4AddressList,
                            "hostUser" : None,
                            "hostPass" : None,
                            "username" : None,
                            "password" : None,
                            "cloneCount" : libsf.IsInteger,
                            "nodeCount" : libsf.IsInteger,
                            "clusterName" : None,
                            "cloneName" : None,
                            "qcow2Name" : None,
                            "nfsMountPoint" : None,
                            "nfsPath" : None,
                            "nfsIP" : libsf.IsValidIpv4Address},
            args)

    def ValidateArgsRegular(self, args):
        libsf.ValidateArgs({"vmHost" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
                            "hostUser" : None,
                            "hostPass" : None,
                            "username" : None,
                            "password" : None,
                            "cloneCount" : libsf.IsInteger,
                            "cloneName" : None,
                            "qcow2Name" : None,
                            "nfsMountPoint" : None,
                            "nfsPath" : None,
                            "nfsIP" : libsf.IsValidIpv4Address},
            args)


    def Execute(self, nodeIPs, nodeCount, mvip, svip, clusterName, username, password, vmHost, hostUser, hostPass, nfsIP, nfsPath, nfsMountPoint, qcow2Name, cloneCount, cloneName, sourceName, skipCluster=False, debug=False):

        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if "_" in cloneName:
            mylog.error("Please rename the clone_name. No '_' permitted")
            return False

        if skipCluster == False:
            self.ValidateArgsCluster(locals())

            mylog.banner("Making Cluster")
            start_time = time.time()
            if make_cluster.Execute(clusterName=clusterName, mvip=mvip, svip=svip, nodeIPs=nodeIPs, nodeCount=nodeCount, username=username, password=password) == False:
                mylog.error("There was an error trying to make the cluster")
                return False
            mylog.time("It took " + libsf.SecondsToElapsedStr(time.time() - start_time) + " to make the cluster")
        else:
            self.ValidateArgsRegular(locals())

        mylog.banner("Setting up KVM Hypervisor")
        start_time = time.time()
        if kvm_hypervisor_setup.Execute(mvip, username, password, vmHost, hostUser, hostPass, nfsIP, nfsMountPoint, nfsPath) == False:
            mylog.error("There was an error trying to set up the hypervisor")
            return False
        mylog.time("It took " + libsf.SecondsToElapsedStr(time.time() - start_time) + " to set up the hypervisor")

        try:
            hypervisor = libclient.SfClient()
            hypervisor.Connect(vmHost, hostUser, hostPass)
            hypervisor_hostname = hypervisor.Hostname
        except libclient.ClientError as e:
            mylog.error(str(e))
            return False

        if "clone" in cloneName:
            template_name = cloneName.replace("clone", "template")
        else:
            template_name = cloneName + "-template"

        mylog.step("Creating 1 volume for client")
        if(create_volumes.Execute(volume_size=65, volume_count=1, volume_name_in=template_name, mvip=mvip, enable_512=False, username=username, password=password, min_iops=100, max_iops=100000, burst_iops=100000, account_name=hypervisor_hostname) == False):
            mylog.error("Failed trying to create 1st volume")
            self._RaiseEvent(self.Events.FAILURE)
            return False

        time.sleep(30)

        mylog.step("Logging in client")
        client_ips = [vmHost]
        if(login_client.Execute(mvip=mvip, client_ips=client_ips, username=username, password=password, client_user=hostUser, client_pass=hostPass) == False):
            mylog.error("Failed trying to log client in")
            self._RaiseEvent(self.Events.FAILURE)
            return False


        iqn = get_volume_iqn.Get(mvip=mvip, volume_name=template_name, username=username, password=password)
        if iqn == False:
            mylog.error("Could not get IQN for the template volume " + template_name)
            return False


        mylog.step("Mounting 1 volume on client")
        if(mount_volumes_test.Execute(clientIP=vmHost, clientUser=hostUser, clientPass=hostPass, iqn=iqn) == False):
            mylog.error("Failed trying to mount volumes on client")
            self._RaiseEvent(self.Events.FAILURE)
            return False


        #get the path to the raw volume 
        retcode, stdout, stderr = hypervisor.ExecuteCommand("ls /dev/disk/by-path/ | grep " + iqn)
        if retcode != 0:
            mylog.error("Could not find the path to the iscsi volume")
            return False
        else:
            stdout = stdout.split("\n")
            stdout.remove("")
            loc = stdout[-1]
            for line in stdout:
                temp = line
                if not "part" in temp:
                    loc = temp
        rawPath = "/dev/disk/by-path/" + loc

        cpuCount = 1
        memorySize = 512
        osType = "linux"

        qcow2Path = nfsMountPoint + "/" + qcow2Name
        #sourceName = "kvm-ubuntu-gold"

        mylog.banner("Setting up VMs on hypervisor")
        start_time = time.time()
        if kvm_complete_creation_test.Execute(mvip, vmHost, cloneCount, cloneName, sourceName, cpuCount, memorySize, osType, qcow2Path, rawPath, username, password, hostUser, hostPass) == False:
            mylog.error("There was an error trying to set up the VMs")
            return False
        mylog.time("It took " + libsf.SecondsToElapsedStr(time.time() - start_time) + " to set up " + str(cloneCount) + " VMs on the hypervisor")

        mylog.step("Waiting for 2 minutes for all the VMs to boot")
        time.sleep(120)

        if check_vm_health_clientmon.Execute(vmType="KVM") == False:
            mylog.error("Not all the VMs are healthy")
            return False

        mylog.passed("The cluster has been set up with " + str(cloneCount) +" VMs")
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    #cluster info
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-s", "--svip", type="string", dest="svip", default=sfdefaults.svip, help="the storage VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    parser.add_option("--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("--cluster_name", type="string", dest="cluster_name", default=None, help="The name of the cluster")
    parser.add_option("--node_count", type="int", dest="node_count", default=3, help="How many nodes to be in the cluster, min = 3")
    parser.add_option("--skip_cluster", action="store_true", dest="skip_cluster", default=False, help="Skips making a cluster. svip, node_ips, node_count, and cluster_name are not needed")
    #vmhost info
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--nfs_ip", type="string", dest="nfs_ip", default=sfdefaults.nfs_ip, help="the IP address of the nfs datastore")
    parser.add_option("--nfs_path", type="string", dest="nfs_path", default=sfdefaults.kvm_nfs_path, help="the path you want to mount from the nfs datastore")
    parser.add_option("--mount_point", type="string", dest="mount_point", default=sfdefaults.nfs_mount_point, help="the location you want to mount the nfs datasore on the client, ex: /mnt/nfs")
    #vm info
    parser.add_option("--qcow2_name", type="string", dest="qcow2_name", default=sfdefaults.kvm_qcow2_name, help="name of the qcow2 image")
    parser.add_option("--cpu_count", type="int", dest="cpu_count", default=sfdefaults.kvm_cpu_count, help="The number of virtural CPUs the VM should have")
    parser.add_option("--memory_size", type="int", dest="memory_size", default=sfdefaults.kvm_mem_size, help="The size of memory in MB for the vm, default 512MB")
    parser.add_option("--os_type", type="string", dest="os_type", default=sfdefaults.kvm_os, help="The OS type of the VM")
    #vm cloning info
    parser.add_option("--template_name", type="string", dest="source_name", default=None, help="the name of the VM to import and then clone from")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=sfdefaults.kvm_clone_name, help="the name of the cloned VMs")
    parser.add_option("--vm_count", type="int", dest="clone_count", default=None, help="the number of VMs to make")
    #debug
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.node_count, options.mvip, options.svip, options.cluster_name, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.nfs_ip, options.nfs_path, options.mount_point, options.qcow2_name, options.clone_count, options.clone_name, options.source_name, options.skip_cluster, options.debug):
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




