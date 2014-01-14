"""
This script will to a complete kvm setup
    3. mount nfs datastore 
    4. create volume on cluster and login on vmhost
    5. clone qcow2 image to volume (raw)
    6. import VM 

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
import get_volume_iqn
import create_volumes
import login_client
import logout_client
import mount_volumes_test
import kvm_mount_nfs_datastore
import create_account_for_client
import kvm_clone_qcow2_to_raw_vm


class KvmEnvironmentSetupAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmHost" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
                            "hostUser" : None,
                            "hostPass" : None,
                            "username" : None,
                            "password" : None,
                            "qcow2Name" : None,
                            "nfsMountPoint" : None,
                            "nfsPath" : None,
                            "nfsIP" : libsf.IsValidIpv4Address},
            args)


    def Execute(self, mvip, username, password, vmHost, hostUser, hostPass, nfsIP, nfsPath, nfsMountPoint, qcow2Name, templateName, debug=False):

        if debug:
            mylog.console.setLevel(logging.DEBUG)

        self.ValidateArgs(locals())

        #log out of current volumes on client
        mylog.step("Trying to log out of client volumes")
        #logout_client and login_client need an array of clients - only using one
        vmHosts = [vmHost]
        if logout_client.Execute(client_ips=vmHosts, client_user=hostUser, client_pass=hostPass) == False:
            mylog.warning("Was unable to logout client prior to setting up new volumes")
            mylog.warning("Will try to continue")

        #create account for client on cluster
        mylog.step("Creating an account for client")
        if(create_account_for_client.Execute(mvip=mvip, client_ips=vmHosts, username=username, password=password, client_user=hostUser, client_pass=hostPass) == False):
            mylog.error("There was an error tying to create an account on the cluster")
            self._RaiseEvent(self.Events.FAILURE)
            return False
        #self._RaiseEvent(self.Events.CREATED_ACCOUNT)

        #connect to hypervisor
        try:
            hypervisor = libclient.SfClient()
            hypervisor.Connect(vmHost, hostUser, hostPass)
            hypervisor_hostname = hypervisor.Hostname
        except libclient.ClientError as e:
            mylog.error(str(e))
            return False

        #create template volume for the hypervisor
        mylog.step("Creating 1 volume for client")
        if(create_volumes.Execute(volume_size=65, volume_count=1, volume_name_in=templateName, mvip=mvip, enable_512=False, username=username, password=password, min_iops=100, max_iops=100000, burst_iops=100000, account_name=hypervisor_hostname) == False):
            mylog.error("Failed trying to create 1st volume")
            self._RaiseEvent(self.Events.FAILURE)
            return False

        time.sleep(30)

        #log into template volume
        mylog.step("Logging in client")
        client_ips = [vmHost]
        if(login_client.Execute(mvip=mvip, client_ips=client_ips, username=username, password=password, client_user=hostUser, client_pass=hostPass) == False):
            mylog.error("Failed trying to log client in")
            self._RaiseEvent(self.Events.FAILURE)
            return False

        #get the ign of the volume that was just created
        iqn = get_volume_iqn.Get(mvip=mvip, volume_name=templateName, username=username, password=password)
        if iqn == False:
            mylog.error("Could not get IQN for the template volume " + template_name)
            return False


        #mount the volume on the client
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

        if kvm_mount_nfs_datastore.Execute(clientIP=vmHost, clientUsername=hostUser, clientPassword=hostPass, nfsIP=nfsIP, nfsPath=nfsPath, mountPoint=nfsMountPoint) == False:
            mylog.error("There was an error mounting the NFS Datastore")
            return False

        qcow2Path = nfsMountPoint + "/" + qcow2Name
        #templateName = "kvm-ubuntu-gold"

        mylog.step("Cloning source image to raw image and importing VM")
        if (kvm_clone_qcow2_to_raw_vm.Execute(vmHost=vmHost, hostUser=hostUser, hostPass=hostPass, qcow2Path=qcow2Path, rawPath=rawPath, vmName=templateName, cpuCount=1, memorySize=512, osType="linux") == False):
            mylog.error("Unable to clone the source image to raw")
            return False


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    #cluster info
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    #vmhost info
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--nfs_ip", type="string", dest="nfs_ip", default=sfdefaults.nfs_ip, help="the IP address of the nfs datastore [%default]")
    parser.add_option("--nfs_path", type="string", dest="nfs_path", default=sfdefaults.kvm_nfs_path, help="the path you want to mount from the nfs datastore [%default]")
    parser.add_option("--mount_point", type="string", dest="mount_point", default=sfdefaults.nfs_mount_point, help="the location you want to mount the nfs datasore on the client, ex: /mnt/nfs [%default]")
    #vm info
    parser.add_option("--qcow2_name", type="string", dest="qcow2_name", default=sfdefaults.kvm_qcow2_name, help="name of the qcow2 image [%default]")
    parser.add_option("--cpu_count", type="int", dest="cpu_count", default=sfdefaults.kvm_cpu_count, help="The number of virtural CPUs the VM should have [%default]")
    parser.add_option("--memory_size", type="int", dest="memory_size", default=sfdefaults.kvm_mem_size, help="The size of memory in MB for the vm, [%default]")
    parser.add_option("--os_type", type="string", dest="os_type", default=sfdefaults.kvm_os, help="The OS type of the VM [%default]")
    #vm cloning info
    parser.add_option("--template_name", type="string", dest="template_name", default=None, help="the name of the VM to import and then clone from")
    #debug
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.nfs_ip, options.nfs_path, options.mount_point, options.qcow2_name, options.template_name, options.debug):
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




