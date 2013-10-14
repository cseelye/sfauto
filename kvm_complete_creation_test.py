"""
This script will
    1. Import a template kvm image
    2. Clone template VM to each volume
    3. Boot all VMs

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

"""


import sys
from optparse import OptionParser
import logging
import re
import time
import platform
import libvirt
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError
from xml.etree import ElementTree
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import kvm_clone_qcow2_to_raw_vm
import kvm_sfclone_vm
import clusterbscheck

class KvmCompleteCreationTestAction(ActionBase):
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
                            "cloneCount" : libsf.IsInteger},
            args)

    def Execute(self, mvip=sfdefaults.mvip, vmHost=sfdefaults.vmhost_kvm, cloneCount=30, cloneName=None, sourceName=None, cpuCount=1, memorySize=512, osType="linux", qcow2Path="/mnt/nfs/kvm-ubuntu-gold.qcow2", rawPath=None, username=sfdefaults.username, password=sfdefaults.password, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.step("Cloning source image to raw image and importing VM")
        if (kvm_clone_qcow2_to_raw_vm.Execute(vmHost=vmHost, hostUser=hostUser, hostPass=hostPass, qcow2Path=qcow2Path, rawPath=rawPath, vmName=sourceName, cpuCount=1, memorySize=512, osType="linux") == False):
            mylog.error("Unable to clone the source image to raw")
            return False

        mylog.step("Attempting to make " + str(cloneCount) + " clones of the template VM")
        for i in xrange(0, cloneCount):
            updated_clone_name = cloneName + "-" + str(i).zfill(5)

            mylog.banner("Cloning VM " + str(i + 1) + " of " + str(cloneCount))
            if (kvm_sfclone_vm.Execute(vm_name=sourceName, connection="tcp", clone_name=updated_clone_name, mvip=mvip, username=username, password=password, vmhost=vmHost, host_user=hostUser, host_pass=hostPass) == False):
                mylog.error("There was an error creating clone number: " + str(i))
                return False

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
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--qcow2_path", type="string", dest="qcow2_path", default="/mnt/nfs/kvm-ubuntu-gold.qcow2", help="The path to the qcow2 file you want to clone")
    parser.add_option("--raw_path", type="string", dest="raw_path", default=None, help="The path to the raw volume. EX: /dev/disk/by-path/....")
    parser.add_option("--cpu_count", type="int", dest="cpu_count", default=1, help="The number of virtural CPUs the VM should have")
    parser.add_option("--memory_size", type="int", dest="memory_size", default=512, help="The size of memory in MB for the vm, default 512MB")
    parser.add_option("--os_type", type="string", dest="os_type", default="linux", help="The OS type of the VM")
    parser.add_option("--template_name", type="string", dest="source_name", default=None, help="the name of the VM to import and then clone from")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=None, help="the name of the cloned VMs")
    parser.add_option("--vm_count", type="int", dest="clone_count", default=None, help="the number of VMs to make")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.vmhost, options.clone_count, options.clone_name, options.source_name, options.cpu_count, options.memory_size, options.os_type, options.qcow2_path, options.raw_path, options.username, options.password, options.host_user, options.host_pass, options.debug):
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


