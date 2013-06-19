"""
This script will
    1. Clone a template qcow2 image to a raw disk
    2. Create and import the raw volume

    When run as a script, the following options/env variables apply:
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
import time
import re
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
import lib.libclient as libclient
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import libvirt
try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree


class KvmCloneQcow2ToRawVmAction(ActionBase):
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
                            "hostPass" : None,
                            "vmName" : None,
                            "cpuCount" : libsf.IsInteger,
                            "memorySize" : libsf.IsInteger
                            },
            args)



    def kvmCreateXML(self, cpuNumber, memorySize, vmName, rawPath):
        """
        Grabs template xml file for kvm and addes the given values
        used to create a new VM
        """
        #convert MB to B
        memorySize *= 1024

        try:
            vm_xml = ElementTree.parse("kvm_template_xml.xml")
        except IOError as e:
            mylog.error("Could not open template XML file. Message: " + str(e))
            return False

        vm_xml.find("devices/disk/source").set("dev", rawPath)
        vm_xml.find("memory").text = str(memorySize)
        vm_xml.find("currentMemory").text = str(memorySize)
        vm_xml.find("vcpu").text = str(cpuNumber)
        vm_xml.find("name").text = vmName
        return vm_xml.getroot()

    def getIscsiPath(self, hypervisor):
        """
        Gets the path to the iscsi volume if none is provided
        """

        retcode, stdout, stderr = hypervisor.ExecuteCommand("ls /dev/disk/by-path/ | grep ip")
        if retcode != 0:
            mylog.error("Could not find the path to the iscsi volume")
            return False
        else:
            stdout = stdout.split("\n")
            stdout.remove("")
            loc = stdout[-1]
            for line in stdout:
                temp = line
                if "part" in temp:
                    loc = temp
        return "/dev/disk/by-path/" + loc


    def Execute(self, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, qcow2Path="/mnt/nfs/kvm-ubuntu-gold.qcow2", rawPath=None, vmName="kvm-ubuntu-gold-template", cpuCount=1, memorySize=512, osType="linux", debug=False):

        self.ValidateArgs(locals())

        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #connect to clientIP
        hypervisor = libclient.SfClient()

        try:
            hypervisor.Connect(vmHost, hostUser, hostPass)
            mylog.info("The connection to the hypervisor has been established")
        except libclient.ClientError as e:
            mylog.error("There was an error connecting to the hypervisor. Message: " + str(e))
            return False

        #if no raw path is provided then find the correct one
        if rawPath is None:
            mylog.info("No raw path provided. Attemping to find the correct path")
            rawPath = self.getIscsiPath(hypervisor)
            if rawPath == False:
                mylog.error("There was an error finding the iscsi volume")
                return False
            mylog.info("Using raw_path=" + rawPath)

        # convert qcow2 to raw - nfs to iscsi
        # use provided path
        mylog.step("Converting the qcow2 image to a raw image on volume")
        retcode, stdout, stderr = hypervisor.ExecuteCommand("qemu-img convert -O raw " + qcow2Path + " " + rawPath)
        if retcode == 0:
            mylog.info("The qcow2 image has been converted to a raw image on " + rawPath)
        else:
            mylog.error("There was an error converting the qcow2 image to a raw image. Error message: " + stderr)
            return False

        #connect to libvirt hypervisor
        try:
            conn = libvirt.open("qemu+ssh://" + vmHost + "/system")
        except libvirt.libvirtError as e:
            mylog.error("Unable to connect to libvirt. Message: " + str(e))
            return False

        if conn is None:
            mylog.error("Failed to connect")
            return False

        #create the XML to import the VM
        vm_xml = self.kvmCreateXML(cpuCount, memorySize, vmName, rawPath)

        #import VM
        try:
            newvm = conn.defineXML(ElementTree.tostring(vm_xml))
        except libvirt.libvirtError as e:
            mylog.error("Unable to create a new VM. Message: " + str(e))
            return False

        mylog.passed("The VM has been Imported")

        mylog.step("Trying to power on the VM")

        try:
            newvm.create()
        except libvirt.libvirtError as e:
            mylog.error("There was and error trying to power on the VM. Message: " + str(e))
            return False

        mylog.step("Waiting 1 minute for the VM to boot")
        time.sleep(60)

        [state, maxmem, mem, ncpu, cputime] = newvm.info()
        if state == libvirt.VIR_DOMAIN_RUNNING:
            mylog.info("The VM has powered on successfully")

        elif state == libvirt.VIR_DOMAIN_SHUTOFF:
            mylog.error("The VM has not powered on successfully")
            return False

        mylog.step("Now powering off the VM")

        try:
            newvm.destroy()
            mylog.info("The VM is now powered off")
        except libvirt.libvirtError as e:
            mylog.error("There was an error powering off the VM. Message: " + str(e))
            return False

        mylog.passed("The template VM is good to go")
        return True



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to clone")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--qcow2_path", type="string", dest="qcow2_path", default=None, help="The path to the qcow2 file you want to clone")
    parser.add_option("--raw_path", type="string", dest="raw_path", default=None, help="The path to the raw volume. EX: /dev/disk/by-path/....")
    parser.add_option("--cpu_count", type="int", dest="cpu_count", default=1, help="The number of virtural CPUs the VM should have")
    parser.add_option("--memory_size", type="int", dest="memory_size", default=512, help="The size of memory in MB for the vm, default 512MB")
    parser.add_option("--os_type", type="string", dest="os_type", default=None, help="The OS type of the VM")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.qcow2_path, options.raw_path, options.vm_name, options.cpu_count, options.memory_size, options.os_type, options.debug):
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
