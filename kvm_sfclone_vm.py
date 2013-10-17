#!/usr/bin/python

"""
This action will clone the volume a VM is on and import the new VM to the hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --vm_name           The name of the VM to clone

    --clone_name        THe name to give the clone
"""

import sys
from optparse import OptionParser
import logging
import re
import time
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import SfClient, ClientError
from xml.etree import ElementTree
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmSfcloneVmAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
			    "host_user" : None,
                            "host_pass" : None,
                            "vm_name" : None,
                            "clone_name" : None},
            args)
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")

    def Execute(self, vm_name, connection=sfdefaults.kvm_connection, clone_name=None, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Clone a VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Connecting to " + vmhost)
        try:
            if connection == "ssh":
                conn = libvirt.open("qemu+ssh://" + vmhost + "/system")
            elif connection == "tcp":
                conn = libvirt.open("qemu+tcp://" + vmhost + "/system")
            else:
                mylog.error("There was an error connecting to libvirt on " + vmHost + " wrong connection type: " + connection)
                return False
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if conn == None:
            mylog.error("Failed to connect")
            self.RaiseFailureEvent(message="Failed to connect")
            return False

        try:
            hypervisor = SfClient()
            hypervisor.Connect(vmhost, host_user, host_pass)
        except ClientError as e:
            mylog.error("Failed to connect - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("Searching for source VM " + vm_name)
        source_vm = None

        # Get a list of running VMs
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in running_vm_list:
            if vm.name() == vm_name:
                source_vm = vm
                break

        if not source_vm:
            # Get a list of stopped VMs
            try:
                vm_ids = conn.listDefinedDomains()
                stopped_vm_list = map(conn.lookupByName, vm_ids)
                stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
            except libvirt.libvirtError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for vm in stopped_vm_list:
                if vm.name() == vm_name:
                    source_vm = vm
                    break

        if not source_vm:
            mylog.error("Could not find VM " + vm_name)
            sys.exit(1)

        # find what disk the VM is using
        disk_list = []
        vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
        for node in vm_xml.findall("devices/disk/source"):
            disk_list.append(node.get("dev"))
        if len(disk_list) > 1:
            mylog.error("Sorry, this is only implemented for VMs with a single disk")
            sys.exit(1)

        mylog.info("Determining source volume")
        source_volume_iqn = None
        disk_path = disk_list[0]

        m = re.search(r"/dev/sd(\w+)", disk_path)
        if m:
            device = m.group(1)
            # Find the path to the volume
            return_code, stdout, stderr = hypervisor.ExecuteCommand("iscsiadm -m session -P3 | egrep 'Target|scsi disk'")
            iqn = None
            for line in stdout.split("\n"):
                m = re.search(r"Target: (\S+)", line)
                if m:
                    iqn = m.group(1)
                m = re.search(r"Attached scsi disk (\S+)", line)
                if m and m.group(1) == device:
                    source_volume_iqn = iqn
                    break
        else:
            m = re.search(r"/dev/disk/by-path/.+(iqn.+solidfire.+)-lun-0", disk_path)
            if m:
                source_volume_iqn = m.group(1)

        mylog.info("Source IQN: " + str(source_volume_iqn))
        if not source_volume_iqn:
            mylog.error("Could not determine source volume for VM " + vm_name)
        mylog.debug("Source volume has IQN " + source_volume_iqn)

        # Find the volume ID of the source
        source_volume_id = None
        source_volume_name = None
        result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        for vol in result["volumes"]:
            if vol["iqn"] == source_volume_iqn:
                source_volume_id = vol["volumeID"]
                source_volume_name = vol["name"]
                break
        if not source_volume_id:
            mylog.error("Could not find volume with IQN " + source_volume_iqn)
            self.RaiseFailureEvent(message="Could not find volume with IQN " + source_volume_iqn)
            return False
        mylog.debug("Source volume ID is " + source_volume_name + " (" + str(source_volume_id) + ")")

        # Clone the source volume
        mylog.info("Making a SolidFire clone of volume '" + source_volume_name + "' to volume '" + clone_name + "'")
        params = {}
        params["volumeID"] = source_volume_id
        params["name"] = clone_name
        params["access"] = "readWrite"

        try:
            result = libsf.CallApiMethod(mvip, username, password, "CloneVolume", params)
        except libsf.SfApiError as e:
            if e.name == "xInvalidAPIParameter":
                mylog.error("Invalid arguments - \n" + str(e))
                return False
            raise

        # Wait for the clone to complete
        clone_volume_id = result["volumeID"]
        params = {}
        params["asyncHandle"] = result["asyncHandle"]
        while True:
            result = libsf.CallApiMethod(mvip, username, password, "GetAsyncResult", params)
            if result["status"].lower() == "complete":
                if "result" in result:
                    #mylog.info("Clone " + clone_name + " finished")
                    break
                else:
                    mylog.error("  Clone " + clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                    self.RaiseFailureEvent(message="Clone " + clone_name + " failed -- " + result["error"]["name"] + ": " + result["error"]["message"])
                    return False
            else:
                time.sleep(10)

        # Get the IQN of the clone
        result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        clone_iqn = None
        for vol in result["volumes"]:
            if vol["volumeID"] == clone_volume_id:
                clone_iqn = vol["iqn"]
                break
        if clone_iqn == None:
            mylog.error("Could not find volume '" + str(clone_volume_id) + "'")
            self.RaiseFailureEvent(message="Could not find volume '" + str(clone_volume_id) + "'")
            return False

        # Get the SVIP of the cluster
        result = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        svip = result["clusterInfo"]["svip"]

        # Log in to the clone
        mylog.info("Loging in to clone volume")
        try:
            hypervisor.RefreshTargets(svip)
            hypervisor.LoginTargets(svip, pTargetList=[clone_iqn])
        except ClientError as e:
            mylog.error("Failed to log in to volume - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Get the disk device of the clone volume
        clone_device_path = None
        while not clone_device_path:
            try:
                return_code, stdout, stderr = hypervisor.ExecuteCommand("find /dev/disk/by-path/ -name '*" + clone_iqn + "*' | sort | head -1")
            except ClientError as e:
                mylog.error("Could not get device path for clone - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            clone_device_path = stdout.strip()
            if not clone_device_path:
                time.sleep(5)

        #clone_device_path += "-part1"

        mylog.info("Importing the new VM")
        # Create the XML for the new VM
        clone_vm_xml = vm_xml
        # Remove unique IDs like KVM UUID and interface MAC addresses
        interface_root = clone_vm_xml.find("devices/interface")
        mac_list = clone_vm_xml.findall("devices/interface/mac")
        for mac_element in mac_list:
            interface_root.remove(mac_element)
        clone_vm_xml.remove(clone_vm_xml.find("uuid"))
        # Set the disk path and name
        clone_vm_xml.find("devices/disk/source").set("dev", clone_device_path)
        clone_vm_xml.find("name").text = clone_name

        # Create the new VM on the hypervisor
        newvm = None
        try:
            newvm = conn.defineXML(ElementTree.tostring(clone_vm_xml))
        except libvirt.libvirtError as e:
            mylog.error("Could not create new VM - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if not newvm:
            mylog.error("Could not create new VM")
            self.RaiseFailureEvent(message="Could not create new VM")
            return False

        mylog.info("Powering on the new VM")
        try:
            newvm.create()
        except libvirt.libvirtError as e:
            mylog.error("Could not power on VM - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully cloned the VM")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to clone")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=None, help="the name of the clone to create")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.connection, options.clone_name, options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.debug):
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

