#!/usr/bin/python

# This script will clone the volume a VM is on and import the new VM to the host

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "solidfire"          # Admin password for the cluster
                                # --pass

vmhost = "172.25.106.000"       # The IP address of the hypervisor
                                # --host_ip

host_user = "root"              # The username for the hypervisor
                                # --client_user

<<<<<<< HEAD
host_pass = "password"           # The password for the hypervisor
=======
host_pass = "solidfire"         # The password for the hypervisor
>>>>>>> 6611be4... KVM scripts - standardize command line args, add a few new ones
                                # --client_pass

vm_name = ""                    # The name of the VM to clone
                                # --vm_name

clone_name = ""                 # The name to give the clone
                                # --clone_name

# ----------------------------------------------------------------------------

import sys, os
from optparse import OptionParser
import json
import time
import re
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
import libsf
from libsf import mylog
import libclient
from libclient import SfClient, ClientError
from xml.etree import ElementTree


def main():
    global mvip, username, password, vmhost, host_user, host_pass, vm_name, clone_name

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--vmhost", type="string", dest="vmhost", default=vmhost, help="the management IP of the hypervisor")
    parser.add_option("--host_user", type="string", dest="host_user", default=host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=vm_name, help="the name of the VM to clone")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=clone_name, help="the name to give to the clone")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    vmhost = options.vmhost
    host_user = options.host_user
    host_pass = options.host_pass
    vm_name = options.vm_name
    clone_name = options.clone_name
    if options.debug:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(vmhost):
        mylog.error("'" + vmhost + "' does not appear to be a valid host IP")
        sys.exit(1)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + vmhost + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Connecting to " + vmhost)
    try:
        conn = libvirt.open("qemu+tcp://" + vmhost + "/system")
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
    if conn == None:
        mylog.error("Failed to connect")
        sys.exit(1)

    try:
        client = SfClient()
        client.Connect(vmhost, host_user, host_pass)
    except ClientError as e:
        mylog.error("Failed to connect - " + str(e))
        sys.exit(1)

    mylog.info("Searching for source VM " + vm_name)
    source_vm = None

    # Get a list of running VMs
    try:
        vm_ids = conn.listDomainsID()
        running_vm_list = map(conn.lookupByID, vm_ids)
        running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
    except libvirt.libvirtError as e:
        mylog.error(str(e))
        sys.exit(1)
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
            sys.exit(1)
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
    m = re.search("/dev/sd(\w+)", disk_path)
    if m:
        device = m.group(1)
        # Find the path to the volume
        return_code, stdout, stderr = client.ExecuteCommand("iscsiadm -m session -P3 | egrep 'Target|scsi disk'")
        iqn = None
        for line in stdout.split("\n"):
            m = re.search("Target: (\S+)", line)
            if m:
                iqn = m.group(1)
            m = re.search("Attached scsi disk (\S+)", line)
            if m and m.group(1) == device:
                source_volume_iqn = iqn
                break
    else:
        m = re.search("/dev/disk/by-path/.+(iqn.+solidfire.+)-lun-0", disk_path)
        if m:
            source_volume_iqn = m.group(1)

    if not source_volume_iqn:
        mylog.error("Could not determine source volume for VM " + vm_name)
    mylog.debug("Source volume has IQN " + source_volume_iqn)

    # Find the volume ID/account ID of the source
    source_volume_id = None
    source_volume_name = None
    account_id = None
    result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
    for vol in result["volumes"]:
        if vol["iqn"] == source_volume_iqn:
            source_volume_id = vol["volumeID"]
            source_volume_name = vol["name"]
            account_id = vol["accountID"]
            break
    if not source_volume_id:
        mylog.error("Could not find volume with IQN " + source_volume_iqn)
        sys.exit(1)
    mylog.debug("Source volume ID is " + source_volume_name + " (" + str(source_volume_id) + ")")

    # Clone the source volume
    mylog.info("Making a SolidFire clone of volume '" + source_volume_name + "' to volume '" + clone_name + "'")
    params = {}
    params["volumeID"] = source_volume_id
    params["name"] = clone_name
    params["access"] = "readWrite"
    result = libsf.CallApiMethod(mvip, username, password, "CloneVolume", params)

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
                sys.exit(1)
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
        mylog.error("Could not find volume '" + volume_name + "'")
        sys.exit(1)

    # Get the SVIP of the cluster
    result = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
    svip = result["clusterInfo"]["svip"]

    # Log in to the clone
    mylog.info("Loging in to clone volume")
    try:
        client.RefreshTargets(svip)
        client.LoginTargets(svip, pTargetList=[clone_iqn])
    except ClientError as e:
        mylog.error("Failed to log in to volume - " + str(e))
        sys.exit(1)

    # Get the disk device of the clone volume
    clone_device_path = None
    while not clone_device_path:
        try:
            return_code, stdout, stderr = client.ExecuteCommand("find /dev/disk/by-path/ -name '*" + clone_iqn + "*' | sort | head -1")
        except ClientError as e:
            mylog.error("Could not get device path for clone - " + str(e))
            sys.exit(1)
        clone_device_path = stdout.strip()
        if not clone_device_path:
            time.sleep(5)

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
    newvw = None
    try:
        newvm = conn.defineXML(ElementTree.tostring(clone_vm_xml))
    except libvirt.libvirtError as e:
        mylog.error("Could not create new VM - " + str(e))
        sys.exit(1)
    if not newvm:
        mylog.error("Could not create new VM")
        sys.exit(1)

    mylog.info("Powering on the new VM")
    try:
        newvm.create()
    except libvirt.libvirtError as e:
        mylog.error("Could not power on VM - " + str(e))
        sys.exit(1)

    mylog.passed("Successfully cloned the VM")




if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
