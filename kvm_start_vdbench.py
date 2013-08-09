#!/usr/bin/python

"""
This action will start vdbench on VMs on a KVM hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_name           The name of the VM to start vdbench on

    --vm_regex          Regex to match to select VMs to start vdbench on

    --vm_count          The number of matching VMs to start vdbench on

    --client_user       The username for the client VMs
    SFCLIENT_USER env var

    --client_pass       The password for the client VMs
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import logging
import re
from xml.etree import ElementTree
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
from clientmon.libclientmon import ClientMon
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmStartVdbenchAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None},
            args)
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")

    def Execute(self, vm_name=None, connection=sfdefaults.kvm_connection, vm_regex=None, vm_count=0, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Power on VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of vm info from the monitor
        monitor = ClientMon()
        monitor_list = monitor.ListClientStatusByGroup("KVM")

        mylog.info("Connecting to " + vmhost)
        try:
            if connection == "ssh":
                conn = libvirt.openReadOnly("qemu+ssh://" + vmhost + "/system")
            elif connection == "tcp":
                conn = libvirt.openReadOnly("qemu+tcp://" + vmhost + "/system")
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

        mylog.info("Searching for matching VMs")
        matched_vms = []

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
            if vm_name and vm.name() == vm_name:
                matched_vms.append(vm)
                break
            if vm_count > 0 and len(matched_vms) >= vm_count:
                break
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)

        if len(matched_vms) <= 0:
            mylog.warning("Could not find any VMs that match")
            return True

        vdbench_count = 0
        matched_vms = sorted(matched_vms, key=lambda vm: vm.name())
        for vm in matched_vms:
            # Find the VM's alphabetically first MAC address from the XML config
            vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
            mac_list = []
            for node in vm_xml.findall("devices/interface/mac"):
                mac_list.append(node.get("address"))
            mac_list.sort()
            mac = mac_list[0]

            # Get the IP of this VM from the monitor info
            ip = ""
            for vm_info in monitor_list:
                if vm_info.MacAddress == mac.replace(":", ""):
                    ip = vm_info.IpAddress
                    break
            if not ip:
                mylog.error("Could not find IP address for " + vm.name())
                continue

            # Start vdbench
            mylog.info("  Starting vdbench on " + vm.name())
            ssh = libsf.ConnectSsh(ip, client_user, client_pass)
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "status vdbench; echo $?")
            stdout_data = stdout.readlines()
            if int(stdout_data.pop()) != 0:
                mylog.error("  Could not get vdbench status on " + vm.name())
                ssh.close()
                continue

            if "start" in stdout_data[0]:
                mylog.passed("  vdbench is already started on " + vm.name())
                ssh.close()
                vdbench_count += 1
                continue

            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "start vdbench; echo $?")
            if int(stdout.readlines().pop()) != 0:
                mylog.error("  Failed to start vdbench on " + vm.name())
                ssh.close()
                continue

            vdbench_count += 1
            mylog.passed("  Successfully started vdbench on " + vm.name())
            ssh.close()

        if vdbench_count == len(matched_vms):
            mylog.passed("vdbench started on all VMs")
            return True
        else:
            mylog.error("Could not start vdbench on all VMs")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to start vdbench on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to start vdbench on")
    parser.add_option("--vm_count", type="string", dest="vm_count", default=None, help="the number of matching VMs to start vdbench on")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the VM clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the VM clients [%default]")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.connection, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.client_user, options.client_pass, options.debug):
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

