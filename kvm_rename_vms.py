#!/usr/bin/python

"""
This action will rename VM hostnames to match VM names

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --client_user       The username for the client VMs
    SFCLIENT_USER env var

    --client_pass       The password for the client VMs
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import logging
from xml.etree import ElementTree
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
from clientmon.libclientmon import ClientMon
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmRenameVmsAction(ActionBase):
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

    def Execute(self, vmhost=sfdefaults.vmhost_kvm, connection=sfdefaults.kvm_connection, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Power on VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of vm info from the monitor
        monitor = ClientMon()
        vm_list = monitor.ListClientStatusByGroup("KVM")

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

        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        updated = 0
        for vm in running_vm_list:
            mylog.info("Updating hostname on " + vm.name())
            # Find the VM alphabetically first MAC address from the XML config
            vm_xml = ElementTree.fromstring(vm.XMLDesc(0))
            mac_list = []
            for node in vm_xml.findall("devices/interface/mac"):
                mac_list.append(node.get("address"))
            mac_list.sort()
            mac = mac_list[0]

            # Get the IP of this VM from the monitor info
            ip = ""
            for vm_info in vm_list:
                if vm_info.MacAddress == mac.replace(":", ""):
                    ip = vm_info.IpAddress
                    break
            if not ip:
                mylog.warning("Could not find IP for " + vm.name())
                continue

            client = SfClient()
            #mylog.info("Connecting to client '" +ip + "'")
            try:
                client.Connect(ip, client_user, client_pass)
            except ClientError as e:
                mylog.error(e)
                continue

            if (client.Hostname == vm.name()):
                mylog.passed("  Hostname is correct")
                updated += 1
                continue

            try:
                client.UpdateHostname(vm.name())
            except ClientError as e:
                mylog.error(e.message)
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            mylog.passed("  Successfully set hostname")
            updated += 1

        if updated == len(vm_ids):
            mylog.passed("Successfully updated hostname on all running VMs")
            return True
        else:
            mylog.error("Could not update hostname on all running VMs")
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
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the VM clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the VM clients [%default]")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.connection, options.host_user, options.host_pass, options.client_user, options.client_pass, options.debug):
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

