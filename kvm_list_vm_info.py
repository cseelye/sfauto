#!/usr/bin/python

"""
This action will show info about the VMs on a KVM hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
from xml.etree import ElementTree
import logging
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
import lib.libsf as libsf
from lib.libsf import mylog
from clientmon.libclientmon import ClientMon
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmListVmInfoAction(ActionBase):
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


    def Execute(self, vmhost=sfdefaults.vmhost_kvm, connection=sfdefaults.kvm_connection, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List VM info
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        # Get a list of vm info from the monitor
        monitor = ClientMon()
        monitor_list = monitor.ListClientStatusByGroup("KVM")

        #for c in monitor_list:
        #    print c.MacAddress + " => " + c.IpAddress
        #exit(0)

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

        # Get a list of VMs
        vm_list = []
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            vm_list += running_vm_list
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        try:
            vm_ids = conn.listDefinedDomains()
            stopped_vm_list = map(conn.lookupByName, vm_ids)
            vm_list += stopped_vm_list
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        vm_list = sorted(vm_list, key=lambda vm: vm.name())

        for vm in vm_list:
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

            # Get the state of this VM
            [state, maxmem, mem, ncpu, cputime] = vm.info()
            if state == libvirt.VIR_DOMAIN_RUNNING:
                state_str = "Running"
            else:
                state_str = "Not running"

            if bash or csv:
                separator = ","
                if bash:
                    separator = " "
                sys.stdout.write(vm.name() + separator + mac + separator + ip + separator + state_str + "\n")
                sys.stdout.flush()
            else:
                mylog.info("  " + vm.name() + " - " + mac + " - " + ip + " - " + state_str)

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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.connection, options.csv, options.bash, options.host_user, options.host_pass, options.debug):
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

