#!/usr/bin/python

"""
This action will show VMs and their MAC addresses on a KVM hypervisor

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
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
from clientmon.libclientmon import ClientMon
import kvm_list_vm_macs


class KvmListVmIpsAction(ActionBase):
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

    def Get(self, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, connection=sfdefaults.kvm_connection, debug=False):
        monitor = ClientMon()
        monitor_list = monitor.ListClientStatusByGroup("KVM")

        mac_ip_table = []

        for vm in monitor_list:
            temp = (vm.MacAddress, vm.IpAddress)
            mac_ip_table.append(temp)

        mac_list = kvm_list_vm_macs.Get(vmhost=vmhost, connection="tcp")

        if mac_list == False:
            mylog.error("Could not get a list of VMs from KVM host")
            return False


        ip_list = []
        
        for i in mac_ip_table:
            for mac in mac_list:
                if i[0] == mac.replace(":", ""):
                    ip_list.append(i[1])

        if len(ip_list) == 0:
            mylog.error("None of the VMs are being tracked by ClientMonitor Unable to get IP address")
            return False
        if len(ip_list) < len(mac_list):
            mylog.warning("There are " + str(len(mac_list) - len(ip_list)) + " VM(s) that are not being tracked by ClientMonitor")

        ip_list.sort()
        return ip_list


    def Execute(self, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, connection=sfdefaults.kvm_connection, debug=False):
        ip_list = self.Get(vmhost, host_user, host_pass, connection, debug)
        if ip_list == False:
            mylog.error("Could not get a list of IPs for " + vmhost)
            return False

        for ip in ip_list:
            mylog.info(ip + ": is on " + vmhost)
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
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.connection, options.debug):
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



