#!/usr/bin/python

"""
This action will show the VMs on a KVM hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import logging
import platform
import re
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class KvmListVmNamesAction(ActionBase):
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


    def Get(self, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, connection=sfdefaults.kvm_connection, vm_regex=None, debug=False):
        """
        List VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #mylog.info("Connecting to " + vmhost)
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
        if vm_regex is None:
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
            vm_names = map(lambda vm: vm.name(), vm_list)

        else:
            try:
                vm_ids = conn.listDomainsID()
                running_vm_list = map(conn.lookupByID, vm_ids)
                for vm in running_vm_list:
                    m = re.search(vm_regex, vm.name())
                    if m:
                        vm_list.append(vm)

            except libvirt.libvirtError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            try:
                vm_ids = conn.listDefinedDomains()
                stopped_vm_list = map(conn.lookupByName, vm_ids)
                for vm in stopped_vm_list:
                    m = re.search(vm_regex, vm.name())
                    if m:
                        vm_list.append(vm)

            except libvirt.libvirtError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            vm_list = sorted(vm_list, key=lambda vm: vm.name())
            vm_names = map(lambda vm: vm.name(), vm_list)


        return vm_names

    def Execute(self, vmhost=sfdefaults.vmhost_kvm, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, connection=sfdefaults.kvm_connection, vm_regex=None, debug=False):
        """
        List VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        mylog.info("Connecting to " + vmhost)
        vm_names = self.Get(vmhost, host_user, host_pass, connection, vm_regex, debug)
        if vm_names == False:
            mylog.error("There was an error getting the list of VMs")
            return False

        if csv or bash:
            separator = ","
            if bash:
                separator = " "
            sys.stdout.write(separator.join(vm_names) + "\n")
            sys.stdout.flush()
        else:
            for name in vm_names:
                mylog.info("  " + name)

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
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power off")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.csv, options.bash, options.host_user, options.host_pass, options.connection, options.vm_regex, options.debug):
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

