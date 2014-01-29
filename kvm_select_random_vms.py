"""
This script will connect to a hypervisor and return a random selection of VMs

input: vm_name, vm_regex, vm_count

"""

import sys
from optparse import OptionParser
import logging
import random
import re
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


class KvmSelectRandomVmsAction(ActionBase):
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
                            "host_pass" : None,
                            "vm_count" : libsf.IsInteger
                            },
            args)
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")



    def Get(self, vm_name=None, connection=sfdefaults.kvm_connection, vm_regex=None, vm_count=1, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, csv=False, bash=False, debug=False):
        """
        Select Random VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

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


        matched_vms = []
        # Get a list of running VMs
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            #self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in running_vm_list:
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)


        # Get a list of stopped VMs
        try:
            vm_ids = conn.listDefinedDomains()
            stopped_vm_list = map(conn.lookupByName, vm_ids)
            stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            #self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in stopped_vm_list:
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)

        mylog.info("Getting a random list of " + str(vm_count) + " VMs on hypervisor")
        #Return a list of VMs
        matched_vms = sorted(matched_vms, key=lambda vm: vm.name())

        random_vms = []
        #make sure there is at least one VM
        if len(matched_vms) == 0:
            mylog.error("There are no VMs on this hypervisor")
            return False

        #if you request more than the actual number of VMs return them all
        elif vm_count >= len(matched_vms):
            mylog.warning("Requested more or equal to the number of VMs on this hypervisor. Returning all VMs")
            return matched_vms

        #get the random VMs
        else:
            for x in xrange(0, vm_count):
                random_index = random.randint(0, len(matched_vms) - 1)
                random_vms.append(matched_vms.pop(random_index).name())
            return random_vms
"""
This script will connect to a hypervisor and return a random selection of VMs

input: vm_name, vm_regex, vm_count

"""

import sys
from optparse import OptionParser
import logging
import random
import re
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


class KvmSelectRandomVmsAction(ActionBase):
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
                            "host_pass" : None,
                            "vm_count" : libsf.IsInteger
                            },
            args)
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")



    def Get(self, vm_name=None, connection=sfdefaults.kvm_connection, vm_regex=None, vm_count=1, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, csv=False, bash=False, debug=False):
        """
        Select Random VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

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


        matched_vms = []
        # Get a list of running VMs
        try:
            vm_ids = conn.listDomainsID()
            running_vm_list = map(conn.lookupByID, vm_ids)
            running_vm_list = sorted(running_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            #self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in running_vm_list:
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)


        # Get a list of stopped VMs
        try:
            vm_ids = conn.listDefinedDomains()
            stopped_vm_list = map(conn.lookupByName, vm_ids)
            stopped_vm_list = sorted(stopped_vm_list, key=lambda vm: vm.name())
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            #self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        for vm in stopped_vm_list:
            if vm_regex:
                m = re.search(vm_regex, vm.name())
                if m:
                    matched_vms.append(vm)
            else:
                matched_vms.append(vm)

        mylog.info("Getting a random list of " + str(vm_count) + " VMs on hypervisor")
        #Return a list of VMs
        matched_vms = sorted(matched_vms, key=lambda vm: vm.name())

        random_vms = []
        #make sure there is at least one VM
        if len(matched_vms) == 0:
            mylog.error("There are no VMs on this hypervisor")
            return False

        #if you request more than the actual number of VMs return them all
        elif vm_count >= len(matched_vms):
            mylog.warning("Requested more or equal to the number of VMs on this hypervisor. Returning all VMs")
            return matched_vms

        #get the random VMs
        else:
            for x in xrange(0, vm_count):
                random_index = random.randint(0, len(matched_vms) - 1)
                random_vms.append(matched_vms.pop(random_index).name())
            return random_vms


    def Execute(self, vm_name=None, vm_regex=None, vm_count=1, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, csv=False, bash=False, debug=False):
        """
        Show the random list of VMs 
        """
        del self
        vm_list = Get(**locals())

        if vm_list is False:
            mylog.error("Unable to get list of VMs on hypervisor")
            return False

        if csv or bash:
            separator = ","
            if bash:
                separator = " "
            vm_names = ""
            for vm in vm_list:
                vm_names += vm + separator
            sys.stdout.write(vm_names[:-1] + "\n")
            sys.stdout.flush()

        else:
            mylog.info(str(len(vm_list)) + " random VMs from " + vmhost)
            for vm in vm_list:
                mylog.info("VM name: " + vm.name())
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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power on")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=1, help="the number of VMs to return")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.csv, options.bash, options.debug):
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



    def Execute(self, vm_name=None, vm_regex=None, vm_count=1, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, csv=False, bash=False, debug=False):
        """
        Show the random list of VMs 
        """
        del self
        vm_list = Get(**locals())

        if vm_list is False:
            mylog.error("Unable to get list of VMs on hypervisor")
            return False

        if csv or bash:
            separator = ","
            if bash:
                separator = " "
            vm_names = ""
            for vm in vm_list:
                vm_names += vm + separator
            sys.stdout.write(vm_names[:-1] + "\n")
            sys.stdout.flush()

        else:
            mylog.info(str(len(vm_list)) + " random VMs from " + vmhost)
            for vm in vm_list:
                mylog.info("VM name: " + vm)
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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to power on")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power on")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=1, help="the number of VMs to return")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.vm_regex, options.vm_count, options.vmhost, options.host_user, options.host_pass, options.csv, options.bash, options.debug):
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


