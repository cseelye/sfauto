"""
This script takes in a list of VMs and waits till they are all booted
Once they are all booted it will return True


"""


import sys
import time
from optparse import OptionParser
try:
    import xml.etree.cElementTree as ElementTree
except ImportError:
    import xml.etree.ElementTree as ElementTree
import logging
import platform
import re
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
import lib.libsf as libsf
from lib.libsf import mylog
from clientmon.libclientmon import ClientMon
import lib.libclient as libclient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import kvm_list_vm_names

class KvmWaitForBootedAction(ActionBase):
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
                            "hostPass" : None},
            args)
        if args["connection"] != "ssh":
            if args["connection"] != "tcp":
                raise libsf.SfArgumentError("Connection type needs to be ssh or tcp")

    def checkVMstatusOn(self, conn, vmNames):
        """
        Takes in a vibvirt connection and a list of VM names
        returns True if all VMs are turned on 
        else returns a list of vm names that are not turned on
        """
        new_vm_names = []

        for name in vmNames:
            try:
                vm = conn.lookupByName(name)
            except libvirt.libvirtError as e:
                mylog.error(str(e))

            [state, maxmem, mem, ncpu, cputime] = vm.info()
            if state != libvirt.VIR_DOMAIN_RUNNING:
                new_vm_names.append(name)

        if len(new_vm_names) == 0:
            return True
        return new_vm_names


    def checkPingVMStatus(self, conn, vmHost, hostUser, hostPass, vmNames):
        """
        Makes sure the host is able to ping all of the VMs that are on it
        """
        #temp - do a simple ping test to each vm
        hypervisor = libclient.SfClient()

        try:
            hypervisor.Connect(vmHost, hostUser, hostPass)
            mylog.info("The connection to the hypervisor has been established")
        except libclient.ClientError as e:
            mylog.error("There was an error connecting to the hypervisor. Message: " + str(e))
            #return False

        #get the list of VM MAC and IP addresses
        retcode, stdout, stderr = hypervisor.ExecuteCommand("cat /var/lib/libvirt/dnsmasq/default.leases")

        ip_list = []
        mac_list = []
        mac_ip_list = []
        full_info = []

        #split the info
        stdout = stdout.split()
        for element in stdout:
            if libsf.IsValidIpv4Address(element):
                ip_list.append(element)
            if self.isValidMACAddress(element):
                mac_list.append(element)

        #make sure there are the same number of MAC and IP addresses
        if len(mac_list) == len(ip_list):
            for x in xrange(0, len(mac_list)):
                temp_info = mac_list[x], ip_list[x]
                mac_ip_list.append(temp_info)


        #match VM name with mac address
        for name in vmNames:
            try:
                vm = conn.lookupByName(name)
            except libvirt.libvirtError as e:
                mylog.error(str(e))
            xml = ElementTree.fromstring(vm.XMLDesc(0))

            for elem in xml.iterfind('devices/interface/mac'):
                for addr in mac_ip_list:
                    if elem.attrib.get('address') == addr[0]:
                        temp = addr[0], addr[1], name
                        full_info.append(temp)

        #temp - try to ping all the VMs 
        for ip in full_info:
            recode, stdout, stderr = hypervisor.ExecuteCommand("ping -n -i 0.2 -c 3 -W 1 -q " + ip[1])
            if recode == 0:
                mylog.info("Was able to ping " + ip[2])
            else:
                mylog.error("Was not able to ping " + ip[2])
                return False
        return True



    def isValidMACAddress(self, macAddress):
        """
        Checks to make sure something is a valid mac address
        """
        if re.match("[0-9a-f]{2}([-:])[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$", macAddress.lower()):
            return True
        return False


    def Execute(self, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, connection=sfdefaults.kvm_connection, vmNames=None, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)


        if vmNames is None:
            #get list of VM names
            mylog.info("No list of VM names provided. Trying to get a full list from hypervisor")
            vmNames = kvm_list_vm_names.Get(vmhost=vmHost, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False)

            if vmNames == False:
                mylog.error("Could not get a list of VMs from the hypervisor")
                return False

        #connect to VM host
        mylog.info("Connecting to " + vmHost)
        try:
            if connection == "ssh":
                conn = libvirt.open("qemu+ssh://" + vmHost + "/system")
            elif connection == "tcp":
                conn = libvirt.open("qemu+tcp://" + vmHost + "/system")
            else:
                mylog.error("There was an error connecting to libvirt on " + vmHost + " wrong connection type: " + connection)
                return False
        except libvirt.libvirtError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False


        all_good = False

        while not all_good:

            #get a list of VMs that are not currently on
            new_vm_names = self.checkVMstatusOn(conn, vmNames)

            #if all the VMs are on then return true
            if new_vm_names == True and self.checkPingVMStatus(conn, vmHost, hostUser, hostPass, vmNames) == True:
                mylog.passed("All the VMs are powered on")
                return True

            else:
                #if the VM is not running try to start it
                done = False
                while not done:
                    for name in new_vm_names:
                        try:
                            vm = conn.lookupByName(name)
                        except libvirt.libvirtErr as e:
                            mylog.error(str(e))

                        try:
                            mylog.info("VM " + vm.name() + " is not on. Trying to power on.")
                            vm.create()
                            mylog.info("VM " + vm.name() + " is now powered on.")
                        except libvirt.libvirtError as e:
                            mylog.error(str(e))
                            return False
                    new_vm_names = self.checkVMstatusOn(conn, new_vm_names)
                    if new_vm_names == True:
                        done = True

                mylog.step("Waiting for 60 seconds")
                time.sleep(60)

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_names", action="list", dest="vm_names", default=None, help="the names of the VMs to power on")
    parser.add_option("--connection", type="string", dest="connection", default=sfdefaults.kvm_connection, help="How to connect to vibvirt on vmhost. Options are: ssh or tcp")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.connection, options.vm_names, options.debug):
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
