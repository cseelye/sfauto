#!/usr/bin/python

"""
This action will return a xen hypervisor to a "normal" state
This inclues powering on VMs with a certain Regex
Powering down VMs with a certain Regex
Deleting all snapshots
Making sure each VM is on it's own SR


When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the hypervisor host

    --host_user         The host username
    SFHOST_USER env var

    --host_pass         The host password
    SFHOST_PASS env var

    --vm_name           The name of a single VM to power on

    --vm_on_regex       Regex to match names of VMs to power on

    --vm_off_regex      Regex to match names of VMs to power off

    -delete_regex       Regex to match names of VMs to delete

    -template_regex     Regex to match of VM templates

"""



import sys
from optparse import OptionParser
import logging
import re
import time
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import xen_delete_all_snapshots
import xen_list_vm_sr
import xen_list_vm_names
import xen_relocate_vm
import xen_delete_vms
import xen_poweroff_vms
import xen_poweron_vms



class XenReturnToNormalAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vm_on_regex" : None,
                            "vm_off_regex" : None,
                            "delete_regex" : None,
                            "vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None
                            },
                    args)

    def Execute(self, vmhost=sfdefaults.vmhost_xen, vm_on_regex=None, vm_off_regex=None, delete_regex=None, template_regex=None, host_user=sfdefaults.username, host_pass=sfdefaults.password, debug=False):

        self.ValidateArgs(locals())

        if debug:
            mylog.console.setLevel(logging.DEBUG)


        if xen_delete_all_snapshots.Execute(vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
            mylog.error("Could not delete all snapshots for VMs")


        vm_list = xen_list_vm_names.Get(vmhost=vmhost, host_user=host_user, host_pass=host_pass)
        if vm_list == False:
            mylog.error("Could not get a running list of VMs on " + vmhost)


        vms_to_delete = False
        for vm in vm_list:
            m = re.search(delete_regex, vm)
            if m:
                vms_to_delete = True
                break

        if vms_to_delete:
            if xen_poweroff_vms.Execute(vm_regex=delete_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
                mylog.error("Could not power down all the VMs we want to delete")

            if xen_delete_vms.Execute(vm_regex=delete_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
                mylog.error("Could not delete all the VMs we want to delete")

        vm_list = xen_list_vm_names.Get(vmhost=vmhost, host_user=host_user, host_pass=host_pass)
        if vm_list == False:
            mylog.error("Could not get a running list of VMs on " + vmhost)

        #the regex of the VMs not to touch: we want to keep the templates as is
        reserved_regex = ["template", "gold"]
        if template_regex != None:
            reserved_regex.append(template_regex)
        template_vm_list = []

        #search through the VMs found and grab the names of all the template VMs
        for reserved in reserved_regex:
            for vm in vm_list:
                m = re.search(reserved, vm)
                if m:
                    template_vm_list.append(vm)

        #delete the template VMs from the found VM list
        vm_list = list(set(vm_list) - set(template_vm_list))
        print vm_list

        for vm in vm_list:
            #turn off logging while we get the SR names
            mylog.silence = True
            vm_sr = xen_list_vm_sr.Get(vm_name=vm, vmhost=vmhost, host_user=host_user, host_pass=host_pass)
            #turn logging back on
            mylog.silence = False
            if vm_sr == False:
                mylog.error("could not get the SR for " + vm)
            else:
                vm_sr_temp = vm_sr.split('.')[:-1][0]
                mylog.info("VM: " + vm + " is on " + vm_sr_temp)
                if vm_sr_temp != vm:
                    mylog.info(vm + " is on the wrong SR. It is currently on " + vm_sr + " it needs to be on " + vm)
                    mylog.info("Trying to move the VM to the correct SR")
                    if xen_relocate_vm.Execute(vm_name=vm, dest_sr=vm, vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
                        mylog.error("The VM could not move to the SR " + vm)
                else:
                    mylog.info(vm + " is currently on the correct SR: " + vm_sr)



        if xen_poweron_vms.Execute(vm_regex=vm_on_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
            mylog.error("Could not power on the correct VMs")
            return False

        if xen_poweroff_vms.Execute(vm_regex=vm_off_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass) == False:
            mylog.error("Could not power off the correct VMs")
            return False

        mylog.passed("The Xen Hypervisor is back to a 'normal' state")
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the Xen hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_on_regex", type="string", dest="vm_on_regex", default=None, help="The regex of the VMs to power on")
    parser.add_option("--vm_off_regex", type="string", dest="vm_off_regex", default=None, help="The regex for the VMs to power off")
    parser.add_option("--template_regex", type="string", dest="template_regex", default=None, help="the regex of any templates. Optional")
    parser.add_option("--delete_regex", type="string", dest="delete_regex", default=None, help="The regex of the VMs to delete")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, vm_on_regex=options.vm_on_regex, vm_off_regex=options.vm_off_regex, delete_regex=options.delete_regex, template_regex=options.template_regex, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

