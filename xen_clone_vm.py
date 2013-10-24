#!/usr/bin/python

"""
This action will clone a VM

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_name           The name of the VM to clone

    --clone_name        The name to give the clone

    --dest_sr           The name of the SR to put the clone in. If not specified, chose an SR from the clone name
"""

import sys
from optparse import OptionParser
import logging
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenCloneVmAction(ActionBase):
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
                            "vm_name" : None,
                            "clone_name" : None,
                            #"dest_sr" : None
                            },
            args)

    def Execute(self, vm_name, clone_name, dest_sr=None, poweron=True, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Clone a VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the source VM
        mylog.info("Searching for source VM")
        vm_ref = None
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find source VM '" + vm_name + "' - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if not vm_ref or len(vm_ref) <= 0:
            mylog.error("Could not find source VM '" + vm_name + "'")
            self.RaiseFailureEvent(message="Could not find source VM '" + vm_name + "'")
            return False
        vm_ref = vm_ref[0]

        # Find the destination SR
        mylog.info("Searching for destination SR")
        if not dest_sr:
            dest_sr = clone_name
        dest_sr_ref = None
        try:
            dest_sr_ref = session.xenapi.SR.get_by_name_label(dest_sr)
        except XenAPI.Failure as e:
            mylog.error("Could not find destination SR '" + dest_sr + "' - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if dest_sr_ref and len(dest_sr_ref) > 0:
            dest_sr_ref = dest_sr_ref[0]
        else:
            # If no exact match, search for an SR that starts with the specified string
            dest_sr_ref = None
            sr_ref_list = session.xenapi.SR.get_all()
            for sr_ref in sr_ref_list:
                sr = session.xenapi.SR.get_record(sr_ref)
                if sr["type"] != "lvmoiscsi":
                    continue
                if sr["name_label"].lower().startswith(dest_sr.lower()):
                    dest_sr_ref = sr_ref
                    dest_sr = sr["name_label"]
                    break
            if not dest_sr_ref:
                mylog.error("Could not find destination SR '" + dest_sr + "'")
                self.RaiseFailureEvent(message="Could not find destination SR '" + dest_sr + "'")
                return False

        # Start the clone
        mylog.info("Cloning VM " + vm_name + " to VM " + clone_name + " in SR " + dest_sr + " ...")
        clone_task = None
        try:
            clone_task = session.xenapi.Async.VM.copy(vm_ref, clone_name, dest_sr_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not start clone " + clone_name + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Wait for clone to finish
        task_record = None
        progress = 0.0
        while True:
            task_record = session.xenapi.task.get_record(clone_task)
            if task_record["status"] == "pending":
                if task_record["progress"] - progress > 0.1:
                    progress = task_record["progress"]
                    mylog.info("  " + clone_name + ": %2.1d%%"%(progress*100))
                time.sleep(5)
                continue
            else:
                break
        if task_record["status"] != "success":
            mylog.error("Error cloning " + vm_name + " to " + clone_name + " - " + str(task_record["error_info"]))
            self.RaiseFailureEvent(message="Error cloning " + vm_name + " to " + clone_name + " - " + str(task_record["error_info"]))
            return False

        # Get the new clone
        clone_ref = session.xenapi.VM.get_by_name_label(clone_name)
        if not clone_ref or len(clone_ref) <= 0:
            mylog.error("Could not find clone " + clone_name + " after creation")
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        clone_ref = clone_ref[0]

        # Set the description of the virtual disk
        clone = session.xenapi.VM.get_record(clone_ref)
        for vbd_ref in clone["VBDs"]:
            vbd = session.xenapi.VBD.get_record(vbd_ref)
            if vbd["type"] != "Disk":
                continue
            vdi_ref = vbd["VDI"]
            #vdi = session.xenapi.VDI.get_record(vdi_ref)
            #session.xenapi.VDI.set_name_label(vdi_ref, clone_name + "-disk0")
            #session.xenapi.VDI.set_name_description(vdi_ref, "Boot disk for " + clone_name)
            try:
                vdi = session.xenapi.VDI.get_record(vdi_ref)
            except XenAPI.Failure as e:
                mylog.error("Failed to get VDI record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                allgood = False
                continue
            try:
                session.xenapi.VDI.set_name_label(vdi_ref, clone_name + "-" + vbd["device"])
                session.xenapi.VDI.set_name_description(vdi_ref, "Disk for " + clone_name)
            except XenAPI.Failure as e:
                mylog.error("Failed to set VDI label - " - str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                allgood = False
                continue

        if poweron:
            # Select a host for the clone
            host_ref_list = session.xenapi.VM.get_possible_hosts(clone_ref)
            min_vms = sys.maxint
            dest_host_ref = None
            dest_host = ""
            for host_ref in host_ref_list:
                h = session.xenapi.host.get_record(host_ref)
                if len(h["resident_VMs"]) < min_vms:
                    min_vms = len(h["resident_VMs"])
                    dest_host_ref = host_ref
                    dest_host = h["name_label"]

            mylog.info("Booting " + clone_name + " on host " + dest_host)
            try:
                session.xenapi.VM.start_on(clone_ref, dest_host_ref, False, False)
            except XenAPI.Failure as e:
                mylog.error("Could not start " + clone_name + " : " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        mylog.passed("Successfully cloned " + vm_name + " to " + clone_name)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to clone")
    parser.add_option("--clone_name", type="string", dest="clone_name", default=None, help="the name of the clone to create")
    parser.add_option("--dest_sr", type="string", dest="dest_sr", default=None, help="the name of the SR to put the clone in. If not specified, determine the SR based on clone name")
    parser.add_option("--nopoweron", action="store_false", dest="poweron", default=True, help="do not power on the clone after creation")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_name, options.clone_name, options.dest_sr, options.poweron, options.vmhost, options.host_user, options.host_pass, options.debug):
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

