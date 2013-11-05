#!/usr/bin/python

"""
This action will relocate (storage migrate) a XenServer VM to the specified SR

When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the pool master

    --host_user         The pool username
    SFHOST_USER env var

    --host_pass         The pool password
    SFHOST_PASS env var

    --vm_name           The name of the VM to migrate

    --dest_sr           The name of the SR to migrate to
"""

import sys
from optparse import OptionParser
import logging
import re
import time
import multiprocessing
import random
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenRelocateVmAction(ActionBase):
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
                            "dest_sr" : None
                            },
            args)

    def Execute(self, vm_name=None, dest_sr=None, vmhost=sfdefaults.vmhost_xen,  host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Relocate a VM
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

        # Find the requested VM
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find VM " + vm_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if not vm_ref or len(vm_ref) <= 0:
            mylog.error("Could not find VM '" + vm_name + "'")
            self.RaiseFailureEvent(message="Could not find VM '" + vm_name + "'")
            return False
        vm_ref = vm_ref[0]
        try:
            vm = session.xenapi.VM.get_record(vm_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not get VM record for " + vm_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find all of the VM's disks
        all_vbd_ref_list = vm['VBDs']
        disk_vbd_ref_list = []
        vdi_list = dict()
        for vbd_ref in all_vbd_ref_list:
            vbd = session.xenapi.VBD.get_record(vbd_ref)
            if vbd['type'].lower() == 'disk':
                disk_vbd_ref_list.append(vbd_ref)
                vdi_ref = vbd['VDI']
                vdi = session.xenapi.VDI.get_record(vdi_ref)
                vdi_list[vdi_ref] = vdi

        # Find the host the VM is on
        #host_ref = vm['resident_on']
        #try:
            #host = session.xenapi.host.get_record(host_ref)
        #except XenAPI.Failure as e:
            #mylog.error("Could not get host record - " + str(e))
            #self.RaiseFailureEvent(message=str(e), exception=e)
            #return False
        #mylog.debug(vm_name + " is resident on " + host['name_label'])

        # Find the requested SR
        try:
            sr_ref_list = session.xenapi.SR.get_by_name_label(dest_sr)
        except XenAPI.Failure as e:
            mylog.error("Could not find SR " + dest_sr + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if len(sr_ref_list) > 0:
            new_sr_ref = sr_ref_list[0]
            try:
                new_sr = session.xenapi.SR.get_record(new_sr_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get SR record for " + dest_sr + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
        else:
            new_sr = None
            try:
                sr_ref_list = session.xenapi.SR.get_all()
            except XenAPI.Failure as e:
                mylog.error("Could not get SR list - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for sr_ref in sr_ref_list:
                try:
                    sr = session.xenapi.SR.get_record(sr_ref)
                except XenAPI.Failure as e:
                    mylog.error("Could not get SR record - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                if dest_sr in sr['name_label']:
                    new_sr = sr
                    new_sr_ref = sr_ref
                    break
        if new_sr == None:
            mylog.error("Could not find SR matching " + dest_sr)
            self.RaiseFailureEvent(message="Could not find SR matching " + dest_sr)
            return False


        # Migrate the VM to the new SR
        mylog.info("  " + vm['name_label'] + ": migrating to " + new_sr['name_label'])
        for vdi_ref in vdi_list.keys():
            mylog.debug("Relocating VDI " + vdi_list[vdi_ref]['name_label'])
            success = False
            retry = 3
            while retry > 0:
                try:
                    session.xenapi.VDI.pool_migrate(vdi_ref, new_sr_ref, {})
                    success = True
                    break
                except XenAPI.Failure as e:
                    if e.details[0] == "CANNOT_CONTACT_HOST":
                        time.sleep(30)
                        retry -= 1
                        continue
                    else:
                        mylog.error("  " + vm['name_label'] + ": Failed to migrate - " + str(e))
                        self.RaiseFailureEvent(message=str(e), vmName=vm['name_label'], exception=e)
                        return False
            if not success:
                mylog.error("Failed to migrate VDI " + vdi_list[vdi_ref]['name_label'])
                self.RaiseFailureEvent(message=str(e), vmName=vm['name_label'])
                return False

        mylog.passed("Successfully migrated " + vm['name_label'] + " to SR " + new_sr['name_label'])
        return True



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the single VM to power on")
    parser.add_option("--dest_sr", type="string", dest="dest_sr", default=None, help="the name of the SR to migrate to - the first SR name that contains this string will be used")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, dest_sr=options.dest_sr, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

