#!/usr/bin/python

"""
This action will return the sr for a given VM


When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the hypervisor host

    --host_user         The host username
    SFHOST_USER env var

    --host_pass         The host password
    SFHOST_PASS env var

    --vm_name           The name of a single VM to power on

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


vmhost = '172.16.150.28'
host_user = 'root'
host_pass = 'solidfire'
vm_name = 'xen-00002'


class XenListVmSrAction(ActionBase):
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


    def Get(self, vm_name=None, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):

        #find the current VM
        mylog.info("Connecting to " + vmhost)
        session = None
        mylog.info("Connecting to the Xen Server")

        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("Trying to find the VM ref")
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
            if not vm_ref or len(vm_ref) <= 0:
                mylog.error("Could not find the source VM " + vm_name)
                return False
        except libxen.XenError as e:
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("Trying to find the VM")
        try:
            vm = session.xenapi.VM.get_record(vm_ref[0])
        except libxen.XenError as e:
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        all_vbds = vm["VBDs"]
        sr_name = None
        for vbd_ref in all_vbds:
            try:
                vbd = session.xenapi.VBD.get_record(vbd_ref)
                if vbd["type"] == "Disk":
                    vdi_ref = vbd['VDI']
                    mylog.debug("Trying to get the vdi from the vdi_ref")
                    try:
                        vdi = session.xenapi.VDI.get_record(vdi_ref)
                    except XenAPI.Failure as e:
                        mylog.error("Could not get the VDI from the VBD")
                        return False
                    sr_ref = vdi["SR"]
                    mylog.debug("Trying to get the SR from the sr_ref")
                    try:
                        sr = session.xenapi.SR.get_record(sr_ref)
                    except XenAPI.Failure as e:
                        mylog.error("Could not get the SR from the VDI")
                        return False
                    sr_name = sr['name_label']
                    sr_name_temp = sr_name.split('.')[:-1][0]
            except libxen.XenError as e:
                mylog.error("Could not get the information for the SR")
                return False

        if sr_name is None:
            mylog.error("error Could not find SR name")
            return False
        return sr_name


    def Execute(self, vm_name=None, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List a VM's SR
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        del self
        matching_srs = Get(**locals())
        if matching_srs == False:
            mylog.error("There was an error getting the list of SRs")
            return False

        mylog.info("The VM: " + vm_name + " is on SR: " + matching_srs)

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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the regex to match SRs - show all if not specified")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

