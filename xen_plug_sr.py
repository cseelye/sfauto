#!/usr/bin/python

"""
This action will plug all of the PBDs for the specified SR

When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the pool master

    --host_user         The pool username
    SFHOST_USER env var

    --host_pass         The pool password
    SFHOST_PASS env var

    --sr_name           The name of the SR to plug - if there is no exact match the first SR name that contains this string will be used
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

class XenPlugSrAction(ActionBase):
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
                            "sr_name" : None
                            },
            args)

    def Execute(self, vm_name=None, sr_name=None, vmhost=sfdefaults.vmhost_xen,  host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
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

        # Find the requested SR
        try:
            sr_ref_list = session.xenapi.SR.get_by_name_label(sr_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find SR " + sr_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if len(sr_ref_list) > 0:
            sr_ref = sr_ref_list[0]
            try:
                sr = session.xenapi.SR.get_record(sr_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get SR record for " + sr_name + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
        else:
            sr = None
            try:
                sr_ref_list = session.xenapi.SR.get_all()
            except XenAPI.Failure as e:
                mylog.error("Could not get SR list - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            for ref in sr_ref_list:
                try:
                    rec = session.xenapi.SR.get_record(ref)
                except XenAPI.Failure as e:
                    mylog.error("Could not get SR record - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                if sr_name in rec['name_label']:
                    sr = rec
                    sr_ref = ref
                    break
        if sr == None:
            mylog.error("Could not find SR matching " + sr_name)
            self.RaiseFailureEvent(message="Could not find SR matching " + sr_name)
            return False

        # Get a list of PBDs for the SR
        pbd_ref_list = sr['PBDs']
        pbd_list = dict()
        for pbd_ref in pbd_ref_list:
            try:
                pbd = session.xenapi.PBD.get_record(pbd_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get PBD record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            pbd_list[pbd_ref] = pbd

        # Unplug each PBD
        mylog.info("Plugging all of the PBDs for SR " + sr['name_label'])
        for pbd_ref in pbd_list.keys():
            host_ref = pbd_list[pbd_ref]['host']
            try:
                host = session.xenapi.host.get_record(host_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get host record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            mylog.debug("Plugging PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'])
            success = False
            retry = 3
            while retry > 0:
                try:
                    session.xenapi.PBD.plug(pbd_ref)
                    success = True
                    break
                except XenAPI.Failure as e:
                    if e.details[0] == "CANNOT_CONTACT_HOST":
                        time.sleep(30)
                        retry -= 1
                        continue
                    else:
                        mylog.error("Failed to plug PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'] + " - " + str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False
            if not success:
                mylog.error("Failed to plug PBD")
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        mylog.passed("Successfully plugged SR " + sr['name_label'])
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
    parser.add_option("--sr_name", type="string", dest="sr_name", default=None, help="the name of the SR to plug - if there is no exact match the first SR name that contains this string will be used")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(sr_name=options.sr_name, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

