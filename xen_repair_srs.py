#!/usr/bin/python

"""
This action will repair SR connections on all of the connected hosts

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor
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

class XenRepairSrsAction(ActionBase):
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

    def Execute(self, vmhost=sfdefaults.vmhost_kvm, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Repair SR connections
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

        # Make a list of icsi SRs
        sr_list = dict()
        sr_ref_list = session.xenapi.SR.get_all()
        for sr_ref in sr_ref_list:
            sr = session.xenapi.SR.get_record(sr_ref)
            if sr["type"] != "lvmoiscsi":
                continue
            sr_list[sr["name_label"]] = sr_ref

        # For each SR, "plug" each Physical Block Device
        for sr_name in sorted(sr_list.keys()):
            sr_ref = sr_list[sr_name]
            sr = session.xenapi.SR.get_record(sr_ref)
            pbd = session.xenapi.PBD.get_record(sr["PBDs"][0])
            iqn = pbd["device_config"]["targetIQN"]
            mylog.info("Repairing " + sr["name_label"] + " (" + iqn + ")")
            for pbd_ref in sr["PBDs"]:
                pbd = session.xenapi.PBD.get_record(pbd_ref)
                host = session.xenapi.host.get_record(pbd["host"])
                if pbd["currently_attached"]:
                    mylog.info("  Already attached to " + host["name_label"])
                    continue

                mylog.info("  Scan and attach device on " + host["name_label"] + " ...")
                retry = 3
                wait = 10
                while True:
                    try:
                        session.xenapi.PBD.plug(pbd_ref)
                    except XenAPI.Failure as e:
                        mylog.error("  Could not plug PBD - " + str(e))
                        retry -= 1
                        if retry <= 0:
                            self.RaiseFailureEvent(message=str(e), exception=e)
                            return False
                        else:
                            mylog.info("    Retrying in " + str(wait) + " sec...")
                            time.sleep(wait)

        mylog.passed("Successfully repaired all SRs")
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.debug):
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

