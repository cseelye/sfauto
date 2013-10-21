#!/usr/bin/python

"""
This action will rename the VMs on a XenServer hypervisor so their hostnames match the VM names

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --client_user       The username for the VMs
    SFCLIENT_USER env var

    --client_pass       The password for the VMs
    SFCLIENT_PASS env var
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import lib.libclient as libclient
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class XenRenameVmsAction(ActionBase):
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

    def Execute(self, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        List VMs
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        try:
            vm_list = libxen.GetAllVMs(session)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        allgood = True
        for vm_name, vm in vm_list.items():
            mylog.info("Updating hostname on " + vm['name_label'])
            guest_ref = vm['guest_metrics']
            try:
                guest = session.xenapi.VM_guest_metrics.get_record(guest_ref)
            except libxen.XenError as e:
                mylog.error("Failed to get guest record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                allgood = False
                continue

            nets = guest['networks']
            for ip in nets.values():
                if libsf.IsValidIpv4Address(ip):
                    try:
                        client = libclient.SfClient()
                        client.Connect(ip, client_user, client_pass)
                        client.UpdateHostname(vm['name_label'])
                        client.RebootSoft()
                        break
                    except libclient.ClientError as e:
                        mylog.error(str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        allgood = False
                        continue

        if allgood:
            mylog.passed("Successfully renamed all VMs")
            return True
        else:
            mylog.error("Failed to rename all VMs")
            return False



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the Xen hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the VMs [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the VMs [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, client_user=options.client_user, client_pass=options.client_pass, debug=options.debug):
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

