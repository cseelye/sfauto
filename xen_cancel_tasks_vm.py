#!/usr/bin/python

"""
This action will show the VMs on a XenServer hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_regex          The regex to match VM names

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class XenCancelTasksVmAction(ActionBase):
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

    def Get(self, vm_regex=None, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Get VM records by regex
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

        mylog.info("Searching for matching VMs")
        try:
            vm_list = libxen.GetVMsRegex(session, vm_regex)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        
        session.xenapi.session.logout()
        return vm_list

    def Execute(self, vm_regex=None, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Cancel the tasks on VMs matching the regex
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
            
        del self

        #matching_vms = {}
        matching_vms = Get(**locals())
        
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        i = 0
        if matching_vms == False:
            mylog.error("There was an error getting the list of VMs")
            return False
        else:
            mylog.info("Attempting to Cancel tasks on %d VMs" % len(matching_vms.keys()))
            mylog.debug(matching_vms.keys())
            for vm in matching_vms:
                current = matching_vms[vm]["current_operations"]
                for t in current.keys():
                    try:
                        mylog.info("\t Canceling operation on VM: %s" % matching_vms[vm]["name_label"])
                        session.xenapi.task.cancel(t)
                        i+=1
                    except Exception, e:
                        if e.details[0] == 'HANDLE_INVALID':
                            mylog.warning("\t Task already executed: %s  on: %s" % (t, matching_vms[vm]["name_label"]))
                        else:
                            mylog.error("Failed to cancel task - \n" + str(e))
                            sys.exit(1)
        
        session.xenapi.session.logout()
        
        if i ==0:
            mylog.error("There are no tasks running to be cancelled")
        else: 
            mylog.info("Cancelled tasks on " + str(i)+" VMs")
            
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default="xen", help="the regex to match VMs - show all if not specified")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the Xen hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_regex=options.vm_regex, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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


