#!/usr/bin/python

"""
This action will get the next VM number that matches a prefix

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_prefix         The prefix of the VM names to match

    --fill              Find the first gap in the sequence instead of the highest number

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import logging
import re
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenGetNextVmNumberAction(ActionBase):
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
                            "vm_prefix" : None},
            args)

    def Execute(self, vm_prefix, fill=False, vmhost=sfdefaults.vmhost_kvm, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Get the next VM number
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

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
        # Get a list of all VMs
        try:
            vm_ref_list = session.xenapi.VM.get_all()
        except XenAPI.Failure as e:
            mylog.error("Could not get VM list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find all the VMs that match
        found_numbers = []
        highest_number = 0
        for vm_ref in vm_ref_list:
            vm = session.xenapi.VM.get_record(vm_ref)
            m = re.search("^" + vm_prefix + r"0*(\d+)$", vm["name_label"])
            if m:
                vm_number = int(m.group(1))
                mylog.debug("Found " + str(vm_number))
                found_numbers.append(vm_number)
                if vm_number > highest_number:
                    highest_number = vm_number
        found_numbers = sorted(found_numbers)

        # Find the first gap in the sequence
        if fill:
            gap = None
            for i in range(1, len(found_numbers) + 1):
                if found_numbers[i-1] != i:
                    gap = i
                    break
            if gap:
                if bash or csv:
                    sys.stdout.write(str(gap))
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                else:
                    mylog.info("The first gap in " + vm_prefix + " is " + str(gap))
                return True
            else:
                mylog.info("There are no gaps in " + vm_prefix)

        # Show the next number in the sequence
        if bash or csv:
            sys.stdout.write(str(highest_number + 1))
            sys.stdout.write("\n")
            sys.stdout.flush()
        else:
            mylog.info("The next VM number for " + str(vm_prefix) + " is " + str(highest_number + 1))
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
    parser.add_option("--vm_prefix", type="string", dest="vm_prefix", default=None, help="the prefix of the VM names to match")
    parser.add_option("--fill", action="store_true", dest="fill", default=False, help="find the first gap in the sequence instead of the highest number")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_prefix, options.fill, options.vmhost, options.csv, options.bash, options.host_user, options.host_pass, options.debug):
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

