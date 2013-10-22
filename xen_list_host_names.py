#!/usr/bin/python

"""
This action will show the hosts an a XenServer pool

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the pool master

    --host_user         The username for the pool

    --host_pass         The password for the pool

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

class XenListHostNamesAction(ActionBase):
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

    def Get(self, vmhost=sfdefaults.vmhost_xen, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List hosts
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
            host_ref_list = session.xenapi.host.get_all()
        except XenAPI.Failure as e:
            mylog.error("Could not get host list - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        host_names = []
        for host_ref in host_ref_list:
            try:
                host = session.xenapi.host.get_record(host_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get host record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            host_names.append(host['name_label'])

        return sorted(host_names)

    def Execute(self, vmhost=sfdefaults.vmhost_xen, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List hosts
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        del self
        matching_srs = Get(**locals())
        if matching_srs == False:
            mylog.error("There was an error getting the list of hosts")
            return False

        if csv or bash:
            separator = ","
            if bash:
                separator = " "
            sys.stdout.write(separator.join(matching_srs) + "\n")
            sys.stdout.flush()
        else:
            for name in matching_srs:
                mylog.info("  " + name)

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
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, csv=options.csv, bash=options.bash, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

