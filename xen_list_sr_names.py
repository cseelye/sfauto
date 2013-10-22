#!/usr/bin/python

"""
This action will show the SRs on a XenServer hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --sr_regex          The regex to match SR names - show all if not specified

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

class XenListSrNamesAction(ActionBase):
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

    def Get(self, sr_regex=None, vmhost=sfdefaults.vmhost_xen, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List SRs
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

        mylog.info("Searching for matching SRs")
        matching_srs = []
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

            if rec['type'] != 'lvmoiscsi':
                continue

            if not sr_regex:
                matching_srs.append(rec['name_label'])
            else:
                m = re.search(sr_regex, rec['name_label'])
                if m:
                    matching_srs.append(rec['name_label'])

        return sorted(matching_srs)

    def Execute(self, sr_regex=None, vmhost=sfdefaults.vmhost_xen, csv=False, bash=False, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List SRs
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
            mylog.error("There was an error getting the list of SRs")
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
    parser.add_option("--sr_regex", type="string", dest="sr_regex", default=None, help="the regex to match SRs - show all if not specified")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(sr_regex=options.sr_regex, vmhost=options.vmhost, csv=options.csv, bash=options.bash, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

