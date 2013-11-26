#!/usr/bin/python

"""
This action will get the hostname of a system as reported by iDRAC

When run as a script, the following options/env variables apply:
    --ipmi_ip           The IP address of iDRAC

    --ipmi_user         The username for iDRAC
    SFIPMI_USER env var

    --ipmi_pass         The password for iDRAC
    SFIPMI_PASS env var

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

from optparse import OptionParser
import sys
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class GetHostnameFromDracAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"ipmi_ip" : libsf.IsValidIpv4Address},
            args)

    def Get(self, ipmi_ip, csv=False, bash=False, ipmi_user=sfdefaults.ipmi_user, ipmi_pass=sfdefaults.ipmi_pass, debug=False):
        """
        Get the hostname from iDRAC
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        mylog.info("Connecting to " + ipmi_ip + "...")
        try:
            ssh = libsf.ConnectSsh(ipmi_ip, ipmi_user, ipmi_pass)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), clientIP=ipmi_ip, exception=e)
            return False

        command = "racadm getsysinfo -s"
        mylog.debug("Executing " + command + " on " + ipmi_ip)
        stdin, stdout, stderr = ssh.exec_command(command)
        return_code = stdout.channel.recv_exit_status()
        stdout_data = stdout.readlines()
        stderr_data = stderr.readlines()
        if return_code != 0:
            mylog.error("Failed to execute racadm command: " + "\n".join(stdout_data) + "\n".join(stderr_data))
            self.RaiseFailureEvent(message="Failed to execute racadm command: " + "\n".join(stdout_data) + "\n".join(stderr_data), clientIP=ipmi_ip)
            return False

        hostname = None
        for line in stdout_data:
            if "Host Name" in line:
                pieces = line.split("=")
                hostname = pieces[1].strip()
                break
        if not hostname:
            mylog.error("Could not find hostname in DRAC output")
            return False

        return hostname

    def Execute(self, ipmi_ip, csv=False, bash=False, ipmi_user=sfdefaults.ipmi_user, ipmi_pass=sfdefaults.ipmi_pass, debug=False):
        """
        Show the hostname from iDRAC
        """
        del self
        hostname = Get(**locals())
        if hostname is False:
            return False

        if csv or bash:
            sys.stdout.write(str(hostname))
            sys.stdout.write("\n")
            sys.stdout.flush()
        else:
            mylog.info(ipmi_ip + " iDRAC reports hostname " + str(hostname))
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--ipmi_ip", type="string", dest="ipmi_ip", default=None, help="the IP address of the iDRAC")
    parser.add_option("--ipmi_user", type="string", dest="ipmi_user", default=sfdefaults.ipmi_user, help="the username for iDRAC [%default]")
    parser.add_option("--ipmi_pass", type="string", dest="ipmi_pass", default=sfdefaults.ipmi_pass, help="the password for iDRAC [%default]")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(ipmi_ip=options.ipmi_ip, csv=options.csv, bash=options.bash, ipmi_user=options.ipmi_user, ipmi_pass=options.ipmi_pass, debug=options.debug):
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
