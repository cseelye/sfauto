"""
This script will start or stop sfclienthealthd on a group of xen clients

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --vm_regex          The regex to match VM names

    --start             The option to start sfclienthealthd

    --stop              The option to stop sfclienthealthd

    --client_user       The account name on the xen client

    --client_pass       The password on the xen client



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
import multiprocessing
import lib.libclient as libclient
import xen_get_vm_ips

class XenStartStopSfclienthealthdAction(ActionBase):
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

    def _StartsfclienthealthdThread(self, ip, username, password, result, output=False):
        client = libclient.SfClient()
        try:
            client.Connect(ip, username, password)
        except libclient.ClientError as e:
            mylog.error(ip + ": Could not connect. Message: " + str(e))
            return
        retcode, stdout, stderr = client.ExecuteCommand("start sfclienthealthd")
        if retcode == 0:
            result[ip] = True
            if output:
                mylog.passed("\t" + ip + ": " + stdout[:-1])
        elif retcode == 1 and "Job is already running" in stderr:
                result[ip] = True
                mylog.info(ip + ": sfclienthealthd already running")
        else:
            mylog.error(ip + ": Could not start sfclienthealthd " + stderr)

    def _StopsfclienthealthdThread(self, ip, username, password, result, output=False):
        client = libclient.SfClient()
        try:
            client.Connect(ip, username, password)
        except libclient.ClientError as e:
            mylog.error(ip + ": Could not connect. Message: " + str(e))
            return
        retcode, stdout, stderr = client.ExecuteCommand("stop sfclienthealthd")
        if retcode == 0:
            result[ip] = True
            if output:
                mylog.passed("\t" + ip + ": " + stdout[:-1])
        elif retcode == 1 and "Unknown istance" in stderr:
                result[ip] = True
                mylog.info(ip + ": sfclienthealthd already stopped")
        else:
            mylog.error(ip + ": Could not stop sfclienthealthd " + stderr)


    def Execute(self, vm_regex=None, vmhost=None, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, threadMax=10, start=False, stop=False, debug=False):

        if start == False and stop == False:
            mylog.error("Please add option --start or --stop")
            return False
        if start == True and stop == True:
            mylog.error("Please only use 1 option --start or --stop, not both")
            return False


        if debug:
            mylog.console.setLevel(logging.DEBUG)

        ip_list = xen_get_vm_ips.Get(vm_regex=vm_regex, vm_name=None, vmhost=vmhost, ip_only=True, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False)
        if ip_list == False:
            mylog.error("Could not get a list of IPs for the VMs on " + vmhost)
            return False


        self._threads = []
        manager = multiprocessing.Manager()
        results = manager.dict()
        for ip in ip_list:
            results[ip] = False
            th = None
            if start:
                th = multiprocessing.Process(target=self._StartsfclienthealthdThread, args=(ip, clientUser, clientPass, results, False))
            elif stop:
                th = multiprocessing.Process(target=self._StopsfclienthealthdThread, args=(ip, clientUser, clientPass, results, False))
            if th is None:
                mylog.error("Could not add the thread to the queue")
            else:
                th.daemon = True
                self._threads.append(th)

        allgood = libsf.ThreadRunner(self._threads, results, threadMax)
        if allgood:
            if start:
                mylog.passed("All VMs started sfclienthealthd")
                return True
            elif stop:
                mylog.passed("All VMs stopped sfclienthealthd")
        else:
            if start:
                mylog.error("Not all VMs could start sfclienthealthd")
                return False
            elif stop:
                mylog.error("Not all VMs could stop sfclienthealthd")
                return False


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
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the single VM to power off")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match names of VMs to power off")
    parser.add_option("--vm_count", type="int", dest="vm_count", default=0, help="the number of matching VMs to power off (0 to use all)")
    parser.add_option("--vm_list", action="list", dest="vm_list_input", default=None, help="A list of VMs that you want to power off")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.xenapi_parallel_calls_thresh, help="do not use multiple threads unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.xenapi_parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--start", action="store_true", dest="start", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--stop", action="store_true", dest="stop", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_regex, options.vmhost, options.host_user, options.host_pass, options.client_user, options.client_pass, options.parallel_max, options.start, options.stop, options.debug):
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

