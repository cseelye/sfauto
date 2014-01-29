#!/usr/bin/python

"""
This action will run a command on all VMs on a KVM hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --command           The command to run

    --thread_max        The number of threads to use

    --output            Shows the output from each command

    
"""

import sys
from optparse import OptionParser
from xml.etree import ElementTree
import logging
import platform
if "win" in platform.system().lower():
    sys.path.insert(0, "C:\\Program Files (x86)\\Libvirt\\python27")
import libvirt
import multiprocessing
sys.path.insert(0, "..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import lib.libclient as libclient
import kvm_list_vm_ips


class KvmIssueCommandToVmsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientUser" : None,
                            "clientPass" : None,
                            "threadMax" : libsf.IsInteger,
                            "command" : None},
            args)


    def _IssueCommandThread(self, ip, username, password, command, result, output=False):
        client = libclient.SfClient()
        try:
            client.Connect(ip, username, password)
        except libclient.ClientError as e:
            mylog.error(ip + ": Could not connect. Message: " + str(e))
            return
        retcode, stdout, stderr = client.ExecuteCommand(command)
        if retcode == 0:
            result[ip] = True
            if output:
                mylog.passed("\t" + ip + ": " + stdout[:-1])
        else:
            mylog.error(ip + ": Bad Command. Error: " + stderr)


    def Execute(self, vmhost=None, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, command=None, threadMax=10, output=False, debug=False):
        
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        ip_list = kvm_list_vm_ips.Get(vmhost)
        if ip_list == False:
            mylog.error("Could not get a list of IPs for the VMs on " + vmhost)
            return False

        mylog.info("Trying to run command '" + command + "' on the VMs located on " + vmhost)

        self._threads = []
        manager = multiprocessing.Manager()
        results = manager.dict()
        for ip in ip_list:
            results[ip] = False
            th = multiprocessing.Process(target=self._IssueCommandThread, args=(ip, clientUser, clientPass, command, results, output))
            th.daemon = True
            self._threads.append(th)

        allgood = libsf.ThreadRunner(self._threads, results, threadMax)
        if allgood:
            mylog.passed("All VMs ran the command '" + command + "'")
            return True
        else:
            mylog.error("Not all VMs could run the command '" + command + "'")
            return False
    
# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--command", type="string", dest="command", default=None, help="the command to run on the clients")
    parser.add_option("--output", action="store_true", dest="output", default=False, help="Add to display the output on each VM")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=10, help="the number of threads to use")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.client_user, options.client_pass, options.command, options.thread_max, options.output, options.debug):
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