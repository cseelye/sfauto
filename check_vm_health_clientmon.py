"""
This script will make sure that all the VMs are healthy
Takes in a hypervisor and a mvip

When run as a script, the following options/env variables apply:

    --vm_type           The type of hypervisor to run a health check on None=All hypervisors

    --client_user       The username for the client

    --client_pass       The password for the client

    --no_logs           If true will suppress most logging

    --thread_max        Max number of threads to use while checking a client's health

"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from clientmon.libclientmon import ClientMon
import lib.libclient as libclient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import multiprocessing



class CheckVmHealthClientmonAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _HealthCheckThread(self, ip, username, password, noLogs, results):
        client = libclient.SfClient()
        isHealthy = False

        try:    
            client.Connect(ip, username, password)
            if noLogs == False:
                isHealthy = client.IsHealthy()
            else:
                isHealthy = client.IsHealthySilent()
        except libclient.ClientError as e:
            mylog.error(ip + ": There was a problem checking the health of the client. Message: " + str(e))
            results[ip] = False
            return
        results[ip] = isHealthy


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientUser" : None,
                            "clientPass" : None,
                            "threadMax" : libsf.IsInteger},
            args)


    def Execute(self, vmType=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, noLogs=False, threadMax=sfdefaults.parallel_max, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #if no vm type is provided then we will check all types of vms 
        if vmType is None:
            vm_type_list = ["KVM", "ESX", "HyperV", "Xen", "Pysical"]
            bad_vm_types = []
            good_vm_types = []
            for vm_type in vm_type_list:
                if noLogs == False:
                    mylog.step("Starting health check on " + vm_type)

                monitor = ClientMon()
                monitor_list = monitor.ListClientStatusByGroup(vm_type)

                #if there are VMs of that type being monitored then we will run a health check on them
                if len(monitor_list) > 0:
                    self._threads = []
                    manager = multiprocessing.Manager()
                    results = manager.dict()
                    for vm in monitor_list:
                        ip = vm.IpAddress
                        th = multiprocessing.Process(target=self._HealthCheckThread, args=(ip, clientUser, clientPass, noLogs, results))
                        th.daemon = True
                        self._threads.append(th)

                    allgood = libsf.ThreadRunner(self._threads, results, threadMax)

                    if allgood:
                        if noLogs == False:
                            mylog.passed("All " + vm_type + " VMs are healthy")
                        good_vm_types.append(vm_type)
                    else:
                        mylog.error("Not all " + vm_type + " VMs are healthy")
                        bad_vm_types.append(vm_type)
                        badVMs = []
                        for vm in monitor_list:
                            ip = vm.IpAddress
                            if results[ip] == False:
                                badVMs.append(ip)
                        for vm in badVMs:
                            mylog.error(vm + " is not healthy. Type is: " + vm_type)

                #else when len < 0
                else:
                    if noLogs == False:
                        mylog.warning("There are no " + vm_type + " VMs being monitored on this database")

            #if there are bad VMs on any of the hypervisors then we will report them here
            if len(bad_vm_types) > 0:
                mylog.error("There are bad VMs on the: " + ", ".join(bad_vm_types) + " hypervisors")
                if len(good_vm_types) > 0:
                    mylog.info("The VMs on: " + ", ".join(good_vm_types) + " are all healthy")
                return False
            mylog.passed("The VMs on: " + ", ".join(good_vm_types) + " are all healthy")
            return True


        #If vmType was provided 
        vm_type_list = ["KVM", "ESX", "HyperV", "Xen", "Pysical"]

        if vmType not in vm_type_list:
            mylog.error("Wrong type of VM Type. Accepted Values are: " + ", ".join(vm_type_list))
            return False

        monitor = ClientMon()
        monitor_list = monitor.ListClientStatusByGroup(vmType)

        self._threads = []
        manager = multiprocessing.Manager()
        results = manager.dict()
        for vm in monitor_list:
            ip = vm.IpAddress
            th = multiprocessing.Process(target=self._HealthCheckThread, args=(ip, clientUser, clientPass, noLogs, results))
            th.daemon = True
            self._threads.append(th)

        allgood = libsf.ThreadRunner(self._threads, results, threadMax)

        if allgood:
            mylog.passed("All KVM VMs are healthy")
            return True
        else:
            mylog.error("Not all KVM VMs are healthy")
            badVMs = []
            for vm in monitor_list:
                ip = vm.IpAddress
                if results[ip] == False:
                    badVMs.append(ip)
            for vm in badVMs:
                mylog.error(vm + " is not healthy")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vm_type", type="string", dest="vm_type", default=None, help="The type of VM to check, ex: KVM")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client [%default]")
    parser.add_option("--no_logs", action="store_true", dest="noLogs", default=False, help="Turns off logs")
    parser.add_option("--thread_max", type="int", dest="thread_max", default=sfdefaults.parallel_max, help="The number of threads to use when checking a client's health")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vm_type, options.client_user, options.client_pass, options.noLogs, options.thread_max, options.debug):
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