"""
This script will make sure that all the VMs are healthy

When run as a script, the following options/env variables apply:

    --group           The type of hypervisor to run a health check on None=All hypervisors, In this code "group--> vm_type"

    --no_logs           If true will suppress most logging

    --vm_regex          Regex to match to select VMs to power on
"""

import sys
import re
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from clientmon.libclientmon import ClientMon
import lib.libclient as libclient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import multiprocessing
import time



class CheckVmHealthClientmonAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    """
    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientUser" : None,
                            "clientPass" : None,
                            "threadMax" : libsf.IsInteger},
            args)"""


    def Execute(self, vmType=None, noLogs=False, vm_regex=None, debug=False, count = None):

        #self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #if no vm type is provided then we will check all types of vms 
        if vmType is None:
            vm_type_list = ["KVM", "ESX", "HyperV", "Xen", "Pysical"]
            bad_vm_types = []
            good_vm_types = []
            BadHostname = []
            VdBenchDown = []
            for vm_type in vm_type_list:
                GoodVm = 0
                BadVm = 0
                matched_vms = []

                if noLogs == False:
                    mylog.step("Starting health check on " + vm_type)

                monitor = ClientMon()
                monitor_list = monitor.ListClientStatusByGroup(vm_type)

                #if there are VMs of that type being monitored then we will run a health check on them
                if len(monitor_list) > 0:
                    for vm in monitor_list:
                        if vm_regex:
                            m = re.search(vm_regex, vm.Hostname)
                            if m:
                                matched_vms.append(vm)
                        else:
                            matched_vms.append(vm)

                    for host in matched_vms:
                        if (time.time()- float(host.Timestamp))< 30:
                            if (host.VdbenchCount == 0 and host.VdbenchExit == 0):
                                mylog.debug("The Host " + host.Hostname +" at, time:" + time.asctime(time.localtime(host.Timestamp))+" Is actively responding, But vdbench is Not running")
                                GoodVm += 1
                                VdBenchDown.append(host.Hostname)     
                                                           
                            elif (host.VdbenchCount>0 or host.VdbenchExit == 0):
                                mylog.debug("The Host " + host.Hostname +" at, time:" + time.asctime(time.localtime(host.Timestamp))+" Is actively responding and vdbench is running")
                                GoodVm += 1
                            else:
                                mylog.debug("The Host " + host.Hostname +" at, time:" + time.asctime(time.localtime(host.Timestamp))+" Is actively responding, But vdbench is Not running")
                                GoodVm += 1
                                VdBenchDown.append(host.Hostname)
                        else:
                            mylog.debug("The Host " + host.Hostname +" Is neither responding nor vdbench is running, Last responded: "+ time.asctime(time.localtime(host.Timestamp)))
                            BadVm += 1
                            BadHostname.append(host.Hostname)  
                    if count:
                        if BadVm > 0 and GoodVm > count: 
                            good_vm_types.append(vm_type)
                    elif BadVm > 0: 
                        bad_vm_types.append(vm_type)
                    elif GoodVm > 0:
                        good_vm_types.append(vm_type)     


                #else when len < 0
                else:
                    if noLogs == False:
                        mylog.warning("There are no " + vm_type + " VMs being monitored on this database")

                
            #if there are bad VMs on any of the hypervisors then we will report them here
            if len(bad_vm_types) > 0:
                mylog.error("There are bad VMs on the " + ",".join(bad_vm_types) + " hypervisors. The bad VMs are:" + ", ".join(BadHostname))
                if len(good_vm_types) > 0:
                    if len(VdBenchDown) > 0:
                        mylog.info("The VMs on " + ",".join(good_vm_types) + " hypervisors are all healthy, But the vdbench is not running on: " + ", ".join(VdBenchDown))
                        return False
                    else:
                        mylog.info("The VMs on " + ",".join(good_vm_types) + " hypervisors are all healthy")
                        return False

            if len(VdBenchDown) > 0:
                mylog.passed("The VMs on " + ",".join(good_vm_types) + " hypervisors are all healthy, But the vdbench is not running on: " + ", ".join(VdBenchDown))
                return True
            else: 
                mylog.passed("The VMs on "+ ",".join(good_vm_types) + " hypervisors are all healthy")
                return True
            
        #If vmType was provided 
        else: 
            vm_type_list = ["KVM", "ESX", "HyperV", "Xen", "Pysical"]
            MatchedVms = []
            GoodVm = 0
            BadVm = 0
            Bad_Hostname = []
            VdBench_Bad = []
            if vmType not in vm_type_list:
                mylog.error("Wrong type of VM Type. Accepted Values are: " + ", ".join(vm_type_list))
                return False
    
            monitor = ClientMon()
            monitor_list = monitor.ListClientStatusByGroup(vmType)
            
            if len(monitor_list) > 0:
    
                for vm in monitor_list:
                    if vm_regex:
                        m = re.search(vm_regex, vm.Hostname)
                        if m:
                            MatchedVms.append(vm)
                    else:
                        MatchedVms.append(vm)
                
                for Host in MatchedVms:
                    if (time.time() - float(Host.Timestamp))< 30:
                        if (Host.VdbenchCount == 0 and Host.VdbenchExit == 0):
                            mylog.debug("The Host " + Host.Hostname +" at, time:" + time.asctime(time.localtime(Host.Timestamp))+" Is actively responding and But vdbench is not running")
                            GoodVm += 1
                            VdBench_Bad.append(Host.Hostname)

                        elif (Host.VdbenchCount>0 or Host.VdbenchExit == 0):
                            mylog.debug("The Host " + Host.Hostname +" at, time:" + time.asctime(time.localtime(Host.Timestamp))+" Is actively responding and vdbench is running")
                            GoodVm += 1
                        
                        else:
                            mylog.debug("The Host " + Host.Hostname +" at, time:" + time.asctime(time.localtime(Host.Timestamp))+" Is actively responding and But vdbench is not running")
                            GoodVm += 1
                            VdBench_Bad.append(Host.Hostname)
                    else:
                        BadVm += 1
                        mylog.debug("The Host " + Host.Hostname +" Is neither responding nor vdbench is running, Last responded: "+ time.asctime(time.localtime(Host.Timestamp)))
                        Bad_Hostname.append(Host.Hostname)
                #if there are bad VMs on any of the hypervisors then we will report them here
                
                if count and GoodVm >= count and BadVm > 0:
                    mylog.warning("There are " +str(GoodVm)+" >= "+str(count)+"(min count) Good VMs on the " + vmType + " hypervisor. The bad VMs are: " + ", ".join(Bad_Hostname))
                    return True
                
                elif BadVm > 0: 
                    mylog.error("There are " +str(BadVm)+" bad VMs on the " + vmType + " hypervisor. The bad VMs are: " + ", ".join(Bad_Hostname))
                    return False
                
                elif GoodVm == len(MatchedVms):
                    if len(VdBench_Bad) > 0: 
                        mylog.passed("The VMs on "+vmType + " hypervisors are all healthy, But vdbench is not running on: " + ", ".join(VdBench_Bad))
                        return True
                    else: 
                        mylog.passed("The VMs on "+vmType + " hypervisors are all healthy")
                        return True
            else: 
                mylog.warning("There are no " + vmType + " VMs being monitored on this database")
                return False
# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--group", type="string", dest="group", default=None, help="The type of VM to check, ex: KVM")
    parser.add_option("--count", type="int", dest="count", default=None, help="Minimum number of VMs to be found healthy, if count > number of VMs checks for all VMs")
    parser.add_option("--no_logs", action="store_true", dest="noLogs", default=False, help="Turns off logs")
    parser.add_option("--vm_regex", type="string", dest="vm_regex", default=None, help="the regex to match VMs to power on")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.group, options.noLogs, options.vm_regex, options.debug, options.count):
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
