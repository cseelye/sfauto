"""
This script will make sure that all the VMs are healthy
Takes in a hypervisor and a mvip

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS
    
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --threading         If true will suppress most logging

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



class KvmCheckVmHealthClientmonAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmHost" : libsf.IsValidIpv4Address,
                            "hostUser" : None,
                            "hostPass" : None},
            args)


    def Execute(self, vmHost=None, hostUser=sfdefaults.host_user, hostPass=sfdefaults.host_pass, threading=False, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        monitor = ClientMon()
        monitor_list = monitor.ListClientStatusByGroup("KVM")

        unhealthyVMs = []
        for vm in monitor_list:
            ip = vm.IpAddress
            temp_client = libclient.SfClient()
            try:
                temp_client.Connect(ip, "root", "solidfire")
            except libclient.ClientError as e:
                pass
            try:
                if threading == False:
                    mylog.step(ip + " starting health check")
                    isHealthy = temp_client.IsHealthy()
                else:
                    isHealthy = temp_client.IsHealthySilent()
                if isHealthy == False:
                    unhealthyVMs.append(ip)
            except libclient.ClientError as e:
                mylog.error("There was a problem checking the health of the client. Message: " + str(e))
                unhealthyVMs.append(ip)

        if len(unhealthyVMs) == 0:
            mylog.passed("All VMs are healthy")
            return True
        else:
            mylog.error("There are a total of " + str(len(unhealthyVMs)) + " unhealthy VMs")
            mylog.error("IPs include: " + "\n\t\t\t\t\t      ".join(unhealthyVMs))
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
    parser.add_option("--threading", action="store_true", dest="threading", default=False, help="Turns off logs when using as a thread")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.threading, options.debug):
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