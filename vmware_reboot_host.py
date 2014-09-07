#!/usr/bin/env python

"""
This action will reboot an ESX host

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vmhost            The IP of one or more ESX hosts to verify on

    --norestart         Do not restart the VMs on the host after reboot

"""

from optparse import OptionParser
# pylint: disable-msg=E0611
from pyVmomi import vim, vmodl
# pylint: enable-msg=E0611
import re
import string
import sys
import threading
import time

import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareRebootHostAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mgmt_server" : libsf.IsValidIpv4Address,
                            "mgmt_user" : None,
                            "mgmt_pass" : None,
                            "vmhost" : libsf.IsValidIpv4AddressList},
            args)


    def _RebootThread(self, vsphere, host, restart_vms, results):
        myname = threading.current_thread().name
        results[myname] = False
        mylog.info("Rebooting host " + host.name)

        # Shutdown any running VMs
        vms2shutdown = []
        shutdown_names = []
        for vm in host.vm:
            if vm.runtime.powerState == "poweredOn":
                vms2shutdown.append(vm)
        if vms2shutdown:
            try:
                libvmware.ShutdownVMs(vsphere, vms2shutdown)
            except libvmware.VmwareError as e:
                mylog.error("  Failed to shutdown VMs on host " + host.name + ": " + str(e))
                return

        # Send the reboot command
        try:
            task = host.RebootHost_Task(force=True)
            libvmware.WaitForTasks(vsphere, [task])
        except vmodl.MethodFault as e:
            mylog.error("  Failed to reboot host " + host.name + ": " + str(e))
            return
        except libvmware.VmwareError as e:
            mylog.error("  Failed to reboot host " + host.name + ": " + str(e))
            return

        # Wait for host to go down
        mylog.info("  Waiting for host " + host.name + " to go down")
        while libsf.Ping(host.name):
            pass

        # Wait for the host to come back up
        mylog.info("  Waiting for host " + host.name + " to come back up")
        while not libsf.Ping(host.name):
            pass
        mylog.info("  Waiting for host " + host.name + " to reconnect")
        while host.runtime.connectionState == "notResponding":
            pass

        if restart_vms:
            # Bring up VMs
            try:
                libvmware.PoweronVMs(vsphere, vms2shutdown)
            except vmodl.MethodFault as e:
                mylog.error("  Failed to bring up VMs on host " + host.name + ": " + str(e))
                return
            except libvmware.VmwareError as e:
                mylog.error("  Failed to bring up VMs on host " + host.name + ": " + str(e))
                return

        mylog.passed("  Reboot host " + host.name + " succeeded")
        results[myname] = True
        return


    def Execute(self, vmhost, restart_vms=True, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, debug=False):
        """
        Reboot host
        """
        if vmhost == None:
            vmhost = []
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        host_ip_list = []
        if isinstance(vmhost, basestring):
            host_ip_list = vmhost.split(",")
        else:
            try:
                host_ip_list = list(vmhost)
            except ValueError:
                host_ip_list.append(vmhost)
        host_ip_list = map(string.strip, host_ip_list)

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            _threads = []
            results = {}
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                host_list = libvmware.FindHost(vsphere, host_ip_list)
                for host in host_list:
                    thread_name = "reboot-" + host.name
                    results[thread_name] = False
                    t = threading.Thread(name=thread_name, target=self._RebootThread, args=(vsphere, host, restart_vms, results))
                    t.daemon = True
                    _threads.append(t)

                allgood = libsf.ThreadRunner(_threads, results, len(_threads))

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        if allgood:
            mylog.passed("Successfully rebooted all hosts")
            return True
        else:
            mylog.error("Not all hosts were rebooted successfully")
            return False


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-s", "--mgmt_server", type="string", dest="mgmt_server", default=sfdefaults.fc_mgmt_server, help="the IP/hostname of the vSphere Server [%default]")
    parser.add_option("-m", "--mgmt_user", type="string", dest="mgmt_user", default=sfdefaults.fc_vsphere_user, help="the vsphere admin username [%default]")
    parser.add_option("-a", "--mgmt_pass", type="string", dest="mgmt_pass", default=sfdefaults.fc_vsphere_pass, help="the vsphere admin password [%default]")
    parser.add_option("-o", "--vmhost", action="list", dest="vmhost", default=None, help="the IP of one or more ESX hosts to reboot")
    parser.add_option("--norestart", action="store_false", dest="restart_vms", default=True, help="do not restart the VMs on the host after reboot")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, restart_vms=options.restart_vms, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, debug=options.debug):
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
