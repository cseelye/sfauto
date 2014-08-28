#!/usr/bin/env python

"""
This action will rescan the FC HBAs in an ESX host

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vmhost            The IP of one or more ESX hosts to verify on

"""

import sys
from optparse import OptionParser
import multiprocessing

import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareRescanFcAction(ActionBase):
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


    def _RescanThread(self, mgmt_server, mgmt_user, mgmt_pass, host_ip, hba_name, results, debug):
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        myname = multiprocessing.current_process().name
        results[myname] = False
        mylog.debug("Starting " + myname)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                host = libvmware.FindHost(vsphere, host_ip)
                for adapter in host.config.storageDevice.hostBusAdapter:
                    if hasattr(adapter, "portWorldWideName") and adapter.device == hba_name:

                        mylog.info("  Rescanning " + adapter.device + " on " + host.name)
                        storage_sys = host.configManager.storageSystem
                        try:
                            storage_sys.RescanHba(hbaDevice=adapter.device)
                        except vmodl.MethodFault as e:
                            mylog.error("  Rescan " + adapter.device + " on " + host.name + " failed: " + str(e))
                            return
        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return

        mylog.passed("  Rescan " + hba_name + " on " + host_ip + " succeeded")
        results[myname] = True
        return


    def Execute(self, vmhost, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, debug=False):
        """
        Rescan FC HBAs
        """
        if vmhost == None:
            vmhost = []
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                manager = multiprocessing.Manager()
                results = manager.dict()
                _threads = []

                mylog.info("Finding hosts and HBAs...")
                hosts = libvmware.FindHost(vsphere, vmhost)
                for host in hosts:
                    for adapter in host.config.storageDevice.hostBusAdapter:
                        if hasattr(adapter, "portWorldWideName"):
                            thread_name = host.name + "-" + adapter.device
                            results[thread_name] = False
                            th = multiprocessing.Process(target=self._RescanThread, name=thread_name, args=(mgmt_server, mgmt_user, mgmt_pass, host.name, adapter.device, results, debug))
                            th.daemon = True
                            _threads.append(th)

                allgood = libsf.ThreadRunner(_threads, results, len(_threads))

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        if allgood:
            mylog.passed("Successfully rescanned FC HBAs")
            return True
        else:
            mylog.error("Error rescanning FC HBAs")
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
    parser.add_option("-o", "--vmhost", action="list", dest="vmhost", default=None, help="the IP of one or more ESX hosts to get WWNs from")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, debug=options.debug):
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
