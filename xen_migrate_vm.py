#!/usr/bin/python

"""
This action will migrate a XenServer VM to the specified host

When run as a script, the following options/env variables apply:
    --vmhost            The managment IP of the pool master

    --host_user         The pool username
    SFHOST_USER env var

    --host_pass         The pool password
    SFHOST_PASS env var

    --vm_name           The name of the VM to migrate

    --dest_host         The name of the host to migrate to - a random host is picked if not specified
"""

import sys
from optparse import OptionParser
import logging
import re
import time
import multiprocessing
import random
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenMigrateVmAction(ActionBase):
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
                            "host_pass" : None,
                            "vm_name" : None
                            },
            args)

    def Execute(self, vm_name=None, dest_host=None, vmhost=sfdefaults.vmhost_xen,  host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        Migrate a VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the requested VM
        try:
            vm_ref = session.xenapi.VM.get_by_name_label(vm_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find VM " + vm_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        if not vm_ref or len(vm_ref) <= 0:
            mylog.error("Could not find VM '" + vm_name + "'")
            self.RaiseFailureEvent(message="Could not find VM '" + vm_name + "'")
            return False
        vm_ref = vm_ref[0]
        try:
            vm = session.xenapi.VM.get_record(vm_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not get VM record for " + vm_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        if dest_host:
            # Find the requested host
            try:
                new_host_ref = session.xenapi.host.get_by_name_label(dest_host)[0]
            except XenAPI.Failure as e:
                mylog.error("Could not find host " + dest_host + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            try:
                new_host = session.xenapi.host.get_record(new_host_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get host record for " + dest_host + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
        else:
            # Pick a random host from all of the hosts in the pool
            host_ref_list = session.xenapi.host.get_all()
            all_hosts = dict()
            for hr in host_ref_list:
                host = session.xenapi.host.get_record(hr)
                all_hosts[hr] = host
            new_host_ref = random.choice(all_hosts.keys())
            while new_host_ref == vm['resident_on']:
                new_host_ref = random.choice(all_hosts.keys())
            new_host = all_hosts[new_host_ref]


        # Migrate the VM to the new host
        mylog.info("  " + vm['name_label'] + ": migrating to " + new_host['name_label'])
        success = False
        retry = 3
        while retry > 0:
            try:
                session.xenapi.VM.pool_migrate(vm_ref, new_host_ref, {})
                success = True
                break
            except XenAPI.Failure as e:
                if e.details[0] == "CANNOT_CONTACT_HOST":
                    time.sleep(30)
                    retry -= 1
                    continue
                else:
                    mylog.error("  " + vm['name_label'] + ": Failed to migrate - " + str(e))
                    self.RaiseFailureEvent(message=str(e), vmName=vm['name_label'], exception=e)
                    return False

        if success:
            mylog.passed("Successfully migrated " + vm['name_label'] + " to host " + new_host['name_label'])
            return True
        else:
            mylog.error("Failed to migrate " + vm['name_label'] + " to host " + new_host['name_label'])
            self.RaiseFailureEvent(message=str(e), vmName=vm['name_label'])
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
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the single VM to power on")
    parser.add_option("--dest_host", type="string", dest="dest_host", default=None, help="the name of the host to migrate to - a random host is picked if not specified")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, dest_host=options.dest_host, vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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

