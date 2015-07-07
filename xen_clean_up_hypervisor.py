#shutdown all VMs
#delete all VMs except the template/gold copy
#remove all datastores except where the template/gold copy is
#remove the same volumes on the cluster


import sys
from optparse import OptionParser
import re
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import xen_list_vm_names
import xen_destroy_sr
import xen_shutdown_vms
import delete_volumes
import xen_poweroff_vms
import xen_delete_vms
import get_client_hostname
import delete_account

class XenCleanUpHypervisorAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None,
                            "username" : None,
                            "password" : None},
            args)


    def Execute(self, vmhost=None, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, account_name=None, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):

        if debug:
            mylog.console.setLevel(logging.DEBUG)

        self.ValidateArgs(locals())

        vm_list = xen_list_vm_names.Get(vmhost=vmhost, host_user=host_user, host_pass=host_pass, debug=debug)
        if vm_list is False:
            mylog.error("Could not get a list of VMs on the Xen Server: " + vmhost)
            return False

        #the regex of the VMs not to delete: we want to keep the templates
        reserved_regex = ["template", "gold"]
        template_vm_list = []

        #search through the VMs found and grab the names of all the template VMs
        for reserved in reserved_regex:
            for vm in vm_list:
                m = re.search(reserved, vm)
                if m:
                    template_vm_list.append(vm)

        if len(template_vm_list) > 0:
            mylog.info("The following template VMs will not be deleted: " + ", ".join(template_vm_list))

            #delete the template VMs from the found VM list
            vm_list = list(set(vm_list) - set(template_vm_list))

        #shutdown all the VMs
        if xen_poweroff_vms.Execute(vm_list_input=vm_list, vmhost=vmhost, host_user=host_user, host_pass=host_pass, parallel_thresh=parallel_thresh, parallel_max=parallel_max, debug=debug) is False:
            mylog.error("Could not shutdown all the VMs on " + vmhost)
            return False

        if xen_delete_vms.Execute(vm_list_input=vm_list, vmhost=vmhost, host_user=host_user, host_pass=host_pass, parallel_thresh=parallel_thresh, parallel_max=parallel_max, debug=debug) is False:
            mylog.error("Coule not delelte all the VMs on " + vmhost)
            return False

        if xen_destroy_sr.Execute(sr_regex="xen", vmhost=vmhost, host_user=host_user, host_pass=host_pass, parallel_thresh=parallel_thresh, parallel_max=parallel_max, debug=debug) is False:
            mylog.error("Could not delete all the SRs on " + vmhost)
            return False

        if account_name is None:
            #get client hostname
            hostname = get_client_hostname.Get(client_ip=vmhost, client_user=host_user, client_pass=host_pass, debug=debug)
            if hostname is False:
                mylog.error("Could not get the hostname for " + vmhost)
                return False
            account_name = hostname

        if delete_volumes.Execute(mvip=mvip, username=username, password=password, source_account=account_name, purge=True, debug=debug) is False:
            mylog.error("Could not delete all the volumes for " + account_name)
            return False

        if delete_account.Execute(mvip=mvip, username=username, password=password, account_name=account_name) is False:
            mylog.error("Could not delete the account: " + account_name)
            return False

        mylog.passed("The Xen Hypervisor is now clean")
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    #cluster info
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    #vmhost info
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_kvm, help="the management IP of the KVM hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the account name associated with the hypervisor [%default]")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.xenapi_parallel_calls_thresh, help="do not use multiple threads unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.xenapi_parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.account_name, options.mvip, options.username, options.password, options.parallel_thresh, options.parallel_max, options.debug):
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