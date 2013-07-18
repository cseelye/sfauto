"""
    This script will make n number of VM clones on an esxi server

    When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
    --vm_host        The IP address of the vmHost

    --host_user       The username for the vmHost
    SFCLIENT_USER env var

    --host_pass       The password for the vmHost

    --source_name           The name of the VM to clone

    --clone_name        The name to give the clone

    --vm_count          The number of clones to make

    --mgmt_server       The IP address of the esxi server 

"""



import sys
import os
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.libsf import ChildScript
from lib.libclient import ClientError, SfClient
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import create_volumes
import list_accounts
import create_account_for_client

class VmwareMassCloneVmAction(ActionBase):
    
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "vmHost" : libsf.IsValidIpv4Address,
                            "mgmtServer" : libsf.IsValidIpv4Address,
                            "sourceName" : None,
                            "cloneName" : None,
                            "vmCount" : libsf.IsInteger,
                            "folder" : None
                            },
            args)


    def CreateDatastore(self, mgmtServer, vmHost, debug):
        """  
        Calls the perl script to create datastores from new iscsi volumes
        """

        action_name = "vmware_create_datastores.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vmhost=" + vmHost
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args, timeout=1200)
        result = script.Run()
        if script.returncode != 0:
            return False
        return result

    def GetVmSize(self, mgmtServer, vmName, debug):
        """  
        Calls the perl script to clone a source vm onto a datastore
        """

        action_name = "vmware_get_vm_disk_size.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vm_name=" + vmName + " --memory"
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return result

    def CloneVM(self, mgmtServer, vmHost, sourceName, cloneName, datastore, folder, thin, debug):
        """  
        Calls the perl script to clone a source vm onto a datastore
        """

        action_name = "vmware_clone_vm.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vmhost=" + vmHost + " --source_vm=" + sourceName + " --clone_name=" + cloneName + " --datastore=" + datastore + " --folder=" + folder
        if thin:
            script_args += " --thin"
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True



    def PowerOnVm(self, mgmtServer, vmName, folder, debug):
        """  
        Calls the perl script to power on a vm
        """

        action_name = "vmware_poweron_vms.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --folder=" + folder + " --vm_name=" + vmName

        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmCount=None, sourceName=None, cloneName=None, mgmtServer=None, vmHost=sfdefaults.client_ips, vmUser=sfdefaults.client_user, vmPass=sfdefaults.client_pass, folder=None, thin=False, debug=False):
        
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.banner("Attempting to make " + str(vmCount) + " clones of " + sourceName + " on " + vmHost + " with storage from " + mvip)


        client = SfClient()

        try:
            client.Connect(vmHost, vmUser, vmPass)
            hostname = client.Hostname
            accountName = hostname.lower()
        except SfClient.ClientError as e:
            mylog.error(vmHost + ": Could not connect to client")
            return False

        #get the list of accounts on the cluster
        account_list = list_accounts.Get(mvip=mvip, username=username, password=password)
        if account_list == False:
            mylog.error("There was an error getting a list of accounts on " + mvip)
            return False

        #check to see if the vmHost already has an account on the cluster and if not add it
        if accountName not in account_list:
            client_ips = [vmHost]
            mylog.info("Creating account: " + accountName + " on " + mvip)
            if create_account_for_client.Execute(mvip=mvip, client_ips=client_ips, username=username, password=password, client_user=vmUser, client_pass=vmPass) == False:
                mylog.error("Could not create account: " + accountName + " on " + mvip)
                return False


        volumeSize = self.GetVmSize(mgmtServer, sourceName, debug)
        if volumeSize == False:
            mylog.warning("Could not get volume size of " + sourceName + ". Defaulting to 65 GB")
            volumeSize = 65
        else:
            volumeSize = int(volumeSize)
            volumeSize /= 1024
            volumeSize /= 1024
            volumeSize *= 2
        if volumeSize < 65:
            volumeSize = 65

        #create volumes
        mylog.step("Creating volumes with prefix: " + cloneName)
        cloneName += "-"
        if create_volumes.Execute(mvip=mvip, username=username, password=password, volume_size=volumeSize, volume_count=vmCount, volume_prefix=cloneName, enable_512=True, min_iops=100, max_iops=100000, burst_iops=100000, account_name=accountName) == False:
            mylog.error("Unable to create volumes with prefix " + cloneName + " on " + mvip)
            return False

        #create datastores on vmhost
        datastore_list = self.CreateDatastore(mgmtServer, vmHost, debug)

        if datastore_list == False:
            mylog.error("Could not create new datastore for vm clone")
            return False

        for x in xrange(1, vmCount + 1):
            postfix = str(x).zfill(5)
            for datastore in datastore_list:
                if postfix in datastore:
                    tempCloneName = cloneName + postfix
                    
                    mylog.banner("Cloning VM " + sourceName + " to " + tempCloneName + " on " + datastore)
                    if self.CloneVM(mgmtServer, vmHost, sourceName, tempCloneName, datastore, folder, thin, debug) == False:
                        mylog.error("Could not clone new VM")
                        return False
                    
                    mylog.step("  Powering on " + tempCloneName)
                    if self.PowerOnVm(mgmtServer, tempCloneName, folder, debug) == False:
                        mylog.error("There was an error trying to power on the VM: " + cloneName)
                        return False

        mylog.passed(str(vmCount) + " VMs were created on " + vmHost + " with storage on " + mvip)
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")

    parser.add_option("--mgmt_server", type="string", dest="mgmt_server", default=None, help="the management IP of the esxi server")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=None, help="the IP of the VM host")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.client_user, help="the username for the VM host")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.client_pass, help="the password for the VM host")
    
    parser.add_option("--source_name", type="string", dest="source_name", default=None, help="The name of the VM to clone")  
    parser.add_option("--clone_name", type="string", dest="clone_name", default=sfdefaults.client_user, help="the name of the VM clone") 
    parser.add_option("--vm_count", type="int", dest="vm_count", default=None, help="The number of VM clones to make")
    parser.add_option("--folder", type="string", dest="folder", default=None, help="The name of the folder to put the VM clones in")
    parser.add_option("--thin", action="store_true", dest="thin", default=False, help="to make a thin VM clone")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vm_count, options.source_name, options.clone_name, options.mgmt_server, options.vmhost, options.host_user, options.host_pass, options.folder, options.thin, options.debug):
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

