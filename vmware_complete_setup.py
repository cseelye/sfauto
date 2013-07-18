"""
    
    This script will:
    1. create account for esxi server w/ CHAP 
    2. mount datastores
    3. Create Folder on vmHost
    4. register template image from datastore
    5. create template volume on cluster
    6. clone template image to cluster
    7. unregister orginial template imageop
    8. start mass clone

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

    --template_name     The name of the VM template

    --clone_name        The name to give the clone

    --vm_count          The number of clones to make

    --mgmt_server       The IP address of the esxi server 
"""


import sys
import os
import time
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
import vmware_mass_clone_vm

class VmwareMassCloneVmAction(ActionBase):
    
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def VMwareRegisterVM(self, mgmtServer, vmHost, folder, vmName, path, datastore, debug):
        """
            Calls a Perl script
            Registers a VM 
        """
        action_name = "vmware_register_vm_as_template.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vm_name=" + vmName + " --vmhost=" + vmHost + " --datastore_name=" + datastore + " --path=" + path + " --folder=" + folder
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return result

    def VMwareUnregisterVM(self, mgmtServer, vmName, debug):
        """
            Calls a Perl script
            Unregisters a VM from it's name 
        """
        action_name = "vmware_unregister_vm.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vm_name=" + vmName
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True

    def VMwareMountNFS(self, mgmtServer, vmHost, datastoreAddress, datastorePath, datastoreLocalPath, debug):
        """
            Calls a Perl script
            Creates an NFS datastore on the vmHost
        """
        action_name = "vmware_add_nfs_datastore.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vmhost=" + vmHost + " --nfs_address=" + datastoreAddress + " --nfs_path=" + datastorePath + " --nfs_local_path=" + datastoreLocalPath
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True


    def VMwareCloneVM(self, mgmtServer, vmHost, sourceName, cloneName, datastore, folder, thin, template, debug):
        """  
        Calls the perl script to clone a source vm onto a datastore
        """

        action_name = "vmware_clone_vm.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vmhost=" + vmHost + " --source_vm=" + sourceName + " --clone_name=" + cloneName + " --datastore=" + datastore + " --folder=" + folder
        if thin:
            script_args += " --thin"
        if template:
            script_args += " --template"
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True

    def VMwareCreateFolder(self, mgmtServer, folder, parentFolder, debug):
        """  
        Calls the perl script to clone a source vm onto a datastore
        """

        action_name = "vmware_create_folder.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --folder=" + folder + " --parent=" + parentFolder
        if debug:
            script_args += " --debug"
        script = ChildScript("perl -Ivmware_perl vmware_perl/" + action_name + " " + script_args)
        result = script.Run()
        if script.returncode != 0:
            return False
        return True

    def VMwareCreateDatastore(self, mgmtServer, vmHost, debug):
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

    def VMwareSetupChap(self, mgmtServer, vmHost, chapName, chapSecret, chapTarget, svip, debug):
        """  
        Calls the perl script to create datastores from new iscsi volumes
        """

        action_name = "vmware_setup_chap.pl"
        script_args = "--mgmt_server=" + mgmtServer + " --vmhost=" + vmHost + " --chap_name=" + chapName + " --chap_secret=" + chapSecret + " --chap_target=" + chapTarget + " --svip=" + svip
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

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "vmHost" : libsf.IsValidIpv4Address,
                            "mgmtServer" : libsf.IsValidIpv4Address,
                            "nfsAddress" : libsf.IsValidIpv4Address,
                            "cloneName" : None,
                            "vmCount" : libsf.IsInteger,
                            "folder" : None,
                            "templateName" : None

                            },
            args)


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, vmHost=sfdefaults.esx_vmhost, hostUser=sfdefaults.host_user, hostPass=sfdefaults.client_pass, mgmtServer=sfdefaults.esx_mgmt_server, parentFolder=sfdefaults.esx_parent_folder, folder=None, nfsAddress=sfdefaults.nfs_ip, nfsPath=sfdefaults.esx_nfs_path, nfsLocalPath=sfdefaults.esx_nfs_local_path, templatePath=sfdefaults.esx_template_path, templateName=None, cloneName=None, vmCount=sfdefaults.esx_vm_count, thin=False, debug=False):
        
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.banner("Starting to setup on " + vmHost)

        #create an account for the esxi vmhost
        client_ips = [vmHost]
        if create_account_for_client.Execute(mvip=mvip, username=username, password=password, client_ips=client_ips, client_user=hostUser, client_pass=hostPass) == False:
            mylog.error("Could not create an account for " + vmHost)
            return False


        #set up chap
        vmhost_client = SfClient()
        vmhost_client.Connect(vmHost, hostUser, hostPass)
        vmhost_hostname = vmhost_client.Hostname
        vmhost_hostname = vmhost_hostname.lower()

        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListAccounts', {})
        except libsf.SfError as e:
            mylog.error("Failed to get account list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        init = None
        target = None
        for account in result["accounts"]:
            if account["username"] == vmhost_hostname:
                init = account["initiatorSecret"]
                target = account["targetSecret"]

        svip = None
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        except libsf.SfError as e:
            mylog.error("Failed to get account list: " + e.message)
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        svip = result["clusterInfo"]["svip"]

        if init is not None:
            if target is not None:
                if svip is not None:
                    if self.VMwareSetupChap(mgmtServer, vmHost, vmhost_hostname, init, target, svip, debug) == False:
                        mylog.error("Unable to set up CHAP on " + vmHost)

        #create a folder to hold the templates and vm that will be created
        if self.VMwareCreateFolder(mgmtServer, folder, parentFolder, debug) == False:
            mylog.error("Unable to create folder " + folder)
            return False

        #mount the NFS datastore where the template image is located
        if self.VMwareMountNFS(mgmtServer, vmHost, nfsAddress, nfsPath, nfsLocalPath, debug) == False:
            mylog.error("Unable to mount NFS datastore at " + nfsAddress)
            return False

        #register the template vm from the nfs datastore
        tempVMName = "temp-template-nfs"
        template_source_name = self.VMwareRegisterVM(mgmtServer, vmHost, folder, tempVMName, templatePath, nfsLocalPath, debug)

        if template_source_name == False:
            mylog.error("Unable to register the template VM from the NFS datastore on " + vmHost)
            return False

        #get the volume size of the template and double it for the VM clones
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

        #create a volume to clone the nfs template image to
        if create_volumes.Execute(mvip=mvip, username=username, password=password, volume_size=volumeSize, volume_count=1, volume_name_in=templateName) == False:
            mylog.error("Unable to create the volume for the template VM on " + mvip)
            return False

        mylog.step("Waiting for 30 seconds")
        time.sleep(30)

        #create a datastore on the vmhost from the new volume that was just created
        datastore = self.VMwareCreateDatastore(mgmtServer, vmHost, debug)

        if datastore == False:
            mylog.error("Unable to create datastore on " + vmHost + " from " + templateName)
            return False

        #clone the template vm from the nfs datastore to the new datastore on the sf cluster
        if self.VMwareCloneVM(mgmtServer, vmHost, tempVMName, templateName, datastore[0], folder, thin, True, debug) == False:
            mylog.error("Unable to clone the template VM on " + nfsAddress + " to " + templateName)
            return False

        #remove the old template from the inventory, if this fails we will continue
        if self.VMwareUnregisterVM(mgmtServer, tempVMName, debug) == False:
            mylog.warning("Unable to unregister the template on the NFS datastore " + nfsAddress)

        #preform the mass clone on the vmhost
        if vmware_mass_clone_vm.Execute(mvip, username, password, vmCount, templateName, cloneName, mgmtServer, vmHost, hostUser, hostPass, folder, thin) == False:
            mylog.error("Failed during Mass clone")
            return False

        mylog.passed("The VM host has been set up with " + str(vmCount) + " VMs")
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
    
    parser.add_option("--template_name", type="string", dest="template_name", default=None, help="What to name the VM template")  
    parser.add_option("--clone_name", type="string", dest="clone_name", default=sfdefaults.client_user, help="the name of the VM clone") 
    parser.add_option("--vm_count", type="int", dest="vm_count", default=None, help="The number of VM clones to make")
    parser.add_option("--folder", type="string", dest="folder", default=None, help="The name of the folder to put the VM clones in")
    parser.add_option("--parent_folder", type="string", dest="parent_folder", default=None, help="The name of the parent folder")    
    parser.add_option("--thin", action="store_true", dest="thin", default=False, help="to make a thin VM clone")

    parser.add_option("--nfs_ip", type="string", dest="nfsAddress", default=sfdefaults.nfs_ip, help="The IP address of the nfs datastore")
    parser.add_option("--nfs_path", type="string", dest="nfsPath", default=sfdefaults.esx_nfs_path, help="The path to on the nfs datastore you want to mount")
    parser.add_option("--nfs_local_path", type="string", dest="nfsLocalPath", default=sfdefaults.esx_nfs_local_path, help="The path/name of where to mount the nfs datastore on the vmhost")
    parser.add_option("--template_path", type="string", dest="templatePath", default=sfdefaults.esx_template_path, help="The path to the template image on the nfs datastore. Either .vmx or .vmtx file")
    parser.add_option("--clone_name", type="string", dest="cloneName", default=None, help="What to name the VM clones on the host")   
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.vmhost, options.host_user, options.host_pass, options.mgmt_server, options.parent_folder, options.folder, options.nfsAddress, options.nfsPath, options.nfsLocalPath, options.templatePath, options.template_name, options.clone_name, options.vm_count, options.thin, options.debug):
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
