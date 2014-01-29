"""
This script will do a complete setup of a kvm hypervisor
    1. Create SF account for KVM on cluster
    2. Create template volume for KVM host
    3. Login and mount template volume on KVM host
    4. Mount NFS share on KVM host

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password

    --client_ip        The IP address of the client

    --client_user       The username for the client

    --client_pass       The password for the client

    --nfs_ip            The IP address of the nfs datastore

    --nfs_path          The path on the nfs datastore you want to mount

    --mount_point       The location on the client where you want to mount the nfs datastore

    --debug             More verbose logging


"""


import lib.libsf as libsf
import logging
import lib.sfdefaults as sfdefaults
import sys
from optparse import OptionParser
from lib.libsf import mylog
from lib.action_base import ActionBase
import kvm_mount_nfs_datastore
import create_volumes_for_client
import create_account_for_client
import login_client
import logout_client
import mount_volumes_test

class KvmHypervisorSetupAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"
        CREATED_ACCOUNT = "CREATED_ACCOUNT"
        CREATED_VOLUME_FOR_ACCOUNT = "CREATED_VOLUME_FOR_ACCOUNT"
        LOGGED_IN_CLIENT = "LOGGED_IN_CLIENT"
        MOUNTED_VOLUME = "MOUNTED_VOLUME"
        MOUNTED_NFS = "MOUNTED_NFS"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIP" : libsf.IsValidIpv4Address,
                            "nfsIP" : libsf.IsValidIpv4Address,
                            "mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "clientPass" : None,
                            "clientUser" : None,
                        },
        args)


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, clientIP=None, clientUser=sfdefaults.host_user, clientPass=sfdefaults.host_pass, nfsIP=sfdefaults.nfs_ip, mountPoint=sfdefaults.nfs_mount_point, nfsPath=sfdefaults.kvm_nfs_path, debug=False):

        self.ValidateArgs(locals())

        if debug:
            mylog.console.setLevel(logging.DEBUG)

        temp = []
        temp.append(clientIP)
        clientIP = temp

        mylog.step("Trying to log out of client volumes")
        if logout_client.Execute(client_ips=clientIP, client_user=clientUser, client_pass=clientPass) == False:
            mylog.warning("Was unable to logout client prior to setting up new volumes")
            mylog.warning("Will try to continue")

        mylog.step("Creating an account for client")
        if(create_account_for_client.Execute(mvip=mvip, client_ips=clientIP, username=username, password=password, client_user=clientUser, client_pass=clientPass) == False):
            mylog.error("There was an error tying to create an account on the cluster")
            self._RaiseEvent(self.Events.FAILURE)
            return False
        self._RaiseEvent(self.Events.CREATED_ACCOUNT)

        # mylog.step("Creating 1 volume for client")
        # if(create_volumes_for_client.Execute(volume_size=65, volume_count=1, mvip=mvip, client_ips=clientIP, enable_512=False, username=username, password=password, client_user=clientUser, client_pass=clientPass) == False):
        #     mylog.error("Failed trying to create 1st volume")
        #     self._RaiseEvent(self.Events.FAILURE)
        #     return False
        # self._RaiseEvent(self.Events.CREATED_VOLUME_FOR_ACCOUNT)

        # mylog.step("Logging in client")
        # if(login_client.Execute(mvip=mvip, client_ips=clientIP, username=username, password=password, client_user=clientUser, client_pass=clientPass) == False):
        #     mylog.error("Failed trying to log client in")
        #     self._RaiseEvent(self.Events.FAILURE)
        #     return False
        # self._RaiseEvent(self.Events.LOGGED_IN_CLIENT)

        # mylog.step("Mounting 1 volume on client")
        # if(mount_volumes_test.Execute(clientIP=clientIP[0], clientUser=clientUser, clientPass=clientPass) == False):
        #     mylog.error("Failed trying to mount volumes on client")
        #     self._RaiseEvent(self.Events.FAILURE)
        #     return False
        # self._RaiseEvent(self.Events.MOUNTED_VOLUME)

        mylog.step("Mounting the nfs datastore")
        if(kvm_mount_nfs_datastore.Execute(clientIP=clientIP[0], clientUsername=clientUser, clientPassword=clientPass, nfsIP=nfsIP, nfsPath=nfsPath, mountPoint=mountPoint) == False):
            mylog.error("There was an error mounting the nfs datastore")
            self._RaiseEvent(self.Events.FAILURE)
            return False
        self._RaiseEvent(self.Events.MOUNTED_NFS)

        mylog.passed("The KVM hypervisor has been set up")
        #done
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="The IP address of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    parser.add_option("-c", "--client_ip", type="string", dest="client_ip", default=None, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client")
    parser.add_option("--nfs_ip", type="string", dest="nfs_ip", default=sfdefaults.nfs_ip, help="the IP address of the nfs datastore")
    parser.add_option("--nfs_path", type="string", dest="nfs_path", default=sfdefaults.kvm_nfs_path, help="the path you want to mount from the nfs datastore")
    parser.add_option("--mount_point", type="string", dest="mount_point", default=sfdefaults.nfs_mount_point, help="the location you want to mount the nfs datasore on the client, ex: /mnt/nfs")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.client_ip, options.client_user, options.client_pass, options.nfs_ip, options.mount_point, options.nfs_path, options.debug):
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