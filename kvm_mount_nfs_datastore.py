"""
This script will mount a nfs datastore on a remote client

"""

import lib.libsf as libsf
import logging
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
import lib.libsfnode as libsfnode
import lib.libclient as libclient
import time
import sys
from optparse import OptionParser
from lib.libsf import mylog
from lib.action_base import ActionBase


class KvmMountNfsDatastoreAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"
        PUSHED_SSH_KEYS = "PUSHED_SSH_KEYS"
        BEFORE_SFNODE_RESET = "BEFORE_SFNODE_RESET"
        AFTER_SFNODE_RESET = "AFTER_SFNODE_RESET"
        CLUSTER_NAME_SET = "CLUSTER_NAME_SET"


    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)


    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIP" : libsf.IsValidIpv4Address,
                            "nfsIP" : libsf.IsValidIpv4Address,
                            "clientUsername" : None,
                            "clientPassword" : None
                            },
            args)



    def Execute(self, clientIP, clientUsername, clientPassword, nfsIP, nfsPath="/templates/kvm-templates", mountPoint="/mnt/nfs", write=False, debug=False):
        
        self.ValidateArgs(locals())

        if debug:
            mylog.console.setLevel(logging.DEBUG)


        #connect to the client
        client = libclient.SfClient()

        try:
            client.Connect(clientIP, clientUsername, clientPassword)
            mylog.info("Connection to the client has been established")
        except libsf.SfError as e:
            mylog.error("Unable to connect to the client, error message: " + e.message)
            return False
        
        #check to see if the mount already exists
        result = client.ExecuteCommand("mount -v | grep " + nfsIP)

        #loop over the results
        for i in xrange(0, len(result)):
            try:
                mount_exists = nfsIP in result[i] and nfsPath in result[i] and mountPoint in result[i]
                if mount_exists:
                    mylog.info("The mount already exists")
                    return True
            except TypeError:
                pass

        #make sure the mountPoint already exists, if not create it
        path_result = client.ExecuteCommand("cd " + mountPoint)
        create_folder = False

        #loop over the results
        for i in xrange(0, len(path_result)):
            try:
                path_exists = "No such file or directory" in path_result[i]
                if path_exists:
                    create_folder = True
            except TypeError:
                pass

        #make the directory if needed
        if create_folder:
            mylog.info("Creating the directory: " + mountPoint)
            client.ExecuteCommand("mkdir " + mountPoint)

        #create the mount if needed
        mylog.info("The mount does not exist. Trying to create it now")
        mount_result = client.ExecuteCommand("mount -t nfs " + nfsIP + ":" + nfsPath + " " + mountPoint)

        #make sure the mount is actually there
        done = False
        start_time = time.time()
        while not done:
            result = client.ExecuteCommand("mount -v | grep " + nfsIP)
            result = result[1]
            mount_exists = nfsIP in result and nfsPath in result and mountPoint in result
            if mount_exists:
                mylog.passed("The mount has been created")
                return True
            if(time.time() - start_time > 300):
                mylog.error("Timed out while trying to created the mount")
                return False
            time.sleep(15)

    


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ip", type="string", dest="client_ip", default=None, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client")
    parser.add_option("--nfs_ip", type="string", dest="nfs_ip", default=None, help="the IP address of the nfs datastore")
    parser.add_option("--nfs_path", type="string", dest="nfs_path", default=None, help="the path you want to mount from the nfs datastore")
    parser.add_option("--mount_point", type="string", dest="mount_point", default=None, help="the location you want to mount the nfs datasore on the client, ex: /mnt/nfs")
    parser.add_option("--write", action="store_true", dest="write", default=False, help="True for rw, False for ro")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.client_user, options.client_pass, options.nfs_ip, options.nfs_path, options.mount_point, options.write, options.debug):
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
