"""
This script will
    1. connect to a client
    2. locate an iscsi volume
    3. partition and format the iscsi volume
    4. mount the volume

    When run as a script, the following options/env variables apply:
    --client_ip         The IP address of the client

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --mount_name        The name of the directory in /mnt/ to mount the volume

"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase


class MountVolumesTestAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"clientIP" : libsf.IsValidIpv4Address,
                        },
        args)


    def Execute(self, clientIP, clientUser, clientPass, mountName, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)


        #connect to the client - i.e. kvm hypervisor
        client = SfClient()
        mylog.info(clientIP + ": Connecting")
        try:
            client.Connect(clientIP, clientUser, clientPass)
        except ClientError as e:
            mylog.error(clientIP + ": " + str(e))
            return False

        #get the iscsi volumes and the iqn
        retcode, stdout, stderr = client.ExecuteCommand("iscsiadm -m node")
        if retcode == 1:
            mylog.error("There was an error issuing an iscsiadm command. Make sure open-iscsi is installed")
            return False

        #get the iqn for later use
        stdout = stdout.split()
        iqn = stdout[1]

        #use the iqn to find the disk path
        retcode, stdout, stderr = client.ExecuteCommand("ls -l /dev/disk/by-path/ | grep " + iqn)
        if retcode == 1:
            mylog.error("There was an error trying to find the disk path")
            return False

        #strip out everything but the location
        stdout = stdout.split("\n")
        loc = stdout[0]
        index = loc.rindex("/")
        loc = loc[index + 1:]

        #partition and format the volume
        #need to find a better way of doing this
        #requires the file /home/solidfire/fdisk_input.txt - contents are n \n p \n 1 \n \n \n w
        #Creates a (n)ew partition, makes it (p)rimary, (1) partition,() start at the start,() end at the end, (w)rite
        retcode, stdout, stderr = client.ExecuteCommand("cat /home/solidfire/fdisk_input.txt | fdisk /dev/" + loc)
        if retcode == 0:
            mylog.info("The volume has been partitioned")
        elif retcode == 1:
            mylog.error("There was an error partitioning the volume")
            return False

        retcode, stdout, stderr = client.ExecuteCommand("mkfs.ext4 -E nodiscard /dev/" + loc + "1")
        if retcode == 0:
            mylog.info("The volume has been formatted ext4")
        elif retcode == 1:
            mylog.error("There was an error formatting the volume")
            return False

        #check to see if the directory where we will mount the volume already exists
        #if it doesn't exist then we will create it
        retcode, stdout, stderr = client.ExecuteCommand("cd /mnt/" + mountName)
        if retcode == 1:
            retcode, stdout, stderr = client.ExecuteCommand("mkdir /mnt/" + mountName)
            if retcode == 0:
                mylog.info("The directory /mnt/" + mountName + " has been created")
            elif retcode == 1:
                mylog.error("There was an error creating the directory /mnt/" + mountName)
                return False
        elif retcode == 0:
            mylog.info("The directory already exists. Attempting to mount volume to it")

        #attempt to mount the volume
        #mount /dev/ + loc + "1 /mnt/" + mountName
        retcode, stdout, stderr = client.ExecuteCommand("mount /dev/" + loc + "1 /mnt/" + mountName)
        if retcode == 0:
            mylog.info("The mount has been created")
        elif retcode == 1:
            mylog.error("There was an error creating the mount at /mnt/" + mountName)
            return False

        #done
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ip", type="string", dest="client_ip", default=sfdefaults.client_ip, help="the IP address of the client")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the client")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the client")
    parser.add_option("--mount_name", type="string", dest="mountName", default=None, help="The name of the directory to mount the volume in /mnt/")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.client_user, options.client_pass, options.mountName, options.debug):
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
