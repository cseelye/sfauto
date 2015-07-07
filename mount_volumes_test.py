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
                            "iqn" : None},
        args)


    def Execute(self, clientIP, clientUser, clientPass, iqn=None, debug=False):

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
        if retcode != 0:
            mylog.error("There was an error issuing an iscsiadm command. Make sure open-iscsi is installed and your are logged into a volume")
            return False

        #get the iqn for later use
        # stdout = stdout.split()
        # iqn = stdout[1]

        #use the iqn to find the disk path
        retcode, stdout, stderr = client.ExecuteCommand("ls /dev/disk/by-path/ | grep " + iqn)
        if retcode != 0:
            mylog.error("There was an error trying to find the disk path")
            return False

        #strip out everything but the location
        stdout = stdout.split("\n")
        stdout.remove("")
        loc = stdout[-1]

        for line in stdout:
            temp = line
            if "part" in temp:
                loc = temp


        #partition and format the volume
        #need to find a better way of doing this - hack
        #requires the file /home/solidfire/fdisk_input.txt - contents are n \n p \n 1 \n \n \n w
        #Creates a (n)ew partition, makes it (p)rimary, (1) partition,() start at the start,() end at the end, (w)rite

        inputTest = " n \n p \n 1 \n \n \n w "
        retcode, stdout, stderr = client.ExecuteCommand("echo -e '" + inputTest + "' > /tmp/fdisk_input.txt")

        mylog.step("Partitioning the volume")
        retcode, stdout, stderr = client.ExecuteCommand("cat /tmp/fdisk_input.txt | fdisk /dev/disk/by-path/" + loc)
        mylog.debug("Retcode: " + str(retcode))
        mylog.debug("Standard Output: " + str(stdout))
        mylog.debug("Standard Error: " + str(stderr))
        if retcode == 0:
            mylog.passed("The volume has been partitioned")
        else:
            mylog.error("There was an error partitioning the volume")
            return False

        mylog.info("Path to the volume: /dev/disk/by-path/" + loc + "-part1")
        mylog.step("Formatting the volume ext4")
        retcode, stdout, stderr = client.ExecuteCommand("mkfs.ext4 -E nodiscard /dev/disk/by-path/" + loc + "-part1")
        mylog.debug("Retcode: " + str(retcode))
        mylog.debug("Standard Output: " + str(stdout))
        mylog.debug("Standard Error: " + str(stderr))
        if retcode == 0:
            mylog.passed("The volume has been formatted ext4")
        else:
            mylog.error("There was an error formatting the volume")
            return False


        #done
        mylog.passed("The volume has been partitioned and formatted")
        return True



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
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ip, options.client_user, options.client_pass, options.debug):
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
