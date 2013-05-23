#!/usr/bin/python

"""
This action will create volumes for an account

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --volume_prefix     Prefix for the volumes to create

    --volume_count      The number of volumes to create

    --volume_size       The size of the volumes, in GB

    --volume_start      The volume number to start from

    --512e              Use 512e

    --max_iops          QoS maxIOPS

    --min_iops          QoS minIOPS

    --burst_iops        QoS burstIOPS

    --account_name      Name of the account to create the volumes for

    --account_id        ID of the account to create the volumes for

    --wait              How long to pause between creating each volume
"""

import sys
from optparse import OptionParser
import time
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateVolumesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "volume_size" : None,
                            "volume_count" : None,
                            "min_iops" : libsf.IsInteger,
                            "max_iops" : libsf.IsInteger,
                            "burst_iops" : libsf.IsInteger
                            },
                    args)

    def Execute(self, volume_size, volume_count, volume_prefix=None, volume_start=1, enable_512=True, min_iops=100, max_iops=100000, burst_iops=100000, account_name=None, account_id=0, wait=0, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create volumes
        """
        if not account_name and account_id <= 0:
            account_name = self.GetSharedValue(SharedValues.accountName)

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Find the account
        if account_id > 0:
            account_name = None
        mylog.info("Searching for account")
        try:
            account = libsf.FindAccount(mvip, username, password, AccountName=account_name, AccountId=account_id)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        account_name = account["username"]
        account_id = account["accountID"]

        mylog.info("Account name   = " + account_name)
        mylog.info("Account ID     = " + str(account_id))

        if volume_prefix is None:
            volume_prefix = account_name + "-"

        mylog.info("Volume prefix  = " + volume_prefix)
        mylog.info("Volume size    = " + str(volume_size) + " GB")
        mylog.info("Volume count   = " + str(volume_count))
        mylog.info("Max IOPS       = " + str(max_iops))
        mylog.info("Min IOPS       = " + str(min_iops))
        mylog.info("Burst IOPS     = " + str(burst_iops))
        mylog.info("512e           = " + str(enable_512))

        # Create the requested volumes
        created = 0
        for vol_num in range(volume_start, volume_start + volume_count):
            volume_name = volume_prefix + "%05d" % vol_num
            params = {}
            params["name"] = volume_name
            params["accountID"] = account_id
            params["totalSize"] = int(volume_size * 1000 * 1000 * 1000)
            params["enable512e"] = enable_512
            qos = {}
            qos["maxIOPS"] = max_iops
            qos["minIOPS"] = min_iops
            qos["burstIOPS"] = burst_iops
            params["qos"] = qos
            try:
                libsf.CallApiMethod(mvip, username, password, "CreateVolume", params)
            except libsf.SfError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            mylog.info("Created volume " + volume_name)
            created += 1
            if (wait > 0):
                time.sleep(wait)

        if (created == volume_count):
            mylog.passed("Successfully created " + str(volume_count) + " volumes")
            return True
        else:
            mylog.error("Could not create all volumes")
            return False

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
    parser.add_option("--volume_prefix", type="string", dest="volume_prefix", default=None, help="the prefix for the volume (volume name will be volume prefix + %05d)")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=None, help="the number of volumes to create")
    parser.add_option("--volume_size", type="int", dest="volume_size", default=None, help="the volume size in GB")
    parser.add_option("--volume_start", type="int", dest="volume_start", default=1, help="the volume number to start from")
    parser.add_option("--max_iops", type="int", dest="max_iops", default=100000, help="the max sustained IOPS to allow on this volume")
    parser.add_option("--min_iops", type="int", dest="min_iops", default=100, help="the min sustained IOPS to guarentee on this volume")
    parser.add_option("--burst_iops", type="int", dest="burst_iops", default=100000, help="the burst IOPS to allow on this volume")
    parser.add_option("--512e", action="store_true", dest="enable_512", default=False, help="use 512 sector emulation")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the account to create the volumes for (either name or id must be specified)")
    parser.add_option("--account_id", type="int", dest="account_id", default=0, help="the account to create the volumes for (either name or id must be specified)")
    parser.add_option("--wait", type="int", dest="wait", default=0, help="how long to wait between creating each volume (seconds)")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.volume_size, options.volume_count, options.volume_prefix, options.volume_start, options.enable_512, options.min_iops, options.max_iops, options.burst_iops, options.account_name, options.account_id, options.wait, options.mvip, options.username, options.password, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
