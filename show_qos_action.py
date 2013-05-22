#!/usr/bin/python

"""
This script will show the list of volumes and their current QoS for a specified account
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --source_account    The account name
"""
import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults


def ValidateArgs(args):
    libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                        "username" : None,
                        "password" : None,
                        "sourceAccount" : None},
        args)

def Execute(sourceAccount, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
    """
    Show the list of volumes and their QoS for an account
    """
    ValidateArgs(locals())
    if debug:
        mylog.console.setLevel(logging.DEBUG)

    # Get a list of accounts from the cluster
    accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})

    # Find the corresponding account on the cluster
    account_id = 0
    for account in accounts_list["accounts"]:
        if account["username"].lower() == sourceAccount.lower():
            account_id = account["accountID"]
            break
    if account_id == 0:
        mylog.error("Could not find account " + sourceAccount + " on " + mvip)
        sys.exit(1)

    try:
        stats = libsf.CallApiMethod(mvip, username, password, "GetCompleteStats", {})
    except libsf.SfError as e:
        mylog.error(str(e))
        return False

    cluster_name = stats.keys()[0]
    stats = stats[cluster_name]

    # Get the list of volumes for the account
    try:
        volume_list = libsf.CallApiMethod(mvip, username, password, "ListVolumesForAccount", { "accountID" : account_id })
    except libsf.SfError as e:
        mylog.error(str(e))
        return False

    print
    print "%19s  %5s  %5s %7s %11s %6s %7s %7s" % ("Volume", "ID", "IOPS", "IOSize", "TargetIOPS", "ssLoad", "minIOPS", "maxIOPS")
    for vol in volume_list["volumes"]:
        volume_id = str(vol["volumeID"]) # volume ID keys in GetCompleteStats are strings, not integers
        if volume_id in stats["volumes"]:
            iops = stats["volumes"][volume_id]["actualIOPS"][0]
            io_size = stats["volumes"][volume_id]["averageIOPSize"][0]
            target = stats["volumes"][volume_id]["targetIOPS"][0]
            ssload = stats["volumes"][volume_id]["serviceSSLoad"][0]

            print "%19s  %5s  %5d %7d %11d %6d %7d %7d" % (vol["name"], volume_id, iops, io_size, target, ssload, vol["qos"]["minIOPS"], vol["qos"]["maxIOPS"])

    return True

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--source_account", type="string", dest="source_account", default=None, help="the name of the account to list volumes from")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.source_account, options.mvip, options.username, options.password, options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

