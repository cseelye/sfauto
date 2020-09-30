#!/usr/bin/env python

"""
This action will create volumes for an account
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient, OSType
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveNonZeroIntegerType, PositiveIntegerType, ItemList, BoolType, SolidFireMinIOPSType, SolidFireBurstIOPSType, SolidFireMaxIOPSType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil
import copy
import re
from volume_create import VolumeCreate

#pylint: disable=unused-argument
@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "volume_size" : (PositiveNonZeroIntegerType, None),
    "volume_count" : (PositiveNonZeroIntegerType, None),
    "min_iops" : (SolidFireMinIOPSType, 100),
    "max_iops" : (SolidFireMaxIOPSType, 100000),
    "burst_iops" : (SolidFireBurstIOPSType, 100000),
    "enable512e" : (BoolType, False),
    "gib" : (BoolType, False),
    "create_single" : (BoolType, False),
    "wait" : (PositiveIntegerType, 0),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientCreateVolumes(volume_size,
                        volume_count,
                        min_iops,
                        max_iops,
                        burst_iops,
                        enable512e,
                        gib,
                        create_single,
                        wait,
                        client_ips,
                        client_user,
                        client_pass,
                        mvip,
                        username,
                        password):
    """
    Create volumes

    Args:
        volume_size:        the volume size in GB
        volume_count:       the number of volumes to create
        min_iops:           the min guaranteed IOPS for the volumes
        max_iops:           the max sustained IOPS for the volumes
        burst_iops:         the max burst IOPS for the volumes
        enable512e:         use 512 byte sector emulation on the volumes
        gib:                create volume size in GiB instead of GB
        create_single:      create single volumes at once (do not use CreateMultipleVolumes API)
        wait:               wait for this long between creating each volume (seconds)
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    # Make a copy of the arguments passed to this function, to pass on to client threads
    params = copy.deepcopy(locals())

    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # Get a list of accounts from the cluster
    log.info("Searching for accounts")
    try:
        params["account_list"] = SFCluster(mvip, username, password).ListAccounts()
    except SolidFireError as e:
        log.error("Failed to list accounts: {}".format(e))
        return False

    # Get a list of volumes
    log.info("Searching for volumes")
    try:
        params["volume_list"] = cluster.ListActiveVolumes()
    except SolidFireError as e:
        log.error("Failed to list volumes: {}".format(e))
        return False

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        # Pass on to each thread a copy of the arguments passed to this function, but substitute the one IP for this thread
        client_params = copy.deepcopy(params)
        client_params.pop("client_ips")
        client_params["client_ip"] = client_ip
        results.append(pool.Post(_ClientThread, client_params))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully created accounts for all clients")
        return True
    else:
        log.error("Could not create accounts for all clients")
        return False
#pylint: enable=unused-argument

@threadutil.threadwrapper
def _ClientThread(params):
    """create the volumes for a single client, run as a thread"""
    log = GetLogger()

    client_ip = params.pop("client_ip")
    client_user = params.pop("client_user")
    client_pass = params.pop("client_pass")
    account_list = params.pop("account_list")
    volume_list = params.pop("volume_list")
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    if client.remoteOS == OSType.Windows and not params["enable512e"]:
        log.warning("512e not enabled - this may cause Windows problems if using anything earlier than Windows 2012!")

    # Find the account
    account_name = client.HostnameToAccountName()
    params["account_id"] = None
    for account in account_list:
        if account.username.lower() == account_name.lower():
            params["account_id"] = account.ID
            break
    if not params["account_id"]:
        raise SolidFireError("Could not find account {}".format(account_name))

    log.info("Using account {}".format(account_name))

    # See if there are existing volumes to continue the numbering
    params["volume_start"] = 1
    params["volume_prefix"] = account_name + "-v"
    for vol in volume_list:
        m = re.search(params["volume_prefix"] + r"(\d+)$", vol["name"])
        if m:
            vol_num = int(m.group(1))
            params["volume_start"] = max(params["volume_start"], vol_num+1)

    if not VolumeCreate(**params):
        raise SolidFireError("Failed to create volumes")


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    parser.add_argument("--volume-size", type=PositiveNonZeroIntegerType, required=True, metavar="SIZE", help="the volume size in GB")
    parser.add_argument("--volume-count", type=PositiveNonZeroIntegerType, required=True, metavar="COUNT", help="the number of volumes to create")
    parser.add_argument("--min-iops", type=PositiveNonZeroIntegerType, default=100, required=True, metavar="IOPS", help="the min IOPS guarantee for the volumes")
    parser.add_argument("--max-iops", type=PositiveNonZeroIntegerType, default=100000, required=True, metavar="IOPS", help="the max sustained IOPS for the volumes")
    parser.add_argument("--burst-iops", type=PositiveNonZeroIntegerType, default=100000, required=True, metavar="IOPS", help="the max burst IOPS for the volumes")
    parser.add_argument("--enable512e", action="store_true", default=False, help="use 512 byte sector emulation on the volumes")
    parser.add_argument("--gib", action="store_true", default=False, help="create volume size in GiB instead of GB")
    parser.add_argument("--create-single", action="store_true", default=False, help="create single volumes at once (do not use CreateMultipleVolumes API)")
    parser.add_argument("--wait", type=PositiveNonZeroIntegerType, metavar="SECONDS", help="wait for this long between creating each volume (seconds)")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientCreateVolumes, args)
    app.Run(**args)
