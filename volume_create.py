#!/usr/bin/env python2.7

"""
This action will create volumes for an account
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, StrType, IPv4AddressType, NameOrID, PositiveNonZeroIntegerType, PositiveIntegerType, OptionalValueType, SolidFireBurstIOPSType, SolidFireMaxIOPSType, SolidFireMinIOPSType, BoolType, SolidFireIDType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError
import time

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "volume_size" : (PositiveNonZeroIntegerType, None),
    "volume_prefix" : (OptionalValueType(StrType), None),
    "volume_name" : (OptionalValueType(StrType), None),
    "volume_count" : (PositiveNonZeroIntegerType, None),
    "volume_start" : (PositiveNonZeroIntegerType, 1),
    "min_iops" : (SolidFireMinIOPSType, sfdefaults.min_iops),
    "max_iops" : (SolidFireMaxIOPSType, sfdefaults.max_iops),
    "burst_iops" : (SolidFireBurstIOPSType, sfdefaults.burst_iops),
    "enable512e" : (BoolType, False),
    "account_name" : (OptionalValueType(StrType), None),
    "account_id" : (OptionalValueType(SolidFireIDType), None),
    "gib" : (BoolType, False),
    "create_single" : (BoolType, False),
    "wait" : (PositiveIntegerType, 0),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def VolumeCreate(volume_size,
                 volume_prefix,
                 volume_name,
                 volume_count,
                 volume_start,
                 min_iops,
                 max_iops,
                 burst_iops,
                 enable512e,
                 account_name,
                 account_id,
                 gib,
                 create_single,
                 wait,
                 mvip,
                 username,
                 password):
    """
    Create volumes

    Args:
        volume_size:        the volume size in GB
        volume_prefix:      the prefix for creating volume names (names will be prefix + %05d)
        volume_name:        the name of the volume if only creating one
        volume_count:       the number of volumes to create
        volume_start:       the numer to start volume names from
        min_iops:           the min guaranteed IOPS for the volumes
        max_iops:           the max sustained IOPS for the volumes
        burst_iops:         the max burst IOPS for the volumes
        enable512e:         use 512 byte sector emulation on the volumes
        account_name:       the name of the account
        account_id:         the ID of the account
        gib:                create volume size in GiB instead of GB
        create_single:      create single volumes at once (do not use CreateMultipleVolumes API)
        wait:               wait for this long between creating each volume (seconds)
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()
    NameOrID(account_name, account_id, "account")

    cluster = SFCluster(mvip, username, password)

    # Find the account
    log.info("Searching for accounts")
    try:
        account = cluster.FindAccount(accountName=account_name, accountID=account_id)
    except UnknownObjectError:
        log.error("Account {} does not exist".format(account_id or account_name))
        return False
    except SolidFireError as e:
        log.error("Could not search for accounts: {}".format(e))
        return False

    # Naming for the volume
    if volume_prefix is None:
        volume_prefix = account.username + "-"
    vol_fmt_str = "{}{{:05d}}".format(volume_prefix)

    # Size of the volume in bytes
    if gib:
        total_size = volume_size * 1024 * 1024 * 1024
    else:
        total_size = volume_size * 1000 * 1000 * 1000

    # Create volumes
    log.info("Creating {} volumes for {}...".format(volume_count, account.username))
    allgood = True
    if create_single or (volume_name and volume_count == 1):
        for vol_num in range(volume_start, volume_start + volume_count):
            if volume_name and volume_count == 1:
                vol_name = volume_name
            else:
                vol_name = vol_fmt_str.format(vol_num)

            try:
                cluster.CreateVolume(vol_name, total_size, account.ID, enable512e, min_iops, max_iops, burst_iops)
            except SolidFireError as e:
                log.error("Failed to create volume {}: {}".format(vol_name, e))
                allgood = False
            
            if wait > 0:
                time.sleep(sfdefaults.TIME_SECOND * wait)

    else:
        vol_names = []
        for vol_num in range(volume_start, volume_start + volume_count):
            vol_names.append(vol_fmt_str.format(vol_num))

        try:
            cluster.CreateVolumes(vol_names, total_size, account.ID, enable512e, min_iops, max_iops, burst_iops)
        except SolidFireError as e:
            log.error("Failed to create volumes for {}: {}".format(account.username, e))
            allgood = False

    if allgood:
        log.passed("Successfully created {} volumes for {}".format(volume_count, account.username))
        return True
    else:
        log.error("Failed to create all volumes for {}".format(account.username))
        return False

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    vol_naming_group = parser.add_mutually_exclusive_group()
    vol_naming_group.add_argument("--volume-name", type=str, metavar="NAME", help="the name for the new volume")
    vol_naming_group.add_argument("--volume-prefix", type=str, metavar="NAME", help="the prefix for creating names for the new volumes (name will be prefix + %%05d)")
    parser.add_argument("--volume-size", type=PositiveNonZeroIntegerType, required=True, metavar="SIZE", help="the volume size in GB")
    parser.add_argument("--volume-count", type=PositiveNonZeroIntegerType, required=True, metavar="COUNT", help="the number of volumes to create")
    parser.add_argument("--volume-start", type=PositiveNonZeroIntegerType, default=1, required=True, metavar="START", help="the volume number to start naming from")
    parser.add_qos_args()
    parser.add_argument("--enable512e", action="store_true", default=False, help="use 512 byte sector emulation on the volumes")
    parser.add_argument("--gib", action="store_true", default=False, help="create volume size in GiB instead of GB")
    parser.add_argument("--create-single", action="store_true", default=False, help="create single volumes at once (do not use CreateMultipleVolumes API)")
    parser.add_argument("--wait", type=PositiveNonZeroIntegerType, metavar="SECONDS", help="wait for this long between creating each volume (seconds)")

    parser.add_account_selection_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VolumeCreate, args)
    app.Run(**args)




