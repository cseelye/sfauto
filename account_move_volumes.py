#!/usr/bin/env python

"""
This action will move a list of volumes to an account
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, NameOrID, BoolType, StrType, OptionalValueType, ItemList, SolidFireIDType, PositiveIntegerType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "account_name" : (OptionalValueType(StrType), None),
    "account_id" : (OptionalValueType(SolidFireIDType), None),
    "volume_names" : (OptionalValueType(ItemList(StrType, allowEmpty=True)), None),
    "volume_ids" : (OptionalValueType(ItemList(SolidFireIDType, allowEmpty=True)), None),
    "volume_prefix" : (OptionalValueType(StrType), None),
    "volume_regex" : (OptionalValueType(StrType), None),
    "volume_count" : (OptionalValueType(PositiveIntegerType), None),
    "source_account" : (OptionalValueType(StrType), None),
    "source_account_id" : (OptionalValueType(SolidFireIDType), None),
    "test" : (BoolType, False),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def AccountMoveVolumes(account_name,
                       account_id,
                       volume_names,
                       volume_ids,
                       volume_prefix,
                       volume_regex,
                       volume_count,
                       source_account,
                       source_account_id,
                       test,
                       strict,
                       mvip,
                       username,
                       password):
    """
    Move volumes to an account

    Args:
        account_name:       the name of the account to move to
        account_id:         the ID of the account to move to
        volume_names:       list of volume names to select
        volume_ids:         list of volume IDs to select
        volume_prefix:      select volumes whose names start with this prefix
        volume_regex:       select volumes whose names match this regex
        volume_count:       only select this many volumes
        source_account:     select volumes from this account
        source_account_id:  select volumes from this account
        test:               show the volumes that would be selected but don't actually do anything
        strict:             fail if there are no volumes to add
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()
    account_name, account_id = NameOrID(account_name, account_id, "account")

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for accounts")
    try:
        account = SFCluster(mvip, username, password).FindAccount(accountName=account_name, accountID=account_id)
    except UnknownObjectError:
        log.error("Account does not exist")
        return False
    except SolidFireError as e:
        log.error("Could not search for accounts: {}".format(e))
        return False

    # Get a list of volumes to move
    log.info("Searching for volumes")
    try:
        match_volumes = cluster.SearchForVolumes(volumeID=volume_ids, volumeName=volume_names, volumeRegex=volume_regex, volumePrefix=volume_prefix, accountName=source_account, accountID=source_account_id, volumeCount=volume_count)
    except SolidFireError as e:
        log.error("Failed to search for volumes: {}".format(e))
        return False

    already_in = list(set(account.volumes).intersection(list(match_volumes.keys())))
    log.debug("{} total matches, {} volumes are already already in account".format(len(list(match_volumes.keys())), len(already_in)))

    volumes_to_add = [match_volumes[vid] for vid in set(match_volumes.keys()).difference(account.volumes)]

    if strict and len(volumes_to_add) <= 0:
        log.error("No matching volumes were found")
        return False
    elif len(volumes_to_add) <= 0:
        log.passed("No volumes to move")
        return True

    log.info("{} volumes will be moved to account {}: {}".format(len(volumes_to_add), account.username, ",".join(sorted([vol["name"] for vol in volumes_to_add]))))

    if test:
        log.warning("Test option set; volumes will not be moved")
        return True

    # Move the volumes
    log.info("Moving volumes to account")
    pool = threadutil.GlobalPool()
    results = []
    for volume in volumes_to_add:
        log.info("  Moving volume {} to account {}".format(volume["name"], account.username))
        results.append(pool.Post(_APICallThread, mvip, username, password, volume["volumeID"], account.ID))

    allgood = True
    for idx, volume in enumerate(volumes_to_add):
        try:
            
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error moving volume {}: {}".format(volume["name"], e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully moved all volumes")
        return True
    else:
        log.error("Could not move all volumes")
        return False

@threadutil.threadwrapper
def _APICallThread(mvip, username, password, volume_id, dest_account_id):
    """Modify a volume, run as a thread"""
    vol = SFCluster(mvip, username, password).ModifyVolume(volume_id, {"accountID" : dest_account_id})
    if vol["accountID"] != dest_account_id:
        raise SolidFireError("accountID is not the new account after modifying volume {}".format(volume_id))


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_account_selection_args()
    parser.add_volume_search_args("to be moved")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the volumes are already in the group")
    args = parser.parse_args_to_dict()

    app = PythonApp(AccountMoveVolumes, args)
    app.Run(**args)
