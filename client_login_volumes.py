#!/usr/bin/env python2.7

"""
This action will log in to iSCSI volumes on the clients
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient, OSType
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, SelectionType, OptionalValueType, BoolType, StrType, SolidFireIDType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil
import six

@logargs
@ValidateAndDefault({
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "login_order" : (SelectionType(sfdefaults.all_login_orders), sfdefaults.login_order),
    "auth_type" : (SelectionType(sfdefaults.all_auth_types), sfdefaults.auth_type),
    "account_name" : (OptionalValueType(StrType), None),
    "account_id" : (OptionalValueType(SolidFireIDType), None),
    "target_list" : (OptionalValueType(ItemList(StrType)), None),
    "clean" : (BoolType, True),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientLoginVolumes(client_ips,
                       client_user,
                       client_pass,
                       login_order,
                       auth_type,
                       account_name,
                       account_id,
                       target_list,
                       clean,
                       mvip,
                       username,
                       password):
    """
    Login to iSCSI volumes on clients

    Args:
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        login_order:        how to login, parallel or serial per target
        auth_type:          what type of authentication to use, CHAP or IQN
        account_name:       the name of the account to use, shared across all clients
        account_id:         the ID of the account, shared across all clients
        target_list:        explicit list of targets to log in to
        clean:              clean the iSCSI initiator before logging in
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    try:
        svip = SFCluster(mvip, username, password).GetClusterInfo()["svip"]
    except SolidFireError as ex:
        log.error(ex)
        return False

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip,
                                                client_user,
                                                client_pass,
                                                auth_type,
                                                account_name,
                                                account_id,
                                                login_order,
                                                target_list,
                                                clean,
                                                svip,
                                                mvip,
                                                username,
                                                password))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Failure connecting to volumes: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully logged in to volumes on all clients")
        return True
    else:
        log.error("Could not log in to volumes on all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, auth_type, account_name, account_id, login_order, target_list, clean, svip, mvip, username, password):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    # Clean iSCSI if there are no volumes logged in
    targets = client.GetLoggedInTargets()
    if not targets and clean:
        log.info("Cleaning iSCSI initiator")
        client.CleanIscsi()

    expected_volumes = 0
    cluster = SFCluster(mvip, username, password)
    if auth_type == "chap":
        # If we are using CHAP, find/create the account on the cluster
        if not account_name:
            account_name = client.hostname
        
        # Find the account
        account = cluster.FindAccount(accountName=account_name, accountID=account_id)

        # If this is a Windows client, make sure the CHAP secret is aphanumeric
        if client.remoteOS == OSType.Windows:
            if not account.initiatorSecret.isalnum():
                raise SolidFireError("CHAP secret must be alphanumeric for Windows client")

        log.info("Using account {} with initiator secret {}".format(account.username, account.initiatorSecret))
        expected_volumes = len(account.volumes)

        # Setup the CHAP credentials on the client
        client.SetupCHAP(svip, account.username, account.initiatorSecret)

    else: # auth_type is volume access group
        client_iqn = client.GetInitiatorIDs()[0]
        volgroups = cluster.ListVolumeAccessGroups()
        for group in volgroups:
            if client_iqn in group.initiators:
                expected_volumes += len(group.volumes)

    # Do an iSCSI discovery
    log.info("Discovering iSCSI volumes")
    client.RefreshTargets(svip, expected_volumes)

    # Log in to the volumes
    log.info("Logging in to {} iSCSI volumes".format(expected_volumes))
    client.LoginTargets(svip, login_order, target_list)

    # List out volumes and their info
    log.info("Gathering info about connected volumes")
    volumes = client.GetVolumeSummary()
    for _, volume in sorted(volumes.items(), key=lambda vol: vol[1]["iqn"]):
        log.info("   {} -> {}, SID: {}, SectorSize: {}, Portal: {}, State: {}".format(volume["iqn"], volume["device"], volume["sid"], volume["sectors"], volume["portal"], volume["state"]))


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_client_list_args()
    parser.add_argument("--login-order", type=str, choices=sfdefaults.all_login_orders, default=sfdefaults.login_order, help="order to login to volumes")
    parser.add_argument("--auth-type", type=str, choices=sfdefaults.all_auth_types, default=sfdefaults.auth_type, help="how to authenticate to volumes")
    parser.add_argument("--target-list", type=ItemList(), help="the list of targets to log in to, instead of all")
    parser.add_argument("--noclean", dest="clean", action="store_false", default=True, help="do not clean iSCSI before logging in")
    parser.add_account_selection_args(required=False)
    parser.add_cluster_mvip_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientLoginVolumes, args)
    app.Run(**args)
