#!/usr/bin/env python

"""
This action will delete the CHAP account that corresponds to the specified clients
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, SolidFireAPIError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "account_name" : (OptionalValueType(StrType), None),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientDeleteAccount(account_name,
                        strict,
                        client_ips,
                        client_user,
                        client_pass,
                        mvip,
                        username,
                        password):
    """
    Delete the account for each client

    Args:
        account_name:   the name of the account, client hostname is used if this is not specified
        strict:         fail if the account does not exist
        client_ips:     the list of client IP addresses
        client_user:    the username for the clients
        client_pass:    the password for the clients
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, mvip, username, password, client_ip, client_user, client_pass, account_name, strict))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error deleting group: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully deleted accounts for all clients")
        return True
    else:
        log.error("Could not delete accounts for all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(mvip, username, password, client_ip, client_user, client_pass, account_name, strict):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    if not account_name:
        account_name = client.HostnameToAccountName()
    log.debug("Using account name {}".format(account_name))

    # Find the account
    try:
        account = SFCluster(mvip, username, password).FindAccount(accountName=account_name)
    except UnknownObjectError:
        if strict:
            raise SolidFireError("Account {} does not exist".format(account_name))
        else:
            log.passed("Account {} does not exist".format(account_name))
            return True

    log.info("Purging deleted volumes from account {}".format(account_name))
    try:
        account.PurgeDeletedVolumes()
    except SolidFireError as e:
        raise SolidFireError("Failed to delete account: {}".format(e))

    # Delete the account
    log.info("Deleting account {}".format(account_name))
    try:
        account.Delete()
    except SolidFireAPIError as e:
        # Ignore xAccountIDDoesNotExist; we may have multiple threads trying to delete the same account
        if e.name != "xAccountIDDoesNotExist":
            raise

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--account-name", type=str, metavar="NAME", help="the name for the account (client hostname is used if this is not specified)")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the account already exists")
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientDeleteAccount, args)
    app.Run(**args)
