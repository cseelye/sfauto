#!/usr/bin/env python

"""
This action will create a CHAP account on the cluster for each client and configure the client with the CHAP credentials
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.sfcluster import SFCluster
from libsf.sfaccount import SFAccount
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, SolidFireAPIError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "account_name" : (OptionalValueType(StrType), None),
    "chap" : (BoolType, False),
    "strict" : (BoolType, False),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientCreateAccount(account_name,
                        chap,
                        strict,
                        client_ips,
                        client_user,
                        client_pass,
                        mvip,
                        username,
                        password):
    """
    Create an account for each client

    Args:
        account_name:   the name of the account, client hostname is used if this is not specified
        chap:           whether or not to configure CHAP on the clients
        strict:         fail if the account already exists
        client_ips:     the list of client IP addresses
        client_user:    the username for the clients
        client_pass:    the password for the clients
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    log.info("Searching for accounts")
    cluster = SFCluster(mvip, username, password)

    try:
        svip = cluster.GetClusterInfo()["svip"]
    except SolidFireError as e:
        log.error("Failed to get cluster info: {}".format(e))
        return False

    # Get a list of accounts from the cluster
    try:
        allaccounts = SFCluster(mvip, username, password).ListAccounts()
    except SolidFireError as e:
        log.error("Failed to list accounts: {}".format(e))
        return False

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, mvip, username, password, client_ip, client_user, client_pass, account_name, svip, allaccounts, chap, strict))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error creating account: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully created accounts for all clients")
        return True
    else:
        log.error("Could not create accounts for all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(mvip, username, password, client_ip, client_user, client_pass, account_name, svip, accounts_list, chap, strict):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    if not account_name:
        account_name = client.HostnameToAccountName()
    log.debug("Using account name {}".format(account_name))

    # See if the account already exists
    init_secret = ""
    found = False
    for account in accounts_list:
        if account["username"].lower() == account_name.lower():
            init_secret = account["initiatorSecret"]
            found = True
            break

    if found:
        if strict:
            raise SolidFireError("Account {} already exists".format(account_name))
        else:
            log.passed("Account {} already exists".format(account_name))
    else:
        # Create the account
        log.info("Creating account {}".format(account_name))
        try:
            account = SFCluster(mvip, username, password).CreateAccount(accountName=account_name,
                                                                        initiatorSecret=SFAccount.CreateCHAPSecret(),
                                                                        targetSecret=SFAccount.CreateCHAPSecret())
        except SolidFireAPIError as e:
            # Ignore xDuplicateUsername; we may have multiple threads trying to create the same account
            if e.name != "xDuplicateUsername":
                raise
        log.passed("Created account {}".format(account_name))

    if chap:
        log.info("Setting CHAP credentials")
        client.SetupCHAP(portalAddress=svip,
                         chapUser=account_name.lower(),
                         chapSecret=init_secret)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--account-name", type=str, metavar="NAME", help="the name for the account (client hostname is used if this is not specified)")
    parser.add_argument("--nochap", action="store_false", dest="chap", default=True, help="do not configure CHAP on the clients")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the account already exists")
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientCreateAccount, args)
    app.Run(**args)
