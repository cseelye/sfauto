#!/usr/bin/env python2.7

"""
This action will delete a CHAP account on the cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, NameOrID, IPv4AddressType, BoolType, StrType, OptionalValueType, SolidFireIDType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "account_name" : (OptionalValueType(StrType), None),
    "account_id" : (OptionalValueType(SolidFireIDType), None),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def AccountDelete(account_name,
                  account_id,
                  strict,
                  mvip,
                  username,
                  password):
    """
    Create an account

    Args:
        account_name:       the name of the account
        account_id:         the ID of the account
        strict:             fail if the account does not exist
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()
    NameOrID(account_name, account_id, "account")

    cluster = SFCluster(mvip, username, password)

    # See if the account already exists
    log.info("Searching for accounts")
    try:
        account = cluster.FindAccount(accountName=account_name, accountID=account_id)
    except UnknownObjectError:
        # Group does not exist
        if strict:
            log.error("Account does not exists")
            return False
        else:
            log.passed("Account does not exists")
            return True
    except SolidFireError as e:
        log.error("Could not search for accounts: {}".format(e))
        return False

    log.info("Deleting account {}".format(account.username))
    try:
        account.Delete()
    except SolidFireError as e:
        log.error("Failed to delete account: {}".format(e))
        return False

    log.passed("Successfully deleted account {}".format(account_name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_account_selection_args()
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the account does not exist")
    args = parser.parse_args_to_dict()

    app = PythonApp(AccountDelete, args)
    app.Run(**args)
