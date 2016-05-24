#!/usr/bin/env python2.7

"""
This action will create a CHAP account on the cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, BoolType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "account_name" : (StrType, None),
    "initiator_secret" : (OptionalValueType(StrType), None),
    "target_secret" : (OptionalValueType(StrType), None),
    "strict" : (BoolType, False),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def AccountCreate(account_name,
                  initiator_secret,
                  target_secret,
                  strict,
                  mvip,
                  username,
                  password):
    """
    Create an account

    Args:
        account_name:       the name for the new account
        initiator_secret:   the initiator CHAP secret
        target_secret:      the target CHAP secret
        strict:             fail if the account already exists
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    # See if the account already exists
    log.info("Searching for accounts")
    try:
        cluster.FindAccount(accountName=account_name)
        if strict:
            log.error("Account already exists")
            return False
        else:
            log.passed("Account already exists")
            return True
    except UnknownObjectError:
        # Group does not exist
        pass
    except SolidFireError as e:
        log.error("Could not search for accounts: {}".format(e))
        return False

    log.info("Creating account '{}'".format(account_name))
    try:
        cluster.CreateAccount(account_name, initiator_secret, target_secret)
    except SolidFireError as e:
        log.error("Failed to create account: {}".format(e))
        return False

    log.passed("Successfully created account {}".format(account_name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_argument("--account-name", type=str, required=True, metavar="NAME", help="the name for the new account")
    parser.add_argument("--init-secret", type=str, help="the initiator secret for the account")
    parser.add_argument("--targ-secret", type=str, help="the target secret for the account")
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the account already exists")
    args = parser.parse_args_to_dict()

    app = PythonApp(AccountCreate, args)
    app.Run(**args)
