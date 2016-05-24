#!/usr/bin/env python2.7

"""
This action will clean the iSCSI initiator configuration on a client
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil

@logargs
@ValidateAndDefault({
    "default_iscsid" : (BoolType, True),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
})
def ClientCleanIscsi(default_iscsid,
                     client_ips,
                     client_user,
                     client_pass):
    """
    Clean iSCSI initiator

    Args:
        default_iscsid:     restore the client iscsid.conf to default
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
    """
    log = GetLogger()

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip, client_user, client_pass, default_iscsid))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error cleaning iSCSI: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully cleaned iSCSI on all clients")
        return True
    else:
        log.error("Could not clean iSCSI on all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, default_iscsid):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    log.info("Cleaning iSCSI initiator")
    client.CleanIscsi(default_iscsid)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--nodefault-iscsid", action="store_false", dest="default_iscsid", default=True, help="do not recreate a default iSCSI config file")
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientCleanIscsi, args)
    app.Run(**args)
