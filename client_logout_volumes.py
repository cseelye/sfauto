#!/usr/bin/env python2.7

"""
This action will log out of iSCSI volumes on the clients
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
    # "arg_name" : (arg_type, arg_default)
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "clean" : (BoolType, True),
})
def ClientLogoutVolumes(client_ips,
                        client_user,
                        client_pass,
                        clean):
    """
    Logout of iSCSI volumes on clients

    Args:
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        clean:              clean the iSCSI initator after logging out
    """
    log = GetLogger()

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip,
                                                client_user,
                                                client_pass,
                                                clean))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Failure logging out of volumes: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully logged out of volumes on all clients")
        return True
    else:
        log.error("Could not log out of volumes on all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, clean):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    client.LogoutTargets()
    
    if clean:
        log.info("Cleaning iSCSI")
        client.CleanIscsi()


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_client_list_args()
    parser.add_argument("--noclean", dest="clean", action="store_false", default=True, help="do not clean iSCSI after logging out")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientLogoutVolumes, args)
    app.Run(**args)
