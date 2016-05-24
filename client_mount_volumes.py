#!/usr/bin/env python2.7

"""
This action will partition, format, mount iSCSI volumes on a list of clients
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, StrType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
})
def ClientMountVolumes(client_ips,
                       client_user,
                       client_pass):
    """
    Format/mount the iSCSI volumes connected to the clients

    Args:
        client_ips:     the list of client IP addresses
        client_user:    the username for the clients
        client_pass:    the password for the clients
    """
    log = GetLogger()

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip, client_user, client_pass))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error mounting volumes: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully mounted volumes on all clients")
        return True
    else:
        log.error("Could not mount volumes on all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    client.SetupVolumes()


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientMountVolumes, args)
    app.Run(**args)
