#!/usr/bin/env python

"""
This action will verify the number of connected volumes on a client
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, IPv4AddressType, PositiveIntegerType, ItemList, StrType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil


@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "expected" : (PositiveIntegerType, None),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
})
def ClientVerifyVolumeCount(expected,
                            client_ips,
                            client_user,
                            client_pass):
    """
    Count connected volumes

    Args:
        expected:           the expected number of volumes
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
        results.append(pool.Post(_ClientThread, client_ip, client_user, client_pass, expected))

    for idx, client_ip in enumerate(client_ips):
        try:
            success = results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error getting volume info: {}".format(client_ip, e))
            allgood = False
            continue
        if not success:
            allgood = False

    if allgood:
        log.passed("All clients have the expected number of volumes")
        return True
    else:
        log.error("Not all clients have the expected number of volumes")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, expected):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    volume_count = len(client.GetLoggedInTargets())
    if volume_count == expected:
        log.passed("Found {} volumes".format(expected))
        return True
    else:
        log.error("Found {} volumes but expected {} volumes".format(volume_count, expected))
        return False

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--expected", type=PositiveIntegerType, required=True, metavar="COUNT", help="the number of volumes to expect")
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientVerifyVolumeCount, args)
    app.Run(**args)
