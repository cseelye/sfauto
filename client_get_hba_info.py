#!/usr/bin/env python

"""
This action will show the FC HBA info from the clients
"""
from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, StrType
from libsf import sfdefaults
from libsf import SolidFireError
from libsf import threadutil

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
})
def ClientGetHBAInfo(client_ips,
                     client_user,
                     client_pass):
    """
    Get FC HBA infor form clients

    Args:
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
        results.append(pool.Post(_ClientThread, client_ip,
                                                client_user,
                                                client_pass))

    hba_info = {}
    for idx, client_ip in enumerate(client_ips):
        try:
            hba_info[client_ip] = results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Failure querying client: {}".format(client_ip, e))
            allgood = False
            continue

    # Display the results from all of the clients
    for client_ip in client_ips:
        hbas = hba_info[client_ip]
        log.info("{} has {} FC HBAs".format(client_ip, len(hbas)))
        for host in sorted(hbas.keys()):
            hba = hbas[host]
            log.info("  {}  {}  {}  {}  {}".format(host, hba["desc"], hba["wwn"], hba["link"], hba["speed"]))
            for targ in hba["targets"]:
                log.info("    Target {}".format(targ))

    if not allgood:
        log.error("Could not get HBA info from all clients")
        return False

    return True


@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass):
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    return client.GetHBAInfo()


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_client_list_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientGetHBAInfo, args)
    app.Run(**args)
