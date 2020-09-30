#!/usr/bin/env python

"""
This action will delete volumes that were made for a client
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, IPv4AddressType, ItemList, BoolType, StrType
from libsf import sfdefaults
from libsf import SolidFireError, UnknownObjectError
from libsf import threadutil

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "purge" : (BoolType, False),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientDeleteVolumes(purge,
                        client_ips,
                        client_user,
                        client_pass,
                        mvip,
                        username,
                        password):
    """
    Delete volumes in accounts corresponding to a list of clients

    Args:
        purge:              purge the volumes after deleting them
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip, client_user, client_pass, mvip, username, password, purge))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully deleted volumes for all clients")
        return True
    else:
        log.error("Could not delete volumes for all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, mvip, username, password, purge):
    """delete the volumes for a client, run as a thread"""
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)
    account_name = client.HostnameToAccountName()

    cluster = SFCluster(mvip, username, password)
    try:
        match_volumes = cluster.SearchForVolumes(accountName=account_name)
    except UnknownObjectError:
        log.passed("Account is already deleted")
        return True

    if len(list(match_volumes.keys())) <= 0:
        log.passed("No volumes to delete")
        return True

    log.info("Deleting {} volumes".format(len(list(match_volumes.keys()))))
    cluster.DeleteVolumes(volumeIDs=list(match_volumes.keys()), purge=purge)
    log.passed("Successfully deleted volumes")

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    parser.add_argument("--purge", action="store_true", default=False, help="purge the volumes after deletion")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientDeleteVolumes, args)
    app.Run(**args)
