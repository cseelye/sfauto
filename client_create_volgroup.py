#!/usr/bin/env python2.7

"""
This action will create a volume group on the cluster for each client
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.sfcluster import SFCluster
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, BoolType, StrType, OptionalValueType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError, SolidFireAPIError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "strict" : (BoolType, False),
    "volgroup_name" : (OptionalValueType(StrType), None),
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientCreateVolgroup(strict,
                         volgroup_name,
                         client_ips,
                         client_user,
                         client_pass,
                         mvip,
                         username,
                         password):
    """
    Create a volume group for each client

    Args:
        volgroup_name:  the name of the group, client hostname is used if this is not specified
        strict:         fail if the group already exists
        client_ips:     the list of client IP addresses
        client_user:    the username for the clients
        client_pass:    the password for the clients
        mvip:           the management IP of the cluster
        username:       the admin user of the cluster
        password:       the admin password of the cluster
    """
    log = GetLogger()

    cluster = SFCluster(mvip, username, password)

    log.info("Searching for volume groups")
    try:
        allgroups = cluster.ListVolumeAccessGroups()
    except SolidFireError as e:
        log.error("Failed to list groups: {}".format(e))
        return False

    # Run all of the client operations in parallel
    allgood = True
    results = []
    pool = threadutil.GlobalPool()
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, mvip, username, password, client_ip, client_user, client_pass, strict, volgroup_name, allgroups))

    for idx, client_ip in enumerate(client_ips):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Error creating group: {}".format(client_ip, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully created groups for all clients")
        return True
    else:
        log.error("Could not create groups for all clients")
        return False

@threadutil.threadwrapper
def _ClientThread(mvip, username, password, client_ip, client_user, client_pass, strict, volgroup_name, allgroups):
    """Create the volgroup for a single client, run as a thread"""
    log = GetLogger()
    SetThreadLogPrefix(client_ip)

    log.info("Connecting to client")
    client = SFClient(client_ip, client_user, client_pass)

    if not volgroup_name:
        volgroup_name = client.HostnameToAccountName()
    log.debug("Using volgroup name {}".format(volgroup_name))

    # See if the group already exists
    client_group = None
    for group in allgroups:
        if group.name == volgroup_name:
            if strict:
                raise SolidFireError("Group {} already exists".format(volgroup_name))
            else:
                client_group = group
                break

    # Get the client IQN
    iqn = client.GetInitiatorName()

    cluster = SFCluster(mvip, username, password)

    # Create the group if it does not exist
    if not client_group:
        try:
            client_group = cluster.CreateVolumeGroup(volgroup_name)
        except SolidFireAPIError as e:
            # Ignore xDuplicateUsername; we may have multiple threads trying to create the same group
            if e.name != "xDuplicateUsername":
                raise
            client_group = cluster.FindVolumeAccessGroup(volgroupName=volgroup_name)

    # Create/modify the group
    if iqn not in client_group.initiators:
        log.info("Adding initiator to group {}".format(volgroup_name))
        client_group.AddInitiators([iqn])
    else:
        log.passed("Group {} already exists with initiator".format(volgroup_name))

    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    parser.add_argument("--strict", action="store_true", default=False, help="fail if the group already exists")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientCreateVolgroup, args)
    app.Run(**args)
