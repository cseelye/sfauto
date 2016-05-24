#!/usr/bin/env python2.7

"""
This action will remove clients from a volume access group

The action connects to each specified client, queries the iSCSI IQN or FC WWNs, then removes those IDs from the volume access group on the cluster
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, SetThreadLogPrefix, logargs
from libsf.sfclient import SFClient
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, NameOrID, SelectionType, StrType, SolidFireIDType, OptionalValueType
from libsf import sfdefaults
from libsf import threadutil
from libsf import SolidFireError
from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "client_ips" : (ItemList(IPv4AddressType), sfdefaults.client_ips),
    "client_user" : (StrType, sfdefaults.client_user),
    "client_pass" : (StrType, sfdefaults.client_pass),
    "volgroup_name" : (OptionalValueType(StrType), None),
    "volgroup_id" : (SolidFireIDType, None),
    "connection_type" : (SelectionType(sfdefaults.all_client_connection_types, StrType), sfdefaults.connection_type),
    "mvip" : (IPv4AddressType, sfdefaults.mvip),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
})
def ClientRemoveFromVolgroup(client_ips,
                              client_user,
                              client_pass,
                              volgroup_name,
                              volgroup_id,
                              connection_type,
                              mvip,
                              username,
                              password):
    """
    Remove the specified clients from the specified volume access group

    Args:
        client_ips:         the list of client IP addresses
        client_user:        the username for the clients
        client_pass:        the password for the clients
        volgroup_name:      the name of the group
        volgroup_id:        the ID of the group
        connection_type:    the type of volume connection (iSCSI or FC)
        mvip:               the management IP of the cluster
        username:           the admin user of the cluster
        password:           the admin password of the cluster
    """
    log = GetLogger()
    NameOrID(volgroup_name, volgroup_id, "volume group")
    connection_type = connection_type.lower()

    id_type = "IQN"
    if connection_type == 'fc':
        id_type = "WWN"

    # Launch a thread for each client to go get the IQNs from each
    log.info("Getting a list of client {}".format(id_type))
    pool = threadutil.GlobalPool()
    results = []
    for client_ip in client_ips:
        results.append(pool.Post(_ClientThread, client_ip, client_user, client_pass, connection_type))

    allgood = True
    remove_ids = []
    for idx, client_ip in enumerate(client_ips):
        # Get the result for this thread
        try:
            client_ids = results[idx].Get()
        except SolidFireError as e:
            log.error("  {}: Could not get {}: {}".format(client_ip, id_type, e))
            allgood = False
            continue

        # Check for duplicates and add the IDs to the list
        log.info("{} has {} {}".format(client_ip, id_type, ",".join(client_ids)))
        if not set(client_ids).isdisjoint(remove_ids):
            log.error("Duplicate {}".format(id_type))
            return False
        remove_ids += client_ids

    if not allgood:
        log.error("Could not get {}s from all clients".format(id_type))
        return False

    # Remove the IDs from the group
    return RemoveInitiatorsFromVolgroup(initiators=remove_ids,
                                        volgroup_name=volgroup_name,
                                        volgroup_id=volgroup_id,
                                        strict=True,
                                        mvip=mvip,
                                        username=username,
                                        password=password)

@threadutil.threadwrapper
def _ClientThread(client_ip, client_user, client_pass, connection_type):
    """Connect to the client and get the IQN/WWN, run as a thread"""
    log = GetLogger()
    SetThreadLogPrefix(client_ip)
    log.info("Connecting to client")
    return SFClient(client_ip, client_user, client_pass).GetInitiatorIDs(connection_type)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_cluster_mvip_args()
    parser.add_client_list_args()
    parser.add_volgroup_selection_args()
    parser.add_argument("--type", type=str, dest="connection_type", choices=sfdefaults.all_client_connection_types, default=sfdefaults.connection_type, help="the type of volume connection")
    args = parser.parse_args_to_dict()

    app = PythonApp(ClientRemoveFromVolgroup, args)
    app.Run(**args)
