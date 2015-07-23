#!/usr/bin/env python2.7

"""
This action will log in to iSCSI volumes by VLAN on a list of clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --target_list       List of target IQNs to log in to

    --paralell_thresh   Do not thread clients unless there are more than this many
    SFPARALLEL_THRESH env var

    --parallel_max       Max number of client threads to use
    SFPARALLEL_MAX env var
"""

import sys
from optparse import OptionParser
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient, OsType
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class LoginClientVlansAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, client_ip, client_user, client_pass, mvip, username, password, all_vlans, all_volgroups, all_volumes, results):
        myname = multiprocessing.current_process().name
        results[myname] = False

        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            return

        # Find the volume access groups that contain a derivative of this client's IQN
        mylog.info(client_ip + ": Searching for client volume groups")
        client_iqn = client.GetInitiatorName()
        volgroups = []
        for group in all_volgroups:
            for init in group['initiators']:
                if init.startswith(client_iqn):
                    volgroups.append(group)
                    break
        if len(volgroups) == 0:
            mylog.warning(client_ip + ": No appropriate volume groups found")
            results[myname] = True
            return

        # For each volgroup/VLAN, create an appropriate iSCSI iface, discover the volumes, then log in
        for group in volgroups:

            # Get the info for this connection
            vlan_iqn = group['initiators'][0]
            tag = int(vlan_iqn.split(":")[-1].split("-")[-1])
            vlan_targets = [all_volumes[i]['iqn'] for i in group['volumes']]
            vlan_portal = all_vlans[tag]['svip']

            # Create iface and discover on this connection
            mylog.info("{}: Creating iSCSI iface for VLAN {}".format(client_ip, tag))
            iface_name = "vlan-{}".format(tag)
            client.CreateIscsiIface(iface_name, initiatorName=vlan_iqn)
            mylog.info("{}: Discovering volumes on VLAN {}".format(client_ip, tag))
            client.RefreshTargets(vlan_portal, ifaceName=iface_name)

        mylog.info("{}: Logging in to all volumes".format(client_ip))
        client.LoginTargets(pLoginOrder="parallel")

        results[myname] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            "mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            },
            args)

    def Execute(self, mvip, client_ips=None, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Log in to volumes on clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get a list of VLANs in the cluster
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListVirtualNetworks", {}, ApiVersion=8.0)
        except libsf.SfError as e:
            mylog.error(client_ip + ": Failed to get volgroup list - " + str(e))
            return
        all_vlans = {v['virtualNetworkTag'] : v for v in result['virtualNetworks']}

        # Get a list of volume groups in the cluster
        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListVolumeAccessGroups", {})
        except libsf.SfError as e:
            mylog.error(client_ip + ": Failed to get volgroup list - " + str(e))
            return
        all_volgroups = result['volumeAccessGroups']

        try:
            result = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        except libsf.SfError as e:
            mylog.error(client_ip + ": Failed to get volume list - " + str(e))
            return
        all_volumes = {v['volumeID'] : v for v in result['volumes']}

        # Run the client operations in parallel if there are enough clients
        if len(client_ips) <= parallel_thresh:
            parallel_clients = 1
        else:
            parallel_clients = parallel_max

        # Start the client threads
        manager = multiprocessing.Manager()
        results = manager.dict()
        all_threads = []
        for client_ip in client_ips:
            thread_name = "client-" + client_ip
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ClientThread, name=thread_name, args=(client_ip, client_user, client_pass, mvip, username, password, all_vlans, all_volgroups, all_volumes, results))
            th.daemon = True
            all_threads.append(th)

        allgood = libsf.ThreadRunner(all_threads, results, parallel_clients)

        if allgood:
            mylog.passed("Successfully logged in to volumes on all clients")
            return True
        else:
            mylog.error("Could not log in to all volumes on all clients")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.client_ips, options.username, options.password, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

