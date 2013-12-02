#!/usr/bin/python

"""
This action will create volumes for an account

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

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
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class DeleteVolumesFromClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, client_ip, client_user, client_pass, mvip, username, password, accounts_list, results, index, debug):
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        # Connect to the client
        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        # Search for account name and volumes
        account_id = 0
        volume_ids = None
        account_id = 0
        for account in accounts_list["accounts"]:
            if (account["username"].lower() == client.Hostname.lower()):
                account_id = account["accountID"]
                volume_ids = account["volumes"]
                break

        if (account_id <= 0):
            mylog.info(client_ip + ": There is no account with name '" + client.Hostname.lower() + "'")
            mylog.info(client_ip + ": " + client.Hostname + " has no volumes to delete")
            results[index] = True
            return
        if not volume_ids:
            mylog.info(client_ip + ": " + client.Hostname + " has no volumes to delete")
            results[index] = True
            return

        mylog.info(client_ip + ": Deleting/purging " + str(len(volume_ids)) + " volumes from account " + client.Hostname)

        # Delete the requested volumes
        for vol_id in volume_ids:
            params = {}
            params["volumeID"] = vol_id
            try:
                libsf.CallApiMethod(mvip, username, password, "DeleteVolume", params)
            except libsf.SfError as e:
                mylog.error(client_ip + ": failed to delete volumes: " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return

            try:
                libsf.CallApiMethod(mvip, username, password, "PurgeDeletedVolume", params)
            except libsf.SfError as e:
                mylog.error(client_ip + ": failed to purge volumes: " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return

        results[index] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "client_ips" : libsf.IsValidIpv4AddressList
                            },
                    args)

    def Execute(self, mvip=sfdefaults.mvip, client_ips=None, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Delete all of the volumes from a list of clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Get a list of accounts from the cluster
        try:
            accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        except libsf.SfError as e:
            mylog.error("Failed to get account list: " + str(e))
            return False

        # Run the client operations in parallel if there are enough clients
        if len(client_ips) <= parallel_thresh:
            parallel_clients = 1
        else:
            parallel_clients = parallel_max

        # Start the client threads
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        thread_index = 0
        for client_ip in client_ips:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._ClientThread, args=(client_ip, client_user, client_pass, mvip, username, password, accounts_list, results, thread_index, debug))
            self._threads.append(th)
            thread_index += 1

        allgood = libsf.ThreadRunner(self._threads, results, parallel_clients)

        if allgood:
            mylog.passed("Successfully deleted volumes for all clients")
            return True
        else:
            mylog.error("Could not delete volumes for all clients")
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
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(mvip=options.mvip, client_ips=options.client_ips, username=options.username, password=options.password, client_user=options.client_user, client_pass=options.client_pass, parallel_thresh=options.parallel_thresh, parallel_max=options.parallel_max, debug=options.debug):
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
    sys.exit(0)
