#!/usr/bin/python

"""
This action will log in to all iSCSI volumes on a list of clients

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

    --login_order       How to log in to volumes - all at the same time [parallel] or or seqentially [serial] in order by iqn

    --auth_type         iSCSI auth type - CHAP or None

    --account_name      Specifu a CHAP account name instead of using the client hostname

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

class LoginClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ALL = "BEFORE_ALL"
        AFTER_ALL = "AFTER_ALL"
        BEFORE_CLIENT_LOGIN = "BEFORE_CLIENT_LOGIN"
        AFTER_CLIENT_LOGIN = "AFTER_CLIENT_LOGIN"
        BEFORE_CLIENT_CLEAN = "BEFORE_CLIENT_CLEAN"
        AFTER_CLIENT_CLEAN = "AFTER_CLIENT_CLEAN"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, client_ip, client_user, client_pass, account_name, target_list, auth_type, mvip, username, password, svip, login_order, accounts_list, results):
        myname = multiprocessing.current_process().name
        results[myname] = False

        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        # Clean iSCSI if there aren't already volumes logged in
        targets = []
        try:
            targets = client.GetLoggedInTargets()
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        if not targets or len(targets) <= 0:
            self._RaiseEvent(self.Events.BEFORE_CLIENT_CLEAN, clientIP=client_ip)
            try:
                mylog.info(client_ip + ": Cleaning iSCSI on client '" + client.Hostname + "'")
                client.CleanIscsi()
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                return
            self._RaiseEvent(self.Events.AFTER_CLIENT_CLEAN, clientIP=client_ip)

        expected = 0
        if auth_type.lower() == "chap":
            if not account_name:
                account_name = client.Hostname
            mylog.debug(client_ip + ": Using account name " + account_name)


            # See if there is an sf account with this name already created
            mylog.info(client_ip + ": Looking for account '" + account_name + "' on cluster '" + mvip + "'")
            init_password = None
            account_id = None
            for account in accounts_list["accounts"]:
                if (account["username"].lower() == account_name.lower()):
                    account_id = account["accountID"]
                    init_password = account["initiatorSecret"]
                    expected = len(account["volumes"])
                    break

            # If this is a Windows client, make sure the CHAP secret is alphanumeric
            if account_id and client.RemoteOs == OsType.Windows:
                if not init_password.isalnum():
                    init_password = libsf.MakeSimpleChapSecret()
                    mylog.info(client_ip + ": Resetting CHAP password to '" + init_password + "'")
                    params = {}
                    params["accountID"] = account_id
                    params["initiatorSecret"] = init_password
                    params["targetSecret"] = libsf.MakeSimpleChapSecret()
                    try:
                        result = libsf.CallApiMethod(mvip, username, password, "ModifyAccount", params)
                    except libsf.SfError as e:
                        mylog.error(client_ip + ": Failed to modify account for client - " + str(e))
                        self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                        return

            # Create an account if we couldn't find one
            if (account_id == None):
                mylog.info("Creating new account on cluster " + mvip)
                params = {}
                params["username"] = account_name.lower()
                params["initiatorSecret"] = libsf.MakeSimpleChapSecret()
                params["targetSecret"] = libsf.MakeSimpleChapSecret()
                try:
                    result = libsf.CallApiMethod(mvip, username, password, "AddAccount", params)
                except libsf.SfError as e:
                    mylog.error(client_ip + ": Failed to create account for client - " + str(e))
                    self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                    return
                account_id = result["accountID"]
                params = {}
                params["accountID"] = account_id
                try:
                    result = libsf.CallApiMethod(mvip, username, password, "GetAccountByID", params)
                except libsf.SfError as e:
                    mylog.error(client_ip + ": Failed to find account for client - " + str(e))
                    self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                    return
                init_password = result["account"]["initiatorSecret"]

            mylog.info(client_ip + ": Using account " + str(account_id) + " with password '" + init_password + "'")

            # Add the CHAP credentials to the client
            mylog.info(client_ip + ": Setting up iSCSI CHAP on client '" + client.Hostname + "'")
            try:
                client.SetupChap(svip, account_name.lower(), init_password)
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return
        else:
            iqn = client.GetInitiatorName()
            for vag in accounts_list["volumeAccessGroups"]:
                if iqn in vag["initiators"]:
                    expected += len(vag["volumes"])

        # Do an iSCSI discovery
        mylog.info(client_ip + ": Discovering iSCSI volumes on client '" + client.Hostname + "' at VIP '" + svip + "'")
        try:
            client.RefreshTargets(svip, expected)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        # Log in to all volumes
        mylog.info(client_ip + ": Logging in to iSCSI volumes on client '" + client.Hostname + "'")
        try:
            client.LoginTargets(svip, login_order, target_list)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        # List out the volumes and their info
        mylog.info(client_ip + ": Gathering information about connected volumes...")
        volumes = client.GetVolumeSummary()
        if volumes:
            for device, volume in sorted(volumes.iteritems(), key=lambda (k, v): v["iqn"]):
                if "sid" not in volume.keys():
                    volume["sid"] = "unknown"
                outstr = "   " + volume["iqn"] + " -> " + volume["device"] + ", SID: " + volume["sid"] + ", SectorSize: " + volume["sectors"] + ", Portal: " + volume["portal"]
                if "state" in volume:
                    outstr += ", Session: " + volume["state"]
                mylog.info(outstr)

        results[myname] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            "login_order" : lambda x: x in sfdefaults.all_login_orders,
                            "auth_type" : lambda x: x in sfdefaults.all_auth_types,
                            "mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            },
            args)

    def Execute(self, mvip, client_ips=None, login_order=sfdefaults.login_order, auth_type=sfdefaults.auth_type, account_name=None, target_list=None, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Log in to volumes on clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Get the SVIP of the cluster
        try:
            cluster_info = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        except libsf.SfApiError as e:
            mylog.error("Failed to get cluster info: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        svip = cluster_info["clusterInfo"]["svip"]

        # Get a list of accounts/VAGs from the cluster
        try:
            if auth_type == "chap":
                accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
            else:
                accounts_list = libsf.CallApiMethod(mvip, username, password, "ListVolumeAccessGroups", {}, ApiVersion=5.0)
        except libsf.SfApiError as e:
            mylog.error("Failed to get account list: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

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
            th = multiprocessing.Process(target=self._ClientThread, name=thread_name, args=(client_ip, client_user, client_pass, account_name, target_list, auth_type, mvip, username, password, svip, login_order, accounts_list, results))
            th.daemon = True
            all_threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_ALL)
        allgood = libsf.ThreadRunner(all_threads, results, parallel_clients)
        self._RaiseEvent(self.Events.AFTER_ALL)

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
    parser.add_option("--login_order", type="choice", choices=sfdefaults.all_login_orders, dest="login_order", default=sfdefaults.login_order, help="login order for volumes (parallel or serial)")
    parser.add_option("--auth_type", type="choice", choices=sfdefaults.all_auth_types, dest="auth_type", default=sfdefaults.auth_type, help="iSCSI auth type (chap or none)")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the CHAP account name to use instead of the client hostname. This will be used for every client!")
    parser.add_option("--target_list", action="list", dest="target_list", default=None, help="the list of volume IQNs to log in to, instead of all volumes")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.client_ips, options.login_order, options.auth_type, options.account_name, options.target_list, options.username, options.password, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
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

