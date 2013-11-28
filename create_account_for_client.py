#!/usr/bin/python

"""
This action will create a CHAP account on the cluster for each client and configure the client with the CHAP credentials

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --account_name      The name of the account to create

    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --paralell_thresh   Do not thread clients unless there are more than this many
    SFPARALLEL_THRESH env var

    --parallel_max       Max number of client threads to use
    SFPARALLEL_MAX env var
"""

import sys
from optparse import OptionParser
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog, SfApiError
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateAccountForClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, mvip, username, password, client_ip, client_user, client_pass, account_name, svip, accounts_list, results, index):
        mylog.info(client_ip + ": Connecting to client")
        client = SfClient()
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        if not account_name:
            account_name = client.Hostname
        mylog.debug(client_ip + ": Using account name " + account_name)

        # See if the account already exists
        init_secret = ""
        found = False
        for account in accounts_list["accounts"]:
            if account["username"].lower() == account_name.lower():
                init_secret = account["initiatorSecret"]
                found = True
                break

        if found:
            mylog.info(client_ip + ": Account " + account_name + " already exists.")
        else:
            # Create the account
            mylog.info(client_ip + ": Creating account '" + account_name + "'")
            init_secret = libsf.MakeSimpleChapSecret()
            target_secret = libsf.MakeSimpleChapSecret()
            params = {}
            params["username"] = account_name.lower()
            params["initiatorSecret"] = init_secret
            params["targetSecret"] = target_secret
            try:
                libsf.CallApiMethod(mvip, username, password, "AddAccount", params, ExitOnError=False)
            except SfApiError as e:
                # Ignore duplicate error, fail on all others
                if e.name != "xDuplicateUsername":
                    mylog.error(client_ip + ": Error " + e.name + ": " + e.message)
                    self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                    return

        # set the CHAP credentials on the client
        mylog.info(client_ip + ": Setting CHAP credentials on " + client.Hostname)
        try:
            client.SetupChap(svip, account_name.lower(), init_secret)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return

        self.SetSharedValue(SharedValues.accountName, account_name)
        self.SetSharedValue(client_ip + "-accountName", account_name)

        results[index] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "client_ips" : libsf.IsValidIpv4AddressList,
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, client_ips=None, account_name=None, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Create an account for each client
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Getting information from the cluster")
        # Get the SVIP of the cluster
        try:
            cluster_info = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster info: " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        svip = cluster_info["clusterInfo"]["svip"]

        # Get a list of accounts from the cluster
        try:
            accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        except libsf.SfError as e:
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
        self._threads = []
        thread_index = 0
        for client_ip in client_ips:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._ClientThread, args=(mvip, username, password, client_ip, client_user, client_pass, account_name, svip, accounts_list, results, thread_index))
            th.daemon = True
            #th.start()
            self._threads.append(th)
            thread_index += 1

        allgood = libsf.ThreadRunner(self._threads, results, parallel_clients)

        if allgood:
            mylog.passed("Successfully created accounts for all clients")
            return True
        else:
            mylog.error("Could not create accounts for all clients")
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
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the account name to use instead of the client hostname. This will be used for every client!")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.client_ips, options.account_name, options.username, options.password, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
