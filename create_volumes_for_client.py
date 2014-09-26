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

    --volume_prefix     Prefix for the volumes to create

    --volume_count      The number of volumes to create

    --volume_size       The size of the volumes, in GB

    --volume_start      The volume number to start from

    --512e              Use 512e

    --max_iops          QoS maxIOPS

    --min_iops          QoS minIOPS

    --burst_iops        QoS burstIOPS

    --account_name      Name of the account to create the volumes for

    --account_id        ID of the account to create the volumes for

    --wait              How long to pause between creating each volume

    --paralell_thresh   Do not thread clients unless there are more than this many
    SFPARALLEL_THRESH env var

    --parallel_max       Max number of client threads to use
    SFPARALLEL_MAX env var
"""

import sys
from optparse import OptionParser
import time
import re
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
import lib.libclient as libclient
from lib.libclient import ClientError, SfClient
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CreateVolumesForClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, client_ip, client_user, client_pass, accounts_list, mvip, username, password, volume_size, volume_count, enable_512, min_iops, max_iops, burst_iops, wait, results, index):
        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            return

        if client.RemoteOs == libclient.OsType.Windows and not enable_512:
            mylog.warning("512e not enabled - this may cause Windows problems!")

        # Find the corresponding account on the cluster
        account_name = client.Hostname
        account_id = 0
        for account in accounts_list["accounts"]:
            if account["username"].lower() == account_name.lower():
                account_id = account["accountID"]
                break
        if account_id == 0:
            mylog.error(client_ip + ": Could not find account " + client.Hostname + " on " + mvip)
            return

        # See if there are existing volumes
        volume_start = 0
        params = {}
        params["accountID"] = account_id
        try:
            volumes_list = libsf.CallApiMethod(mvip, username, password, "ListVolumesForAccount", params)
        except libsf.SfError as e:
            mylog.error("Failed to get volume list: " + str(e))
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return False
        for vol in volumes_list["volumes"]:
            m = re.search(r"(\d+)$", vol["name"])
            if m:
                vol_num = int(m.group(1))
                if vol_num > volume_start:
                    volume_start = vol_num
        volume_start += 1

        # Create the requested volumes
        mylog.info(client_ip + ": Creating " + str(volume_count) + " volumes for account " + account_name)
        # This split makes sure there are no "." in the volume prefix
        volume_prefix = client.Hostname.lower().split(".")[0] + "-v"
        for vol_num in range(volume_start, volume_start + volume_count):
            volume_name = volume_prefix + "%05d" % vol_num
            params = {}
            params["name"] = volume_name
            params["accountID"] = account_id
            params["totalSize"] = int(volume_size * 1000 * 1000 * 1000)
            params["enable512e"] = enable_512
            qos = {}
            qos["maxIOPS"] = max_iops
            qos["minIOPS"] = min_iops
            qos["burstIOPS"] = burst_iops
            params["qos"] = qos
            try:
                libsf.CallApiMethod(mvip, username, password, "CreateVolume", params)
            except libsf.SfError as e:
                mylog.error("Failed to create volume: " + str(e))
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return False

            mylog.info(client_ip + ":   Created volume " + volume_name)
            if (wait > 0):
                time.sleep(wait)

        mylog.passed(client_ip + ": Successfully created " + str(volume_count) + " volumes for " + client.Hostname)
        results[index] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "volume_size" : None,
                            "volume_count" : None,
                            "min_iops" : libsf.IsInteger,
                            "max_iops" : libsf.IsInteger,
                            "burst_iops" : libsf.IsInteger,
                            "client_ips" : libsf.IsValidIpv4AddressList
                            },
                    args)

    def Execute(self, volume_size, volume_count, mvip=sfdefaults.mvip, client_ips=None, enable_512=True, min_iops=100, max_iops=100000, burst_iops=100000, wait=0, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Create volumes for clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

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
            th = multiprocessing.Process(target=self._ClientThread, args=(client_ip, client_user, client_pass, accounts_list, mvip, username, password, volume_size, volume_count, enable_512, min_iops, max_iops, burst_iops, wait, results, thread_index))
            th.daemon = True
            #th.start()
            self._threads.append(th)
            thread_index += 1

        allgood = libsf.ThreadRunner(self._threads, results, parallel_clients)

        if allgood:
            mylog.passed("Successfully created volumes for all clients")
            return True
        else:
            mylog.error("Could not create volumes for all clients")
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
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster  [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster  [%default]")
    parser.add_option("--volume_count", type="int", dest="volume_count", default=None, help="the number of volumes to create")
    parser.add_option("--volume_size", type="int", dest="volume_size", default=None, help="the volume size in GB")
    parser.add_option("--max_iops", type="int", dest="max_iops", default=100000, help="the max sustained IOPS to allow on this volume  [%default]")
    parser.add_option("--min_iops", type="int", dest="min_iops", default=100, help="the min sustained IOPS to guarentee on this volume  [%default]")
    parser.add_option("--burst_iops", type="int", dest="burst_iops", default=100000, help="the burst IOPS to allow on this volume  [%default]")
    parser.add_option("--512e", action="store_true", dest="enable_512", default=False, help="use 512 sector emulation")
    parser.add_option("--wait", type="int", dest="wait", default=0, help="how long to wait between creating each volume (seconds) [%default]")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.volume_size, options.volume_count, options.mvip, options.client_ips, options.enable_512, options.min_iops, options.max_iops, options.burst_iops, options.wait, options.username, options.password, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
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
