#!/usr/bin/python

"""
This action will log out of volumes and remove persistent iSCSI configuration information.

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --paralell_thresh   Do not thread clients unless there are more than this many

    --parallel_mx       Max number of client threads to use
"""

import sys
from optparse import OptionParser
import multiprocessing
import logging
import lib.libsf as libsf
from lib.libsf import mylog
from lib.libclient import ClientError, SfClient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class CleanIscsiAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThreadFunction(self, client_ip, client_user, client_pass, default_iscsid, results, index):
        client = SfClient()
        mylog.info(client_ip + ": Connecting to client")
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, clientIP=client_ip, exception=e)
            results[index] = False
            return

        mylog.info(client_ip + ": Cleaning iSCSI on client '" + client.Hostname + "'")

        try:
            client.CleanIscsi(default_iscsid)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            super(self.__class__, self)._RaiseEvent(self.Events.FAILURE, clientIP=client_ip, exception=e)
            results[index] = False
            return

        results[index] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, client_ips=None, default_iscsid=True, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Clean iSCSI initiator on a list of clients
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Run the client operations in parallel if there are enough clients
        if len(client_ips) <= parallel_thresh:
            parallel_clients = 1
        else:
            parallel_clients = parallel_max

        # Start the client threads
        thread_index = 0
        self._threads = []
        manager = multiprocessing.Manager()
        results = manager.dict()
        for client_ip in client_ips:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._ClientThreadFunction, args=(client_ip, client_user, client_pass, default_iscsid, results, thread_index))
            th.start()
            self._threads.append(th)
            thread_index += 1

        allgood = libsf.ThreadRunner(self._threads, results, parallel_clients)

        if allgood:
            mylog.passed("Successfully cleaned iSCSI on all clients")
            return True
        else:
            mylog.error("Could not clean iSCSI on all clients")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=sfdefaults.client_ips, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--nodefault_iscsid", action="store_false", dest="default_iscsid", default=True, help="do not restore iscsi config files to defaults")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.default_iscsid, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
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
