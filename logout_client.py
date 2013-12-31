#!/usr/bin/python

"""
This action will log out of iSCSI volumes on a list of clients

When run as a script, the following options/env variables apply:
    --client_ips        The IP addresses of the clients

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --target_list       List of target IQNs to log out of instead of all

    --noclean           Do not "clean" iSCSI after logging out

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

class LogoutClientAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_ALL = "BEFORE_ALL"
        AFTER_ALL = "AFTER_ALL"
        BEFORE_CLIENT_LOGOUT = "BEFORE_CLIENT_LOGOUT"
        AFTER_CLIENT_LOGOUT = "AFTER_CLIENT_LOGOUT"
        BEFORE_CLIENT_CLEAN = "BEFORE_CLIENT_CLEAN"
        AFTER_CLIENT_CLEAN = "AFTER_CLIENT_CLEAN"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _ClientThread(self, client_ip, client_user, client_pass, target_list, clean, results):
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

        # Log out of all volumes
        mylog.info(client_ip + ": Logging out of iSCSI volumes on client '" + client.Hostname + "'")
        self._RaiseEvent(self.Events.BEFORE_CLIENT_LOGOUT, clientIP=client_ip)
        try:
            client.LogoutTargets(target_list)
        except ClientError as e:
            mylog.error(client_ip + ": " + e.message)
            self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
            return
        self._RaiseEvent(self.Events.AFTER_CLIENT_LOGOUT, clientIP=client_ip)

        # Clean iSCSI
        if clean:
            mylog.info(client_ip + ": Cleaning iSCSI on client '" + client.Hostname + "'")
            self._RaiseEvent(self.Events.BEFORE_CLIENT_CLEAN, clientIP=client_ip)
            try:
                client.CleanIscsi()
            except ClientError as e:
                mylog.error(client_ip + ": " + e.message)
                self.RaiseFailureEvent(message=str(e), clientIP=client_ip, exception=e)
                return
            self._RaiseEvent(self.Events.AFTER_CLIENT_CLEAN, clientIP=client_ip)

        results[myname] = True
        return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"client_ips" : libsf.IsValidIpv4AddressList,
                            },
            args)

    def Execute(self, client_ips=None, clean=True, target_list=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, parallel_thresh=sfdefaults.parallel_thresh, parallel_max=sfdefaults.parallel_max, debug=False):
        """
        Log out of iSCSI volumes on clients
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
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        for client_ip in client_ips:
            thread_name = "client-" + client_ip
            results[thread_name] = False
            th = multiprocessing.Process(target=self._ClientThread, name=thread_name, args=(client_ip, client_user, client_pass, target_list, clean, results))
            self._threads.append(th)

        self._RaiseEvent(self.Events.BEFORE_ALL)
        allgood = libsf.ThreadRunner(self._threads, results, parallel_clients)
        self._RaiseEvent(self.Events.AFTER_ALL)

        if allgood:
            mylog.passed("Successfully logged out of volumes on all clients")
            return True
        else:
            mylog.error("Could not log out of all volumes on all clients")
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
    parser.add_option("--noclean", action="store_false", dest="clean", default=True, help="do not clean iSCSI after logging out")
    parser.add_option("--target_list", action="list", dest="target_list", default=None, help="the list of volume IQNs to log out of, instead of all volumes")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.parallel_thresh, help="do not thread clients unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.parallel_max, help="the max number of client threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.client_ips, options.clean, options.target_list, options.client_user, options.client_pass, options.parallel_thresh, options.parallel_max, options.debug):
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
