#!/usr/bin/env python

"""
This action will run sfnodereset on multiple nodes in parallel, destroying a cluster

When run as a script, the following options/env variables apply:
    --node_ips          List of node IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --save_logs         Save a copy of the logs before reset
"""

import sys
import multiprocessing
import time
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class ClusterSfnoderesetAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _NodeThread(self, node_ip, node_user, node_pass, save_logs, results, index):
        try:
            mylog.info(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

            if save_logs:
                archive_name = "sflogs_" + libsf.TimestampToStr(time.time(), "%Y-%m-%d-%H-%M-%S") + ".tgz"
                mylog.info(node_ip + ": Saving sf-* logs to /var/log/" + archive_name)
                libsf.ExecSshCommand(ssh, "cd /var/log&&tar czf " + archive_name + " sf-*")

            mylog.info(node_ip + ": Starting sfnodereset")
            libsf.ExecSshCommand(ssh, "nohup /sf/bin/sfnodereset -fR > sfnr.out 2>&1 &")
            time.sleep(5)
            ssh.close()

            time.sleep(20)
            mylog.info(node_ip + ": Waiting for node to go down")
            # Wait for the node to go down
            wait_start = time.time()
            while(libsf.Ping(node_ip)):
                time.sleep(2)
                if time.time() - wait_start > 60 * 7: # See if it's been longer than 7 minutes
                    mylog.warning(node_ip + ": Taking too long; aborting")
                    results[index] = False
                    return

            mylog.info(node_ip + ": Waiting for node to reboot")
            time.sleep(30)
            # Wait for the node to come back up
            while(not libsf.Ping(node_ip)):
                mylog.debug(node_ip + ": cannot ping yet")
                time.sleep(30)

            mylog.info(node_ip + ": Node is back up")
            results[index] = True
        except KeyboardInterrupt:
            results[index] = False
            return
        except Exception as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            results[index] = False

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, node_ips=None, save_logs=False, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        sfnodereset a list of nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Start one thread per node
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        thread_index = 0
        for node_ip in node_ips:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._NodeThread, args=(node_ip, ssh_user, ssh_pass, save_logs, results, thread_index))
            th.daemon = False
            th.start()
            self._threads.append(th)
            thread_index += 1

        # Wait for all threads to stop
        for th in self._threads:
            th.join()

        # Check the results
        all_success = True
        for res in results.values():
            if not res:
                all_success = False
        if all_success:
            mylog.passed("Successfully reset all nodes")
            return True
        else:
            mylog.error("Could not reset all nodes")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-n", "--node_ips", action="list", dest="node_ips", default=None, help="the IP addresses of the nodes")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--save_logs", action="store_true", dest="save_logs", default=False, help="save a copy of sf logs before reset")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.node_ips, options.save_logs, options.ssh_user, options.ssh_pass, options.debug):
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

