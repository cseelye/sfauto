#!/usr/bin/env python

"""
This action will restart solidfire on a list of nodes simultaneously

When run as a script, the following options/env variables apply:
    --node_ips          List of node IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS
"""

import sys
import multiprocessing
import signal
import time
import inspect
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class RestartSolidfireAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_RESTART_SOLIDFIRE = "BEFORE_RESTART_SOLIDFIRE"
        AFTER_RESTART_SOLIDFIRE = "AFTER_RESTART_SOLIDFIRE"
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"nodeIPs" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, nodeIPs=None, sshUser=sfdefaults.ssh_user, sshPass=sfdefaults.ssh_pass, debug=False):
        """
        Restart solidfire on the nodes
        """
        if not nodeIPs:
            nodeIPs = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Restarting solidfire on " + ", ".join(nodeIPs))

        # Start one thread per node
        starter = multiprocessing.Event()
        starter.clear()
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        counter = libsf.SyncCounter()
        for node_ip in nodeIPs:
            thread_name = "node-" + node_ip
            results[thread_name] = False
            th = multiprocessing.Process(target=self._NodeThread, name=thread_name, args=(node_ip, sshUser, sshPass, counter, starter, results))
            th.daemon = True
            th.start()
            self._threads.append(th)

        # Wait for all threads to be connected
        mylog.debug("Waiting for all nodes to be connected")
        abort = False
        while counter.Value() < len(self._threads):
            for th in self._threads:
                if not th.is_alive():
                    mylog.debug("Thread failed; aborting")
                    abort = True
                    break
            if abort:
                break
            time.sleep(0.2)

        if abort:
            for th in self._threads:
                th.terminate()
                th.join()
            mylog.error("Failed to restart solidfire on all nodes")
            self.RaiseFailureEvent(message="Failed to restart solidfire on all nodes")
            return False

        self._RaiseEvent(self.Events.BEFORE_RESTART_SOLIDFIRE)

        mylog.debug("Releasing threads")
        starter.set()

        # Wait for all threads to stop
        for th in self._threads:
            th.join()

        # Look at the results
        for thread_name in results.keys():
            if not results[thread_name]:
                mylog.error("Failed to restart solidfire on all nodes")
                return False

        self._RaiseEvent(self.Events.AFTER_RESTART_SOLIDFIRE)

        mylog.passed("Successfully restarted solidfire on all nodes")
        return True

    def _NodeThread(self, node_ip, node_user, node_pass, ready_counter, starter, results):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            myname = multiprocessing.current_process().name
            results[myname] = False

            mylog.info(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

            mylog.debug(node_ip + ": Waiting")
            ready_counter.Increment()
            starter.wait()

            mylog.info(node_ip + ": Restarting solidfire")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "stop solidfire;start solidfire;echo $?")
            output = stdout.readlines()
            error = stderr.readlines()
            ssh.close()
            retcode = int(output.pop())
            if retcode != 0:
                mylog.error(node_ip + ": Error restarting solidfire: " + "\n".join(error))
                results[myname] = False
                return

            mylog.passed(node_ip + ": Successfully restarted solidfire")
            results[myname] = True
        except libsf.SfError as e:
            mylog.error(node_ip + ": " + str(e))
            return

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
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if action.Execute(options.node_ips, options.ssh_user, options.ssh_pass, options.debug):
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
        action.Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

