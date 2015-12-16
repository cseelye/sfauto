#!/usr/bin/env python2.7

"""
This action will start an upstart service on a list of nodes simultaneously

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
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class StartUpstartServiceAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"service_name" : None,
                            "node_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, service_name=None, node_ips=None, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, strict=False, debug=False):
        """
        Start upstart service on the nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Starting {} on {}".format(service_name, ", ".join(node_ips)))

        # Start one thread per node
        starter = multiprocessing.Event()
        starter.clear()
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        counter = libsf.SyncCounter()
        for node_ip in node_ips:
            thread_name = "node-" + node_ip
            results[thread_name] = False
            th = multiprocessing.Process(target=self._NodeThread, name=thread_name, args=(service_name, node_ip, ssh_user, ssh_pass, strict, counter, starter, results))
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
            mylog.error("Failed to start {} on all nodes".format(service_name))
            return False

        mylog.debug("Releasing threads")
        starter.set()

        # Wait for all threads to stop
        for th in self._threads:
            th.join()

        # Look at the results
        for thread_name in results.keys():
            if not results[thread_name]:
                mylog.error("Failed to start {} on all nodes")
                return False

        mylog.passed("Successfully started {} on all nodes".format(service_name))
        return True

    def _NodeThread(self, service_name, node_ip, node_user, node_pass, strict, ready_counter, starter, results):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            myname = multiprocessing.current_process().name
            results[myname] = False

            mylog.info("  {}: Connecting".format(node_ip))
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

            mylog.debug(node_ip + ": Waiting")
            ready_counter.Increment()
            starter.wait()

            mylog.info("  {}: Starting {}".format(node_ip, service_name))
            _, stdout, stderr = libsf.ExecSshCommand(ssh, "start {};echo $?".format(service_name))
            output = stdout.readlines()
            error = "\n".join(stderr.readlines())
            error = error.strip()
            ssh.close()
            retcode = int(output.pop())
            if retcode != 0:
                if not strict and "Job is already running" in error:
                    mylog.passed("  {}: {} is already running".format(node_ip, service_name))
                    results[myname] = True
                    return
                else:
                    mylog.error("  {}: Error starting {}: {}".format(node_ip, service_name, error))
                    results[myname] = False
                    return

            mylog.passed("  {}: Successfully started {}".format(node_ip, service_name))
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
    parser.add_option("--service", type="string", dest="service_name", default=None, help="the name of the service to start")
    parser.add_option("--strict", action="store_true", dest="strict", default=False, help="fail if service is already running")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(service_name=options.service_name, node_ips=options.node_ips, ssh_user=options.ssh_user, ssh_pass=options.ssh_pass, strict=options.strict, debug=options.debug):
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

