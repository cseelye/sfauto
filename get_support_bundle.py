#!/usr/bin/env python

"""
This action will get support bundle from a list of nodes

When run as a script, the following options/env variables apply:
    --node_ips          List of node IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --folder            Directory to save the bundles in

    --label             Label to prepend to the bundle filename
"""
import sys
import multiprocessing
import time
import os
import re
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetSupportBundleAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _NodeThread(self, timestamp, label, folder, node_ip, node_user, node_pass, results, index):
        try:
            mylog.info(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

            # Create a support bundle
            mylog.info(node_ip + ": Generating support bundle")
            bundle_name = label + "_" + timestamp
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "/sf/scripts/sf_make_support_bundle " + bundle_name + ";echo $?")
            data = stdout.readlines()
            retcode = int(data.pop())
            if retcode != 0:
                mylog.error("\n".join(data))
                ssh.close()
                self.RaiseFailureEvent(message="\n".join(data), nodeIP=node_ip)
                return False
            full_bundle_name = ""
            for line in reversed(data):
                m = re.search("Generated \"(.+)\" successfully", line)
                if m:
                    full_bundle_name = m.group(1)
                    break

            # Compress the bundle using parallel gzip
            mylog.info(node_ip + ": Compressing bundle")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "pigz " + full_bundle_name + ";echo $?")
            data = stdout.readlines()
            retcode = int(data.pop())
            if retcode != 0:
                err = stdout.readlines()
                err = "\n".join(err)
                mylog.error(err)
                ssh.close()
                self.RaiseFailureEvent(message=err, nodeIP=node_ip)
                return False
            full_bundle_name = full_bundle_name + ".gz"

            # Copy the file to the local system
            mylog.info(node_ip + ": Saving bundle to " + folder + "/" + full_bundle_name)
            sftp = ssh.open_sftp()
            sftp.get(full_bundle_name, folder + "/" + full_bundle_name)
            sftp.close()

            # Remove the copy on the node
            libsf.ExecSshCommand(ssh, "rm " + full_bundle_name + "*")
            ssh.close()
            mylog.info(node_ip + ": Finished")
            results[index] = True
        except libsf.SfError as e:
            mylog.error(node_ip + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            return False

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ips" : libsf.IsValidIpv4AddressList},
            args)

    def Execute(self, folder, label, node_ips=None, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Get support bundle from a list of nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # Create the output directory
        if (not os.path.exists(folder)):
            os.makedirs(folder)

        # Start one thread per node
        manager = multiprocessing.Manager()
        results = manager.dict()
        self._threads = []
        thread_index = 0
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        for node_ip in node_ips:
            results[thread_index] = False
            th = multiprocessing.Process(target=self._NodeThread, args=(timestamp, label, folder, node_ip, ssh_user, ssh_pass, results, thread_index))
            th.daemon = True
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
            mylog.passed("Successfully got bundle from all nodes")
            return True
        else:
            mylog.error("Could not get bundle from all nodes")
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
    parser.add_option("--folder", type="string", dest="folder", default="bundles", help="the name of the directory to store the bundle(s) in.")
    parser.add_option("--label", type="string", dest="label", default="bundle", help="a label to prepend to the name of the bundle file.")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.folder, options.label, options.node_ips, options.ssh_user, options.ssh_pass, options.debug):
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

