#!/usr/bin/env python

"""
This action will get support bundle from a list of nodes

When run as a script, the following options/env variables apply:
    --node_ips          List of node IP addresses
    SFNODE_IPS

    --ssh_user          The nodes SSH username
    SFSSH_USER

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --remote_ip         The remote system to send bundles to. Bundles are downlaoded locally if this is not specified

    --remote_user       The remote system username

    --remote_pass       The remote system password

    --folder            Directory to save the bundles in

    --label             Label to prepend to the bundle filename
"""
from optparse import OptionParser
import os
import re
import socket
import sys
import threading
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class GenerateSupportBundleAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _NodeThread(self, timestamp, label, folder, node_ip, node_user, node_pass, remote_ip, remote_user, remote_pass, results):
        myname = threading.current_thread().name
        results[myname] = False

        try:
            mylog.info(node_ip + ": Connecting")
            ssh = libsf.ConnectSsh(node_ip, node_user, node_pass)

            # Create a support bundle on the node
            mylog.info(node_ip + ": Generating support bundle on node")
            bundle_name = label + "_" + timestamp
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo /sf/scripts/sf_make_support_bundle " + bundle_name + ";echo $?")
            stderr_data = stderr.readlines()
            stdout_data = stdout.readlines()
            retcode = int(stdout_data.pop())
            # retry on zktreeutil error that floods stderr
            retry = 2
            while retcode != 0 and len(stderr_data) > 1000 and retry > 0:
                time.sleep(1)
                mylog.debug(node_ip + ": retry bundle")
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo /sf/scripts/sf_make_support_bundle " + bundle_name + ";echo $?")
                stderr_data = stderr.readlines()
                stdout_data = stdout.readlines()
                retcode = int(stdout_data.pop())
                retry -= 1
            if retcode != 0:
                mylog.error(node_ip + ": Error creating bundle - " + "\n".join(stdout_data) + "\n".join(stderr_data))
                ssh.close()
                self.RaiseFailureEvent(message="Error creating bundle", nodeIP=node_ip)
                return
            full_bundle_name = ""
            for line in reversed(stdout_data):
                m = re.search("Generated \"(.+)\" successfully", line)
                if m:
                    full_bundle_name = m.group(1)
                    break

            # Compress the bundle on the node using parallel gzip
            mylog.info(node_ip + ": Compressing bundle on node")
            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo pigz " + full_bundle_name + ";echo $?")
            stdout_data = stdout.readlines()
            stderr_data = stderr.readlines()
            retcode = int(stdout_data.pop())
            if retcode != 0:
                mylog.error(node_ip + ": Error compressing bundle - " + "\n".join(stdout_data) + "\n".join(stderr_data))
                ssh.close()
                self.RaiseFailureEvent(message="Error compressing bundle", nodeIP=node_ip)
                return
            full_bundle_name = full_bundle_name + ".gz"

            if remote_ip:
                mylog.info(node_ip + ": Sending bundle to " + remote_ip + ":" + folder)
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo sshpass -p " + remote_pass + " ssh -o StrictHostKeyChecking=no " + remote_user + "@" + remote_ip + " \"mkdir -p " + folder + "\";echo $?")
                stdout_data = stdout.readlines()
                stderr_data = stderr.readlines()
                retcode = int(stdout_data.pop())
                if retcode != 0:
                    mylog.error(node_ip + ": Error creating remote folder - " + "\n".join(stdout_data) + "\n".join(stderr_data))
                    ssh.close()
                    self.RaiseFailureEvent(message="Error creating remote folder", nodeIP=node_ip)
                    return

                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "sudo sshpass -p " + remote_pass + " scp -o StrictHostKeyChecking=no " + full_bundle_name + " " + remote_user + "@" + remote_ip + ":" + folder + ";echo $?")
                stdout_data = stdout.readlines()
                stderr_data = stderr.readlines()
                retcode = int(stdout_data.pop())
                if retcode != 0:
                    mylog.error(node_ip + ": Error uploading bundle - " + "\n".join(stdout_data) + "\n".join(stderr_data))
                    ssh.close()
                    self.RaiseFailureEvent(message="Error uploading bundle", nodeIP=node_ip)
                    return
            else:
                # Copy the file to the local system
                mylog.info(node_ip + ": Saving bundle locally to " + folder + "/" + full_bundle_name)
                sftp = ssh.open_sftp()
                sftp.get(full_bundle_name, folder + "/" + full_bundle_name)
                sftp.close()

            # Remove the bundle on the node
            libsf.ExecSshCommand(ssh, "rm " + full_bundle_name + "*")
            ssh.close()
            mylog.info(node_ip + ": Finished")
            results[myname] = True
        except libsf.SfError as e:
            mylog.error(node_ip + ": " + str(e))
            self.RaiseFailureEvent(message=str(e), nodeIP=node_ip, exception=e)
            return
        except socket.timeout:
            mylog.error(node_ip + ": Timed out executing SSH command")
            self.RaiseFailureEvent(message="Timed out executing SSH command", nodeIP=node_ip, exception=e)
            return

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"node_ips" : libsf.IsValidIpv4AddressList,
                            "folder" : None,
                            "label" : None},
            args)

    def Execute(self, folder, label, node_ips=None, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, remote_ip=None, remote_user=sfdefaults.client_user, remote_pass=sfdefaults.client_pass, debug=False):
        """
        Get support bundle from a list of nodes
        """
        if not node_ips:
            node_ips = sfdefaults.node_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Create the local output directory if needed
        if not remote_ip and not os.path.exists(folder):
            os.makedirs(folder)

        # Start one thread per node
        _threads = []
        results = {}
        timestamp = time.strftime("%Y-%m-%d-%H-%M-%S")
        for node_ip in node_ips:
            thread_name = "bundle-" + node_ip
            results[thread_name] = False
            th = threading.Thread(target=self._NodeThread, name=thread_name, args=(timestamp, label, folder, node_ip, ssh_user, ssh_pass, remote_ip, remote_user, remote_pass, results))
            th.daemon = True
            _threads.append(th)

        allgood = libsf.ThreadRunner(_threads, results, len(_threads))

        if allgood:
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
    parser.add_option("--remote_ip", type="string", dest="remote_ip", default=None, help="the IP of a remote server to SCP the bundles to.  If this is not specified bundles will be downloaded locally [%default]")
    parser.add_option("--remote_user", type="string", dest="remote_user", default=sfdefaults.client_user, help="the username of the remote server [%default]")
    parser.add_option("--remote_pass", type="string", dest="remote_pass", default=sfdefaults.client_pass, help="the username of the remote server [%default]")
    parser.add_option("--folder", type="string", dest="folder", default="bundles", help="the name of the directory to store the bundle(s) in.  If remote_ip is specified, this is the folder on th remote machine, otherwise it is a folder on th local machine")
    parser.add_option("--label", type="string", dest="label", default="bundle", help="a label to prepend to the name of the bundle file.")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(folder=options.folder, label=options.label, node_ips=options.node_ips, ssh_user=options.ssh_user, ssh_pass=options.ssh_pass, remote_ip=options.remote_ip, remote_user=options.remote_user, remote_pass=options.remote_pass, debug=options.debug):
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
