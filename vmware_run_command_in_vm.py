#!/usr/bin/env python

"""
This action will run a command line program in a VM
"""

import sys
from optparse import OptionParser
import pipes
from pyVmomi import vim
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareRunCommandInVmAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mgmt_server" : libsf.IsValidIpv4Address,
                            "mgmt_user" : None,
                            "mgmt_pass" : None,
                            "vm_name" : None},
            args)

    def Execute(self, vm_name, cmdline, wait=True, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Get the command
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                mylog.info("Searching for VM " + vm_name)
                vm = libvmware.FindVM(vsphere, vm_name)

                if vm.guest.toolsRunningStatus in [vim.VirtualMachineToolsStatus.toolsNotInstalled, vim.VirtualMachineToolsStatus.toolsNotRunning]:
                    mylog.error('VMware Tools must be installed and running')
                    return -1

                STDOUT_FILE = '/tmp/sftest_stdout'
                STDERR_FILE = '/tmp/sftest_stderr'
                mylog.info("Starting command [ {} ]".format(cmdline))
                args = "-c \"rm -f /tmp/sftest_*; ({}) >{} 2>{}\"".format(cmdline.replace('"', '\\"'), STDOUT_FILE, STDERR_FILE)
                mylog.debug("Running /bin/bash {}".format(args))
                vm_creds = vim.vm.guest.NamePasswordAuthentication(username=client_user, password=client_pass)
                prog_spec = vim.vm.guest.ProcessManager.ProgramSpec(arguments=args, programPath='/bin/bash')
                pid = vsphere.content.guestOperationsManager.processManager.StartProgramInGuest(vm=vm, auth=vm_creds, spec=prog_spec)
                if not wait:
                    return 0

                # Wait for the process to complete
                return_code = None
                done = False
                while not done:
                    try:
                        process_list = vsphere.content.guestOperationsManager.processManager.ListProcessesInGuest(vm=vm, auth=vm_creds)
                    except vim.fault.InvalidState:
                        mylog.debug("invalidState exception")
                        time.sleep(1)
                        continue
                    for p in process_list:
                        if p.pid == pid and p.exitCode != None:
                            return_code = p.exitCode
                            done = True
                            break
                    if not done:
                        time.sleep(1)

                # Get the stdout/stderr of the process
                try:
                    transfer = vsphere.content.guestOperationsManager.fileManager.InitiateFileTransferFromGuest(guestFilePath=STDOUT_FILE, vm=vm, auth=vm_creds)
                    stdout = libsf.HttpRequest(transfer.url, None, None)
                except vim.fault.FileNotFound:
                    stdout = ''
                try:
                    transfer = vsphere.content.guestOperationsManager.fileManager.InitiateFileTransferFromGuest(guestFilePath=STDERR_FILE, vm=vm, auth=vm_creds)
                    stderr = libsf.HttpRequest(transfer.url, None, None)
                except vim.fault.FileNotFound:
                    stderr = ''

            if bash or csv:
                sys.stdout.write(stdout)
                sys.stdout.write("\n")
                sys.stdout.flush()
                if stderr:
                    sys.stderr.write(stderr)
                    sys.stderr.write("\n")
                    sys.stderr.flush()
            else:
                mylog.info("Return code: " + str(return_code))
                mylog.info("STDOUT: ")
                mylog.raw(stdout)
                if stderr:
                    mylog.info("STDERR: ")
                    mylog.raw(stderr)
    
            if return_code != None:
                return return_code
            else:
                return -1

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return -1


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-s", "--mgmt_server", type="string", dest="mgmt_server", default=sfdefaults.fc_mgmt_server, help="the IP/hostname of the vSphere Server [%default]")
    parser.add_option("-m", "--mgmt_user", type="string", dest="mgmt_user", default=sfdefaults.fc_vsphere_user, help="the vsphere admin username [%default]")
    parser.add_option("-a", "--mgmt_pass", type="string", dest="mgmt_pass", default=sfdefaults.fc_vsphere_pass, help="the vsphere admin password [%default]")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the VM [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the VM [%default]")
    parser.add_option("--cmdline", type="string", dest="cmdline", default=None, help="the command to run")
    parser.add_option("--nowait", action="store_false", dest="wait", default=True, help="start the command and immediately return, do not wait for it to complete")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        sys.exit( Execute(vm_name=options.vm_name, cmdline=options.cmdline, wait=options.wait, client_user=options.client_user, client_pass=options.client_pass, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, bash=options.bash, csv=options.csv, debug=options.debug) )
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
