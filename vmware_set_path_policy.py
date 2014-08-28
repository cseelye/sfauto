#!/usr/bin/env python

"""
This action will set the multipathing policy (load balancing policy) on the LUNs attached to a VMware host

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vmhost            The IP of the ESX host to set the policy on

    --policy            The new path selection policy to set
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import lib.libsfcluster as libsfcluster
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareSetPathPolicyAction(ActionBase):
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
                            "vmhost" : None,
                            "new_policy" : None},
            args)

    def Execute(self, vmhost, new_policy, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, debug=False):
        """
        Set the multipath policy
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        if new_policy.lower() == "rr":
            new_policy = "VMW_PSP_RR"
        elif new_policy.lower() == "fixed":
            new_policy = "VMW_PSP_FIXED"
        elif new_policy.lower() == "mru":
            new_policy = "VMW_PSP_MRU"

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:
                # Find the requested host
                mylog.info("Searching for host " + vmhost)
                host = libvmware.FindHost(vsphere, vmhost)

                storage_sys = host.configManager.storageSystem

                mylog.info("Checking path selection policies")
                policies = storage_sys.QueryPathSelectionPolicyOptions()
                allowed_policies = [p.policy.key for p in policies]
                if new_policy not in allowed_policies:
                    mylog.error("Unrecognized multipath policy: only " + " / ".join(allowed_policies) + " are allowed")
                    return False

                mylog.info("Getting a list of LUNs")
                lun2name = {}
                for lun in host.config.storageDevice.scsiLun:
                    lun2name[lun.key] = lun.canonicalName

                allgood = True
                for lun in storage_sys.storageDeviceInfo.multipathInfo.lun:
                    name = lun2name[lun.lun]
                    # Skip non-SF devices
                    if "f47acc" not in name:
                        continue

                    if lun.policy.policy == new_policy:
                        mylog.info(name + " is already using policy " + new_policy)
                        continue

                    mylog.info("Setting " + name + " to " + new_policy)
                    if new_policy != "VMW_PSP_FIXED":
                        p = vim.host.MultipathInfo.LogicalUnitPolicy()
                        p.policy = new_policy
                        try:
                            storage_sys.SetMultipathLunPolicy(lun.id, p)
                        except vmodl.MethodFault as e:
                            mylog.error("Failed to set multipath policy on " + name + ": " + str(e))
                            allgood = False
                    else:
                        # Hacky, but it appears the only workable way to set the policy to fixed is through esxcli or powercli.  WTF
                        ssh = libsf.ConnectSsh(vmhost, "root", "solidfire")
                        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "esxcli storage nmp device set --device=\"" + name + "\" --psp=VMW_PSP_FIXED")
                        output = "\n".join(stdout.readlines()).strip()
                        err = "\n".join(stderr.readlines()).strip()
                        if output or err:
                            mylog.error("Failed to set multipath policy on " + name + ": " + output + err)
                            allgood = False
                        else:
                            # Wait for the policy to change
                            stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "POLICY=; while [[ \"$POLICY\" != \"" + new_policy + "\" ]]; do sleep 1; POLICY=$(esxcli storage nmp device list | grep -A8 " + name + " | grep 'Path Selection Policy:' | awk '{print $4}'); if [[ $? != \"0\" ]]; then break; fi; done")
                        ssh.close()
        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        if allgood:
            mylog.passed("Successfully set multipath policy on " + vmhost)
            return True
        else:
            mylog.error("Failed to set multipath policy on all volumes")
            return False


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
    parser.add_option("-o", "--vmhost", type="string", dest="vmhost", default=None, help="the IP of the ESX host to set the policy on")
    parser.add_option("--policy", type="string", dest="policy", default=None, help="the new path selection policy to set")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, new_policy=options.policy, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, debug=options.debug):
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
