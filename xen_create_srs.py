#!/usr/bin/python

"""
This action will create SRs from available iSCSI volumes

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --account_name      SolidFire CHAP account name, if using CHAP

    --volgroup_name     SolidFire volume access group name, if not using CHAP
"""

import sys
from optparse import OptionParser
import logging
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class XenCreateSrsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None,
                            "mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None},
            args)
        if not args["account_name"] and not args["volgroup_name"]:
            raise libsf.SfArgumentError("Please specify an account name or volgroup name")

    def Execute(self, account_name=None, volgroup_name=None, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Create SRs for all iSCSI volumes
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        chap_user = None
        chap_pass = None
        expected_volumes = 0
        if account_name:
            # Find the account on the SF cluster
            mylog.info("Looking for account '" + account_name + "' on cluster '" + mvip + "'")
            accounts_list = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
            sfaccount = None
            for account in accounts_list["accounts"]:
                if (account["username"].lower() == account_name.lower()):
                    sfaccount = account
                    break
            if not sfaccount:
                mylog.error("Could not find CHAP account " + account_name)
                self.RaiseFailureEvent(message="Could not find CHAP account " + account_name)
                return False
            chap_user = account_name
            chap_pass = sfaccount["initiatorSecret"]
            expected_volumes = len(sfaccount["volumes"])
        elif volgroup_name:
            vag = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=volgroup_name)
            expected_volumes = len(vag["volumes"])

        # Get the SVIP of the SF cluster
        cluster_info = libsf.CallApiMethod(mvip, username, password, "GetClusterInfo", {})
        svip = cluster_info["clusterInfo"]["svip"]

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Get a list of already existing SRs
        existing_srs = dict()
        sr_ref_list = session.xenapi.SR.get_all()
        for sr_ref in sr_ref_list:
            sr = session.xenapi.SR.get_record(sr_ref)
            if sr["type"] != "lvmoiscsi":
                continue
            pbd = session.xenapi.PBD.get_record(sr["PBDs"][0])
            iqn = pbd["device_config"]["targetIQN"]
            existing_srs[iqn] = sr["name_label"]


        # Find the specified vmhost
        xen_host = None
        host_list = session.xenapi.host.get_all()
        for host_ref in host_list:
            h = session.xenapi.host.get_record(host_ref)
            if h["address"] == vmhost:
                xen_host = host_ref
                break

        # Get the list of targets
        mylog.info("Discovering iSCSI volumes")
        target_iqns = dict()
        try:
            target_iqns = libxen.GetIscsiTargets(session, xen_host, svip, chap_user, chap_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            sys.exit(1)
        mylog.debug("Found " + str(len(target_iqns)) + " iSCSI targets")
        if len(target_iqns) != expected_volumes:
            mylog.debug("Discovered " + str(len(target_iqns)) + " targets but expected " + str(expected_volumes) + " targets")
            self.RaiseFailureEvent(message="Discovered " + str(len(target_iqns)) + " targets but expected " + str(expected_volumes) + " targets")
            return False

        # Create an SR on each target
        for iqn in sorted(target_iqns):
            if iqn in existing_srs:
                mylog.info(iqn + " is already used by SR " + existing_srs[iqn])
                continue

            mylog.info("Probing SCSI LUN on " + iqn)
            scsi_id = None
            sr_size = None
            retry = 3
            wait = 20
            while True:
                try:
                    scsi_id, sr_size = libxen.GetScsiLun(session, xen_host, iqn, svip, chap_user, chap_pass)
                    break
                except libxen.XenError as e:
                    retry -= 1
                    if retry <= 0:
                        mylog.error(str(e))
                        sys.exit(1)
                    else:
                        mylog.warning(str(e))
                        mylog.warning("Retrying in " + str(wait) + " sec...")
                        time.sleep(wait)

            desc = "iSCSI SR [" + svip + " (" + iqn + ")]"
            iqn_pieces = iqn.split('.')
            sr_name = iqn_pieces[4] + "." + iqn_pieces[5]

            mylog.info("Creating SR " + sr_name + " (" + str(sr_size/1000/1000/1000) + "GB) SCSIid " + scsi_id)
            sr_args = {
                        "target": svip,
                        "targetIQN": iqn,
                        "SCSIid": scsi_id
            }
            if chap_user:
                sr_args["chapuser"] = chap_user
            if chap_pass:
                sr_args["chappassword"] = chap_pass
            sr_type = "lvmoiscsi"
            retry = 3
            wait = 20
            while True:
                try:
                    # The size arg is a string because the Xen XML-RPC implementation chokes on integers that are this large
                    session.xenapi.SR.create(xen_host, sr_args, str(sr_size), sr_name, desc, sr_type, "user", True)
                    break
                except XenAPI.Failure as e:
                    retry -= 1
                    if retry <= 0:
                        mylog.error("Could not create SR for target " + iqn + " - " + str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False
                    else:
                        mylog.warning("Could not create SR for target " + iqn + " - " + str(e))
                        mylog.warning("Retrying in " + str(wait) + " sec...")
                        time.sleep(wait)
            mylog.passed("  Successfully created SR " + sr_name)

        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--account_name", type="string", dest="account_name", default=None, help="the SolidFire CHAP account name for the hypervisor (if using CHAP)")
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the SolidFire VAG name for the hypervisor (if not using CHAP)")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.account_name, options.volgroup_name, options.vmhost, options.host_user, options.host_pass, options.mvip, options.username, options.password, options.debug):
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

