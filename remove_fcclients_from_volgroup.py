#!/usr/bin/python

"""
This action will remove clients from a volume access group

The action connects to each specified client, queries the FC WWNs, then removes those WWNs from the volume access group on the cluster

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --client_ips        The IP addresses of the clients
    SFCLIENT_IPS env var

    --client_user       The username for the client
    SFCLIENT_USER env var

    --client_pass       The password for the client
    SFCLIENT_PASS env var

    --volgroup_name     The name of the volume group

    --volgroup_id       The ID of the volume group
"""

import sys
from optparse import OptionParser
import logging
import lib.libsf as libsf
from lib.libsf import mylog, SfError
from lib.libclient import ClientError, SfClient
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.libsfcluster import SFCluster

class RemoveFcclientsFromVolgroupAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "client_ips" : libsf.IsValidIpv4AddressList},
            args)
        if not args["volgroup_name"] and args["volgroup_id"] <= 0:
            raise libsf.SfArgumentError("Please specify a volgroup name or ID")

    def Execute(self, mvip=sfdefaults.mvip, client_ips=None, volgroup_name=None, volgroup_id=0, username=sfdefaults.username, password=sfdefaults.password, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, debug=False):
        """
        Remove the specified clients from the specified volume access group
        """
        if not client_ips:
            client_ips = sfdefaults.client_ips
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Get the cluster version so we know which endpoint to use
        cluster = SFCluster(mvip, username, password)
        try:
            api_version = cluster.GetAPIVersion()
        except libsf.SfError as e:
            mylog.error("Failed to get cluster version: " + str(e))
            mylog.info("Assuming API version 7.0")
            api_version = 7.0

        # Find the group
        mylog.info("Finding the volume group on the cluster")
        try:
            volgroup = libsf.FindVolumeAccessGroup(mvip, username, password, VagName=volgroup_name, VagId=volgroup_id, ApiVersion=api_version)
        except libsf.SfError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Separate initiators into FC and iSCSI
        if "iscsiInitiators" not in volgroup:
            volgroup["iscsiInitiators"] = []
        if "fibreChannelInitiators" not in volgroup:
            volgroup["fibreChannelInitiators"] = []
        if "initiators" in volgroup:
            for init in volgroup["initiators"]:
                if init.startswith("iqn") and init not in volgroup["iscsiInitiators"]:
                    volgroup["iscsiInitiators"].append(init)
                elif init not in volgroup["fibreChannelInitiators"]:
                    volgroup["fibreChannelInitiators"].append(init)

        # Get a list of initiator WWNs from the clients
        remove_wwns = []
        for client_ip in client_ips:
            client = SfClient()
            mylog.info("Connecting to client '" + client_ip + "'")
            try:
                client.Connect(client_ip, client_user, client_pass)
            except ClientError as e:
                mylog.error(e)
                return False
            wwns = client.GetWWNs()
            mylog.info("  " + client.Hostname + " has WWNs " + ", ".join(map(libsf.HumanizeWWN, wwns)))

            for wwn in wwns:
                if wwn in remove_wwns:
                    mylog.error("Duplicate WWN " + wwn)
                    self.RaiseFailureEvent(message="Duplicate WWN " + wwn)
                    return False
            remove_wwns += wwns

        full_wwn_list = volgroup["fibreChannelInitiators"]
        full_wwn_list = [x.lower() for x in full_wwn_list]
        for wwn in reversed(full_wwn_list):
            if wwn in remove_wwns:
                mylog.debug("Removing " + wwn)
                full_wwn_list.remove(wwn)
        all_init_list = volgroup["iscsiInitiators"] + full_wwn_list

        # Add the WWNs to the volume group
        mylog.info("Removing client WWNs from group")
        params = {}
        params["volumeAccessGroupID"] = volgroup["volumeAccessGroupID"]
        params["fibreChannelInitiators"] = full_wwn_list
        params["initiators"] = all_init_list
        try:
            libsf.CallApiMethod(mvip, username, password, "ModifyVolumeAccessGroup", params, ApiVersion=api_version)
        except libsf.SfApiError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.passed("Successfully removed clients from group")
        return True

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
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster [%default]")
    parser.add_option("--volgroup_name", type="string", dest="volgroup_name", default=None, help="the name of the group")
    parser.add_option("--volgroup_id", type="int", dest="volgroup_id", default=0, help="the ID of the group")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.client_ips, options.volgroup_name, options.volgroup_id, options.username, options.password, options.client_user, options.client_pass, options.debug):
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
