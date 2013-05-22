#!/usr/bin/python

"""
This action will display the ensemble leader

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --ssh_user          The nodes SSH username
    SFSSH_USER env var

    --ssh_pass          The nodes SSH password
    SFSSH_PASS

    --csv               Display minimal output that is suitable for piping into other programs

    --bash              Display minimal output that is formatted for a bash array/for loop
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class GetEnsembleLeaderAction(ActionBase):
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
                            "password" : None},
            args)

    def Get(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Get the cluster master
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)
        if bash or csv:
            mylog.silence = True

        mylog.info("Looking for ensemble leader on cluster " + mvip)

        # Use the first node in the cluster to connect and query each ensemble node until we find the leader
        try:
            result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        except libsf.SfError as e:
            mylog.error("Failed to get node list - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        try:
            ssh = libsf.ConnectSsh(result["nodes"][0]["mip"], ssh_user, ssh_pass)
        except libsf.SfError as e:
            mylog.error("Failed to connect to node - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # For each ensemble node, query if it is the leader
        leader_10g = None
        try:
            cluster_info = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        except libsf.SfError as e:
            mylog.error("Failed to get cluster info - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        mylog.info("Ensemble is [" + ", ".join(cluster_info["clusterInfo"]["ensemble"]) + "]")
        for teng_ip in cluster_info["clusterInfo"]["ensemble"]:
            try:
                stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "echo 'mntr' | nc " + teng_ip + " 2181 | grep zk_server_state | cut -f2")
                data = stdout.readlines()[0].strip()
                if data == "leader":
                    leader_10g = teng_ip
                    break
            except libsf.SfError as e:
                mylog.error("Failed nc command on node - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)

        if not leader_10g:
            mylog.error("Could not find ensemble leader")
            self.RaiseFailureEvent(message="Could not find ensemble leader")
            return False

        for node in result["nodes"]:
            if node["cip"] == leader_10g or node["sip"] == leader_10g:
                leader_1g = node["mip"]

        self.SetSharedValue(SharedValues.nodeIP, leader_1g)
        self.SetSharedValue(SharedValues.ensembleLeaderIP, leader_1g)
        return leader_1g

    def Execute(self, mvip=sfdefaults.mvip, csv=False, bash=False, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Get the cluster master
        """
        del self
        leader = Get(**locals())
        if leader is False:
            return False

        if csv or bash:
            mylog.debug("Ensemble leader node is " + leader)
            sys.stdout.write(leader + "\n")
            sys.stdout.flush()
        else:
            mylog.info("Ensemble leader node is " + leader)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes.  Only used if you do not have SSH keys set up")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.csv, options.bash, options.username, options.password, options.ssh_user, options.ssh_pass, options.debug):
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

