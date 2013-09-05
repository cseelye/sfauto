#!/usr/bin/python

"""
This action will wait for the cluster to be healthy

Healthy is currently defined as no faults, no cores, no xUnknownBlockID

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


    --fault_whitelist   Ignore these faults if they are present
    SFFAULT_WHITELIST env var

    --ignore_faults     Do not check for cluster faults

    --ignore_cores      Do not check for core files
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import time
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import check_cluster_health

class WaitForClusterHealthyAction(ActionBase):
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
                            },
            args)

    def Execute(self, mvip=sfdefaults.mvip, interval=60, timeout=3600, ignoreCores=False, ignoreFaults=False, fault_whitelist=None, since=0, username=sfdefaults.username, password=sfdefaults.password, ssh_user=sfdefaults.ssh_user, ssh_pass=sfdefaults.ssh_pass, debug=False):
        """
        Wait for the cluster to be healthy
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        mylog.info("Checking the health of the cluster")
        start_time = time.time()
        healthy = False
        while not healthy:
            if time.time() - start_time > timeout:
                mylog.error("Timed out waiting for cluster to become healthy")
                return False
            
            if check_cluster_health.Execute(mvip, ignoreCores, ignoreFaults, fault_whitelist, since, username, password, ssh_user, ssh_pass, debug) == False:
                mylog.info("Waiting " + str(interval) + " seconds before checking again")
                time.sleep(interval)
            else:
                mylog.info("Duration to become healthy: " + libsf.SecondsToElapsedStr(time.time() - start_time) + " seconds")
                return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=sfdefaults.ssh_user, help="the SSH username for the nodes")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=sfdefaults.ssh_pass, help="the SSH password for the nodes")
    parser.add_option("--since", type="int", dest="since", default=0, help="timestamp of when to check health from")
    parser.add_option("--fault_whitelist", action="list", dest="fault_whitelist", default=None, help="ignore these faults and do not wait for them to clear")
    parser.add_option("--ignore_cores", action="store_true", dest="ignoreCores", default=False, help="ignore core files on nodes")
    parser.add_option("--ignore_faults", action="store_true", dest="ignoreFaults", default=False, help="ignore cluster faults")
    parser.add_option("--interval", type="int", dest="interval", default=60, help="how often in seconds to check cluster health")
    parser.add_option("--timeout", type="int", dest="timeout", default=3600, help="the timeout in seconds to wait for a cluster to become healthy")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.interval, options.timeout, options.ignoreCores, options.ignoreFaults, options.fault_whitelist, options.since, options.username, options.password, options.ssh_user, options.ssh_pass, options.debug):
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
    sys.exit(0)
