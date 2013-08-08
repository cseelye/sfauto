"""
This action will preform a volume rebalance stress test

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --emailTo          List of addresses to send email to

    --clientIPs        The IP addresses of the clients

    --clientUser       The username for the client
    SFclientUser env var

    --clientPass       The password for the client

    --waitTime          The time to wait between nodes

    --volume_size       The size of the volumes, in GB

    --addSSH            Boolean to add SSH keys to nodes

    --max_iops          QoS maxIOPS

    --min_iops          QoS minIOPS

    --burst_iops        QoS burstIOPS

    --iteration         how many times the test will be run, 0=forever
"""

import lib.libsf as libsf
import logging
import lib.sfdefaults as sfdefaults
import time
import sys
from optparse import OptionParser
from lib.libsf import mylog
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import get_active_nodes
import push_ssh_keys_to_node
import get_cluster_master
import reboot_node
import check_client_health
import check_cluster_health
import send_email
import power_off_node
import power_on_node
import get_node_ipmi_ip
import create_account
import list_slice_services
import create_volumes
import delete_volumes
import clusterbscheck


class StressVolumeRebalanceAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"
        EMAIL_SENT = "EMAIL_SENT"
        NODES_NOT_FOUND = "NODES_NOT_FOUND"
        REBOOT_NODE_FAIL = "REBOOT_NODE_FAIL"
        NODE_REBOOTED = "NODE_REBOOTED"
        CLIENT_NOT_HEALTHY = "CLIENT_NOT_HEALTHY"
        CLIENT_HEALTHY = "CLIENT_HEALTHY"
        CLUSTER_NOT_HEALTHY = "CLUSTER_NOT_HEALTHY"
        CLUSTER_HEALTHY = "CLUSTER_HEALTHY"
        FAULTS_FOUND = "FAULTS_FOUND"
        FAULTS_NOT_FOUND = "FAULTS_NOT_FOUND"
        ALL_NODES_FOUND = "ALL_NODES_FOUND"
        BEFORE_START_GC = "BEFORE_START_GC"
        GC_FINISHED = "GC_FINISHED"
        DRIVES_ADDED = "DRIVES_ADDED"
        DRIVES_NOT_ADDED = "DRIVES_NOT_ADDED"
        MASTER_NODE_NOT_FOUND = "MASTER_NODE_NOT_FOUND"
        PUSHED_SSH_KEYS = "PUSHED_SSH_KEYS"



    def __init__(self):
        super(self.__class__,self).__init__(self.__class__.Events)

    def fail(self, message, emailTo):
        mylog.error(message)
        send_email.Execute(emailTo=emailTo, emailSubject="Failed Stress Volume Rebalance", emailBody=message)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "minIOPS" : libsf.IsInteger,
                            "maxIOPS" : libsf.IsInteger,
                            "burstIOPS" : libsf.IsInteger,
                            "iteration" : libsf.IsInteger,
                            },
                    args)


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, clientIPs=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, emailTo=None, addSSH=False, bsCheck=True, debug=False, iteration=1, volumeSize=4000, minIOPS=10000, maxIOPS=100000, burstIOPS=100000, waitTime=300):


        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #if the client ips is empty skip the check on the clients
        check_client = True
        if not clientIPs:
            mylog.info("Skipping health check on clients")
            check_client = False

        node_list = get_active_nodes.Get(mvip=mvip, username=username, password=password)
        if(node_list == False):
            message = "Failied getting active nodes on " + mvip
            self.fail(message,emailTo)
            self._RaiseEvent(self.Events.NODES_NOT_FOUND)
            return False

        if(addSSH == True):
            if push_ssh_keys_to_node.Execute(node_ips=node_list):
                mylog.info("Pushed SSH Keys to Nodes")
                self._RaiseEvent(self.Events.PUSHED_SSH_KEYS)
            else:
                message = "Failed pushing SSH keys to nodes"
                self.fail(message, emailTo)
                return False
        else:
            mylog.info("Not pushing SSH Keys to Nodes")

        if(create_account.Execute(mvip=mvip, account_name=accountName) == True):
            mylog.info("New account Created on " + mvip)
        else:
            message = "Failed creating new account on " + mvip
            self.fail(message, emailTo)
            return False

        #iteration_count only used to display the current iteration
        iteration_count = 1

        #boolean to keep the loop running forever
        loop_forever = False
        if(iteration == 0):
            loop_forever = True
            mylog.warning("Looping Forever")

        #record the time of the start of the test
        start_time = time.time()

        while True:

            mylog.banner("Iteration Count: " + str(iteration_count))

            slice_list = list_slice_services.Get(mvip=mvip)
            if(slice_list == False):
                message = "Failied getting slice services " + mvip
                self.fail(message,emailTo)
                #self._RaiseEvent(self.Events.NODES_NOT_FOUND)
                return False

            #determine how many volumes to create based on the number of slice services
            slice_count = len(slice_list)
            volume_count = slice_count / 3
            if(volume_count < 1):
                volume_count = 1

            #create volumes, need to add more here
            mylog.step("Create Volumes")
            if(create_volumes.Execute(mvip=mvip, account_name=accountName, volume_prefix="huge", volume_count=volume_count, volume_size=volumeSize, min_iops=minIOPS, max_iops=maxIOPS, burst_iops=burstIOPS) == True):
                mylog.info(str(volume_count) + " volumes created")
            else:
                message = "Failed creating volumes"
                self.fail(message, emailTo)
                return False

            #wait 30 minutes
            mylog.info("Waiting 30 minutes for rebalancing")
            time.sleep(1800)

            #make sure the cluster is healthy
            mylog.step("Checking Health")
            if(check_cluster_health.Execute(mvip, since=start_time) == True):
                mylog.info("Cluster " + mvip + " is Healthy")
                self._RaiseEvent(self.Events.CLUSTER_HEALTHY)
            else:
                message = "Cluster " + mvip + " failed health check"
                self.fail(message, emailTo)
                self._RaiseEvent(self.Events.CLUSTER_NOT_HEALTHY)
                return False

            if bsCheck:
                mylog.step("Performing a Cluster BS Check")
                if clusterbscheck.Execute(mvip=mvip, username=username, password=password) == False:
                    message = mvip + ": FAILED Cluster BS Check"
                    self.fail(message, emailTo)
                    return False

            #Check the health of the clients
            if(check_client == True):
                if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                    mylog.info("Client is Healthy")
                    self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                else:
                    message = "Failed client health check"
                    self.fail(message, emailTo)
                    self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                    return False

            #delete volumes
            mylog.step("Deleting Volumes")
            if(delete_volumes.Execute(mvip=mvip, source_account=accountName, volume_prefix="huge", purge=True) == True):
                mylog.info("The Volumes that were created have been deleted")
            else:
                message = "Failed to delete newly created volumes"
                self.fail(message, emailTo)
                return False

            #wait 30 minutes
            mylog.info("Waiting 30 minutes for rebalancing")
            time.sleep(1800)

            #make sure cluster is healthy
            mylog.step("Checking Health")
            if(check_cluster_health.Execute(mvip, since=start_time) == True):
                mylog.info("Cluster " + mvip + " is Healthy")
                self._RaiseEvent(self.Events.CLUSTER_HEALTHY)
            else:
                message = "Cluster " + mvip + " failed health check"
                self.fail(message, emailTo)
                self._RaiseEvent(self.Events.CLUSTER_NOT_HEALTHY)
                return False

            if bsCheck:
                mylog.step("Performing a Cluster BS Check")
                if clusterbscheck.Execute(mvip=mvip, username=username, password=password) == False:
                    message = mvip + ": FAILED Cluster BS Check"
                    self.fail(message, emailTo)
                    return False

            #make sure clients are healthy
            if(check_client == True):
                if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                    mylog.info("Client is Healthy")
                    self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                else:
                    message = "Failed client health check"
                    self.fail(message, emailTo)
                    self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                    return False


            iteration_count += 1
            #if iteration is not set to 0 from the start we will decrement iteration
            if(loop_forever == False):
                iteration -= 1
                if(iteration <= 0):
                    break

            #wait before starting the next iteration
            if(waitTime > 0):
                mylog.step("Waiting for " + str(waitTime) + " seconds")
                time.sleep(waitTime)

        end_time = time.time()
        delta_time = libsf.SecondsToElapsedStr(end_time - start_time)

        #calc stats
        iteration_count -= 1
        num_of_nodes = len(node_list)
        time_per_iteration = (end_time - start_time) / iteration_count
        time_per_node = time_per_iteration / num_of_nodes

        time_per_iteration = libsf.SecondsToElapsedStr(time_per_iteration)
        time_per_node = libsf.SecondsToElapsedStr(time_per_node)

        emailBody = ("Number of Nodes:------ " + str(num_of_nodes) + 
                   "\nIteration Count:------ " + str(iteration_count) + 
                   "\nTime Per Iteration:--- " + time_per_iteration + 
                   "\nTime Per Node:-------- " + time_per_node +
                   "\nTotal Time:----------- " + delta_time)

        send_email.Execute(emailTo=emailTo, emailSubject=mvip + ": Finished Stress Volume Rebalance in " + delta_time, emailBody=emailBody)
        
        mylog.info("\tNumber of Nodes:     " + str(num_of_nodes))
        mylog.info("\tIteration Count:     " + str(iteration_count))
        mylog.info("\tTime Per Iteration:  " + time_per_iteration)
        mylog.info("\tTime Per Node:       " + time_per_node)
        mylog.info("\tTotal Time:          " + delta_time)

        mylog.passed("The Stress Volume Rebalance Test has passed")
        return True



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("-c", "--client_ips", action="list", dest="clientIPs", default=None, help="the IP addresses of the clients")
    parser.add_option("--add_ssh", action="store_false", dest="addSSH", default=False, help="Add the SSH key to each node")
    parser.add_option("--email_to", action="list", dest="emailTo", default=None, help="the list of email addresses to send to")
    parser.add_option("--client_user", type="string", dest="clientUser", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="clientPass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    parser.add_option("--iteration", type="int", dest="iteration", default=1, help="how many times to loop over the nodes, 0=forever")
    parser.add_option("--volume_size", type="int", dest="volumeSize", default=4000, help="The size of each volume to be created in GB")
    parser.add_option("--min_iops", type="int", dest="minIOPS", default=10000, help="The min iops for the new volume")
    parser.add_option("--max_iops", type="int", dest="maxIOPS", default=100000, help="The max iops for the new volume")
    parser.add_option("--burst_iops", type="int", dest="burstIOPS", default=100000, help="The burst iops for the new volume")
    parser.add_option("--wait_time", type="int", dest="waitTime", default=300, help="Wait time between each iteration")
    parser.add_option("--bs_check", action="store_true", dest="bs_check", default=False, help="Do a cluster BS check")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.clientIPs, options.clientUser, options.clientPass, options.waitTime, options.emailTo, options.addSSH, options.bs_check, options.debug, options.iteration, options.volumeSize, options.minIOPS, options.maxIOPS, options.burstIOPS, options.waitTime):
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
