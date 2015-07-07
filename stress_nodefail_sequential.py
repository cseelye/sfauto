"""
This action will preform a Sequential reboot stress test

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

    --addSSH            Boolean to add SSH keys to nodes

    --iteration         how many time to loop over the nodes, 0=forever
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
import wait_for_no_faults
import check_client_health
import check_cluster_health
import send_email
import add_available_drives
import start_gc
import wait_for_gc
import get_node_drive_count
import power_off_node
import power_on_node
import get_node_ipmi_ip
import clusterbscheck

class StressNodefailSequentialAction(ActionBase):
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
        send_email.Execute(emailTo=emailTo, emailSubject="Failed Stress Nodefail Sequential", emailBody=message)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "waitTime" : libsf.IsInteger,
                            "iteration" : libsf.IsInteger
                            },
            args)


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, clientIPs=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, waitTime=300, emailTo=None, addSSH=False, bsCheck=True, debug=False, iteration=1):


        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #if the client ips is empty skip the check on the clients
        check_client = True
        if not clientIPs:
            mylog.info("Skipping health check on clients")
            check_client = False


        #get list of the nodes in the cluster
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

        #get the IPMI IP address for each node
        drac_list = []

        for node in node_list:
            temp_drac = get_node_ipmi_ip.Get(node_ip=node)
            if(temp_drac == False):
                message = "Error getting IPMI IP for " + str(node)
                self.fail(message, emailTo)
                return False
            else:
                drac_list.append(temp_drac)


        #make sure there are no faults before starting the test
        #wait for faults to clear
        if(wait_for_no_faults.Execute(mvip) == True):
            #mylog.info("No faults found on " + mvip)
            self._RaiseEvent(self.Events.FAULTS_NOT_FOUND)
        else:
            message = "Faults found on " + mvip
            self.fail(message, emailTo)
            self._RaiseEvent(self.Events.FAULTS_FOUND)
            return False

        #record the time of the start of the test
        start_time = time.time()

        #iteration_count only used to display the current iteration
        iteration_count = 1
        loop_forever = False
        if(iteration == 0):
            loop_forever = True
            mylog.warning("Looping Forever")

        while True:

            mylog.banner("Interation Count: " + str(iteration_count))

            #loop over each node
            for node in node_list:
                mylog.step("Current Node is: " + str(node))
                node_index = node_list.index(node)

                #log the master node
                master_node = get_cluster_master.Get(mvip=mvip, username=username, password=password)
                if(master_node  == False):
                    message = "Failed to get the master node on " + mvip
                    mylog.error(message)
                    self._RaiseEvent(self.Events.MASTER_NODE_NOT_FOUND)
                else:
                    mylog.info("Master Node: " + str(master_node[0]))

                drive_count = get_node_drive_count.Get(node_ip=node, mvip=mvip)
                if(drive_count == False):
                    #error getting drive count
                    return False

                #power off the node
                if(power_off_node.Execute(node_ip=node, ipmi_ip=drac_list[node_index]) == True):
                    mylog.info("Node: " + str(node) + " powered off")
                else:
                    message = "No: " + str(node) + " powered off failed"
                    self.fail(message, emailTo)
                    return False

                #wait for the services to be decommissioned and synced out of the cluster
                mylog.step("Waiting for 10 minutes")
                time.sleep(600)

                #wait for faults to clear
                if(wait_for_no_faults.Execute(mvip) == True):
                    #mylog.info("No faults found on " + mvip)
                    self._RaiseEvent(self.Events.FAULTS_NOT_FOUND)
                else:
                    message = "Faults found on " + mvip
                    self.fail(message, emailTo)
                    self._RaiseEvent(self.Events.FAULTS_FOUND)
                    return False


                #Check the health of the clients
                if(check_client == True):
                    mylog.step("Checking Health")
                    if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                        mylog.info("Client is Healthy")
                        self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                    else:
                        message = "Failed client health check"
                        self.fail(message, emailTo)
                        self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                        return False

                #bring the node back up
                mylog.step("Trying to power on the node")
                if(power_on_node.Execute(node_ip=node, ipmi_ip=drac_list[node_index]) == True):
                    mylog.info("Node: " + str(node) + " is back online")
                else:
                    message = "Node: " + str(node) + " is not back online"
                    self.fail(message, emailTo)
                    return False

                #wait for the node to be fully up and drives available
                mylog.step("Wait for the node to be fully up and drives available")
                if(wait_for_available_drives.Execute(mvip=mvip, drive_count=drive_count) == True):
                    mylog.info("All drives are available")
                else:
                    message = "All drives are not available"
                    self.fail(message, emailTo)
                    return False

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

                #add the drives back to the culster and wait for sync
                mylog.step("Add drives back to the cluster and wait for sync")
                if(add_available_drives.Execute.Execute(mvip=mvip, username=username, password=password) == True):
                    mylog.info("Available drives were added to the cluster")
                else:
                    message = "Avaialbe drives were not added to the cluster"
                    self.fail(message, emailTo)
                    return False

                    #check the health of the clients
                    if(check_client == True):
                        if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                            mylog.info("Client is Healthy")
                            self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                        else:
                            message = "Failed client health check"
                            self.fail(message, emailTo)
                            self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                            return False


                #wait before going to the next node
                if(waitTime > 0):
                    mylog.step("Waiting for " + str(waitTime) + " seconds")
                    time.sleep(waitTime)

                #start gc to keep the cluster from filling up
                mylog.step("Start Garbage Collection")
                self._RaiseEvent(self.Events.BEFORE_START_GC)
                if(start_gc.Execute(mvip=mvip) == True):
                    mylog.info("GC started")
                else:
                    message = "GC not started"
                    self.fail(message, emailTo)
                    return False

                #wait for gc to finish
                if(wait_for_gc.Execute(mvip=mvip) == True):
                    mylog.info("GC finished")
                    self._RaiseEvent(self.Events.GC_FINISHED)
                else:
                    message = "GC failed to finish"
                    self.fail(message, emailTo)
                    self._RaiseEvent(self.Events.FAILURE)
                    return False

            iteration_count += 1
            #condition to end while loop, decrements iteration by one each time
            if(loop_forever == False):
                iteration -= 1
                if(iteration <= 0):
                    break

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

        send_email.Execute(emailTo=emailTo, emailSubject=mvip + ": Finished Stress Nodefail Sequential in " + delta_time, emailBody=emailBody)

        mylog.info("\tNumber of Nodes:     " + str(num_of_nodes))
        mylog.info("\tIteration Count:     " + str(iteration_count))
        mylog.info("\tTime Per Iteration:  " + time_per_iteration)
        mylog.info("\tTime Per Node:       " + time_per_node)
        mylog.info("\tTotal Time:          " + delta_time)

        mylog.passed("The Stress Nodefail Sequential Test has passed")

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
    parser.add_option("--wait_time", type="int", dest="waitTime", default=300, help="wait time after each node reboot")
    parser.add_option("--add_ssh", action="store_false", dest="addSSH", default=False, help="Add the SSH key to each node")
    parser.add_option("--email_to", action="list", dest="emailTo", default=None, help="the list of email addresses to send to")
    parser.add_option("--client_user", type="string", dest="clientUser", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="clientPass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    parser.add_option("--bs_check", action="store_true", dest="bs_check", default=False, help="Do a cluster BS check")
    parser.add_option("--iteration", type="int", dest="iteration", default=1, help="how many iterations to loop over the nodes, 0=forever")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.clientIPs, options.clientUser, options.clientPass, options.waitTime, options.emailTo, options.addSSH, options.bs_check, options.debug, options.iteration):
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
