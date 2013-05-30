"""
This action will preform a Random reboot stress test

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

    --iteration         how many times to loop over the nodes

"""

import lib.libsf as libsf
import logging
import lib.sfdefaults as sfdefaults
import time
import sys
import random
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
import count_available_drives
import send_email
import add_available_drives
import start_gc
import wait_for_gc


class StressRebootRandomAction(ActionBase):
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
        send_email.Execute(emailTo=emailTo, emailSubject="Failed Stress Reboot Random", emailBody=message)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "waitTime" : libsf.IsInteger
                            },
            args)


    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, clientIPs=None, clientUser=sfdefaults.client_user, clientPass=sfdefaults.client_pass, waitTime=300, emailTo=None, addSSH=False, debug=False, iteration=None):


        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #if the client ips is empty skip the check on the clients
        check_client = True
        if not clientIPs:
            mylog.info("Skipping health check on clients")
            check_client = False

        #get list of the nodes in the cluster
        mylog.step("Getting Active Nodes")
        node_list = get_active_nodes.Get(mvip=mvip, username=username, password=password)
        if(node_list == False):
            message = "Failied getting active nodes on " + mvip
            fail(message,emailTo)
            self._RaiseEvent(self.Events.NODES_NOT_FOUND)
            return False

        #if no iteration value is provided then each node is picked at random and rebooted once
        #else it will keep picking nodes at random until the number of interations have been met
        reboot_each_node_once = False
        if not iteration:
            iteration = len(node_list)
            reboot_each_node_once = True
        mylog.info("Will run for " + str(iteration) + " iterations")

        if(addSSH == True):
            if push_ssh_keys_to_node.Execute(node_ips=node_list):
                mylog.info("Pushed SSH Keys to Nodes")
                self._RaiseEvent(self.Events.PUSHED_SSH_KEYS)
            else:
                message = "Failed pushing SSH keys to nodes"
                fail(message, emailTo)
                return False
        else:
            mylog.info("Not pushing SSH Keys to Nodes")

        #if iteration is 0 we will loop over the nodes forever
        loop_forever = False
        iteration_count = 1
        if(iteration == 0):
            loop_forever = True
            iteration = 1
            mylog.warning("Looping Forever")

        #record the time of the start of the test
        start_time = time.time()

        #loop over each node
        while iteration > 0:

            mylog.banner("Iteration Count: " + str(iteration_count))

            #gets a random node from the node list
            random_node_index = random.randint(0,len(node_list) -1)
            node = node_list[random_node_index]

            mylog.step("Current Node is: " + str(node))

            #log the master node
            master_node = get_cluster_master.Get(mvip=mvip, username=username, password=password)
            if(master_node  == False):
                message = "Failed to get the master node on " + mvip
                mylog.error(message)
                self._RaiseEvent(self.Events.MASTER_NODE_NOT_FOUND)
            else:
                mylog.info("Master Node: " + str(master_node[0]))

            #reboot each node
            mylog.step("Rebooting Node")
            if(reboot_node.Execute(node_ip=node) == True):
                #mylog.info("Node: " + str(node) + " has been rebooted")
                self._RaiseEvent(self.Events.NODE_REBOOTED)
            else:
                message = "Node: " + str(node) + " has not been rebooted"
                fail(message, emailTo)
                self._RaiseEvent(self.Events.REBOOT_NODE_FAIL)
                return False

            #wait for faults to clear
            mylog.step("Wait for faults to clear")
            if(wait_for_no_faults.Execute(mvip) == True):
                #mylog.info("No faults found on " + mvip)
                self._RaiseEvent(self.Events.FAULTS_NOT_FOUND)
            else:
                message = "Faults found on " + mvip
                fail(message, emailTo)
                self._RaiseEvent(self.Events.FAULTS_FOUND)
                return False

            #make sure the cluster is healthy
            mylog.step("Check cluster health")
            if(check_cluster_health.Execute(mvip, since=start_time) == True):
                mylog.info("Cluster " + mvip + " is Healthy")
                self._RaiseEvent(self.Events.CLUSTER_HEALTHY)
            else:
                message = "Cluster " + mvip + " failed health check"
                fail(message, emailTo)
                self._RaiseEvent(self.Events.CLUSTER_NOT_HEALTHY)
                return False

            #Check the health of the clients
            if(check_client == True):
                mylog.step("Check client health")
                if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                    mylog.info("Client is Healthy")
                    self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                else:
                    message = "Failed client health check"
                    fail(message, emailTo)
                    self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                    return False

            #check to see if there are available drives because the node took too long to reboot
            mylog.step("Look for available drives")
            if(count_available_drives.Execute(expected=0, compare="gt", mvip=mvip) != True):

                #notify the user about this but continue the test
                send_email.Execute(emailTo=emailTo, emailSubject="Node " + str(node) + " took too long to reboot" )

                #add the drives back to the culster and wait for sync
                if(add_available_drives.Execute.Execute(mvip=mvip, username=username, password=password) == True):
                    mylog.info("Available drives were added to the cluster")
                else:
                    message = "Avaialbe drives were not added to the cluster"
                    fail(message, emailTo)
                    return False

                #check the health of the clients
                if(check_client == True):
                    mylog.step("Check client health")
                    if(check_client_health.Execute(client_ips=clientIPs, client_user=clientUser, client_pass=clientPass) == True):
                        mylog.info("Client is Healthy")
                        self._RaiseEvent(self.Events.CLIENT_HEALTHY)
                    else:
                        message = "Failed client health check"
                        fail(message, emailTo)
                        self._RaiseEvent(self.Events.CLIENT_NOT_HEALTHY)
                        return False


            #wait before going to the next node
            if(waitTime > 0):
                mylog.step("Waiting for " + str(waitTime) + " seconds")
                time.sleep(waitTime)

            #start gc to keep the cluster from filling up
            mylog.step("Garbage Collection")
            self._RaiseEvent(self.Events.BEFORE_START_GC)
            if(start_gc.Execute(mvip=mvip) == True):
                #mylog.info("GC started")
                pass

            else:
                message = "GC not started"
                fail(message, emailTo)
                return False

            #wait for gc to finish
            if(wait_for_gc.Execute(mvip=mvip) == True):
                mylog.info("GC finished")
                self._RaiseEvent(self.Events.GC_FINISHED)
            else:
                message = "GC failed to finish"
                fail(message, emailTo)
                self._RaiseEvent(self.Events.FAILURE)
                return False

            #remove the current node from the node list
            #each node is only rebooted once
            if(reboot_each_node_once == True):
                node_list.pop(random_node_index)

            #if loop_forever is false we decrement iteration to eventually break out of the loop
            if(loop_forever == False):
                iteration -= 1

            #increment iteration_count to display the current iteration
            iteration_count += 1

        end_time = time.time()
        delta_time = libsf.SecondsToElapsedStr(end_time - start_time)

        send_email.Execute(emailTo=emailTo, emailSubject="Finished Stress Reboot Random on: " + mvip +" in " + delta_time)

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
    parser.add_option("-c", "--clientIPs", action="list", dest="clientIPs", default=None, help="the IP addresses of the clients")
    parser.add_option("--waitTime", type="int", dest="waitTime", default=300, help="wait time after each node reboot")
    parser.add_option("--addSSH", action="store_false", dest="addSSH", default=False, help="Add the SSH key to each node")
    parser.add_option("--emailTo", action="list", dest="emailTo", default=None, help="the list of email addresses to send to")
    parser.add_option("--clientUser", type="string", dest="clientUser", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--clientPass", type="string", dest="clientPass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    parser.add_option("--iteration", type="int", dest="iteration", default=None, help="how many times to loop over the nodes")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.clientIPs, options.clientUser, options.clientPass, options.waitTime, options.emailTo, options.addSSH, options.debug, options.iteration):
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
