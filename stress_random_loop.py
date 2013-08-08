"""
This action will run random stress tests in a loop

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --emailTo          List of addresses to send email to

    --iterations         how many times to loop over the stress tests, 0=forever
"""


import sys
import time
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import send_email
import random

import stress_netbounce_sequential
import stress_nodefail_sequential
import stress_reboot_master
import stress_reboot_random
import stress_reboot_sequential
import stress_volume_rebalance
import get_active_nodes


class StressRandomLoopAction(ActionBase):
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
                            "iterationCount" : libsf.IsInteger,
                            "emailTo" : None},
            args)

    def Execute(self, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, iterationCount=100, emailTo=None, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if iterationCount == 0:
            mylog.warning("Looping Forever")
            count = 10
        else:
            count = iterationCount

        stress_test = ["stress_netbounce_sequential", "stress_nodefail_sequential", "stress_reboot_master", "stress_reboot_random", "stress_reboot_sequential", "stress_volume_rebalance"]

        nodes_list = get_active_nodes.Get(mvip=mvip, username=username, password=password)
        if nodes_list == False:
            mylog.error("Could not get the list of active nodes")
            return False
        start_time = time.time()
        for i in xrange(0, count):
            random_index = random.randint(0, len(stress_test) - 1)
            random_iteration = random.randint(1,10)

            if iterationCount == 0:
                mylog.banner("Starting " + stress_test[random_index].replace("_", " ").title() + " on " + mvip + " with " + str(random_iteration) + " iterations" + "\nIteration " + str(i) + " of infinity")
            else:
                mylog.banner("Starting " + stress_test[random_index].replace("_", " ").title() + " on " + mvip + " with " + str(random_iteration) + " iterations" + "\nIteration " + str(i) + " of " + str(iterationCount))
            
            try:
                if stress_test[random_index] == "stress_netbounce_sequential":
                    stress_netbounce_sequential.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)


                #mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, emailTo=None, iteration=1
                if stress_test[random_index] == "stress_nodefail_sequential":
                    if len(nodes_list) <=3:
                        mylog.banner("Skipping Stress Nodefail Sequential because there are not enough nodes")
                    else:
                        stress_nodefail_sequential.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)

                #mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, emailTo=None, iteration=1
                elif stress_test[random_index] == "stress_reboot_master":
                    stress_reboot_master.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)

                #mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, emailTo=None, iteration=1
                elif stress_test[random_index] == "stress_reboot_random":
                    stress_reboot_random.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)

                #mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, emailTo=None, iteration=1
                elif stress_test[random_index] == "stress_reboot_sequential":
                    stress_reboot_sequential.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)

                #mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, emailTo=None, iteration=1
                elif stress_test[random_index] == "stress_volume_rebalance":
                    stress_volume_rebalance.Execute(mvip=mvip, username=username, password=password, iteration=random_iteration, emailTo=emailTo)

            except Exception as e:
                mylog.error("Could not preform " + stress_test[random_index].replace("_", " ").title())
                send_email.Execute(emailTo=emailTo, emailSubject="Test " + stress_test[random_index].replace("_", " ").title() + " failed", emailBody=str(e))

            mylog.step("Waiting 2 minutes")
            time.sleep(120)

            #if loopfoever then increase iterationCount by 1 each time so we never end the for loop
            if iterationCount == 0:
                count += 1

        end_time = time.time()
        delta_time = libsf.SecondsToElapsedStr(end_time - start_time)
        ave_time_per_iteration = (end_time - start_time) / (i + 1) 
        ave_time_per_iteration = libsf.SecondsToElapsedStr(ave_time_per_iteration)

        mylog.info("\tTotal Time:                   " + delta_time)
        mylog.info("\tNumber of Iterations:         " + str(i + 1))
        mylog.info("\tAverage Time Per Iteration:   " + ave_time_per_iteration)

        emailBody = "The stress tests ran for " + delta_time + "\nTotal Iterations     " + str(i + 1) + "\nAverage Time Per Iteration     " + ave_time_per_iteration
        send_email.Execute(emailTo=emailTo, emailSubject="The Testing Finished", emailBody=emailBody)
        mylog.passed("Passed " + str(iterationCount) + " iterations of random stress testing")  
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
    parser.add_option("--iterations", type="int", dest="iterations", default=100, help="How many iterations to loop over. 0 = Forever")
    parser.add_option("--email_to", type="string", dest="email_to", default=None, help="The email account to send the results / updates to")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.iterations, options.email_to, options.debug):
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
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)

