#!/usr/bin/env python
"""
This script causes repeatable random stress on a cluster.

"""

import sys
import random
import time
import inspect
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults

import send_email
import push_ssh_keys_to_cluster
import push_ssh_keys_to_client


class StressRandomAction():

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "run_time" : libsf.IsInteger,
                            "client_ips" : libsf.IsValidIpv4AddressList
                            },
            args)

    def OnFailure(failureMessage):
        send_email.Execute(self.emailTo, "stress_random failure on " + self.mvip, failureMessage)

    def Execute(self, seed=None, run_time=None, email_to=None, mvip=sfdefaults.mvip, username=sfdefaults.username, password=sfdefaults.password, client_ips=None, client_user=sfdefaults.client_user, client_pass=sfdefaults.client_pass, list=False, debug=False):
        """
        Random cluster stress
        """
        # Setup the list of actions
        mylog.info("Searching for known cluster operations")
        import stress_random_ops as ops
        # Primary actions are the main perturbation that the script will execute
        primary_actions = []
        # Secondary operations are the other operations that the script will execute during the primary action
        secondary_operations = []
        for name, obj in inspect.getmembers(ops):
            if inspect.isclass(obj) and getattr(obj, "operation", None):
                if getattr(obj, "operation") == "primary":
                    primary_actions.append(obj)
                elif getattr(obj, "operation") == "secondary":
                    secondary_operations.append(obj)
        mylog.info("  Primary actions: " + ", ".join([o.__name__ for o in primary_actions]))
        mylog.info("  Secondary operations: " + ", ".join([o.__name__ for o in secondary_operations]))
        if list:
            return True

        #self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()


        self.mvip = mvip
        self.emailTo = email_to
        if seed == None:
            seed = int(round(time.time() * 1000))

        import pprint;
        pp = pprint.PrettyPrinter(indent=2)

        # Setup the shared random generator
        print
        mylog.banner("Using seed " + str(seed))
        print
        self.rnd = random.Random(seed)
        ops.rnd = self.rnd

        # Push my SSH keys to the nodes and clients
        if not sys.platform.lower().startswith('win'):
            if not push_ssh_keys_to_cluster.Execute(mvip, username, password):
                return False
        if client_ips:
            if not push_ssh_keys_to_client.Execute(client_ips, client_user, client_pass):
                return False

        cluster = ops.ClusterModel(mvip, username, password)
        iteration = 0
        while True:
            # Update the cluster model
            mylog.info("Updating cluster model")
            cluster.UpdateClusterConfig()

            mylog.info("Selecting primary action")
            current_action = self.rnd.choice(primary_actions)(options.debug)
            current_action.Init(cluster=cluster)

            mylog.info("Selecting secondary operations")
            num_ops = self.rnd.randint(1, len(secondary_operations))
            selected_ops = []
            for op_class in self.rnd.sample(secondary_operations, num_ops):
                selected_ops.append(op_class(options.debug))
            selected_ops = [secondary_operations[3](options.debug)]

            for op in selected_ops:
                op.Init(cluster=cluster)

            mylog.info("-------------------------------------------------------------------------------")
            mylog.info("The following operations are pending on the cluster")
            current_action.PrintOperation()
            for op in selected_ops:
                op.PrintOperation()
            mylog.info("-------------------------------------------------------------------------------")

            #print "Accounts"
            #for o in cluster.accountsToModify:
                #pp.pprint(o)
            #print "Volumes"
            #for o in cluster.volumesToModify:
                #pp.pprint(o)
            #print "Nodes"
            #for o in cluster.nodesToModify:
                #pp.pprint(o)

            # Start the primary action
            mylog.step("Starting primary action " + current_action.Name())
            current_action.Start()

            # Start all of the secondary ops
            for op in selected_ops:
                mylog.step("Starting secondary operation " + op.Name())
                op.Start()

            # Wait for all of the secondary ops to finish
            mylog.info("Waiting for secondary ops to finish")
            for op in selected_ops:
                try:
                    op.End()
                except Exception as e:
                    mylog.error(str(e))
                    return False
            mylog.info("All secondary operations have finished")

            #print "Accounts"
            #for o in cluster.accountsToModify:
                #pp.pprint(o)
            #print "Volumes"
            #for o in cluster.volumesToModify:
                #pp.pprint(o)
            #print "Nodes"
            #for o in cluster.nodesToModify:
                #pp.pprint(o)


            # Keep starting new secondary ops until the primary action is finished
            while not current_action.IsFinished():
                mylog.info("Primary action is still running")
                cluster.UpdateClusterConfig()
                mylog.info("Selecting secondary operations")
                num_ops = self.rnd.randint(1, len(secondary_operations))
                selected_ops = []
                for op_class in self.rnd.sample(secondary_operations, num_ops):
                    selected_ops.append(op_class(options.debug))
                #selected_ops = [secondary_operations[4]()]

                for op in selected_ops:
                    #mylog.info("Initializing secondary operation " + op.Name())
                    op.Init(cluster=cluster)

                mylog.info("-------------------------------------------------------------------------------")
                mylog.info("The following operations are pending on the cluster")
                current_action.PrintOperation()
                for op in selected_ops:
                    op.PrintOperation()
                mylog.info("-------------------------------------------------------------------------------")

                #print "Accounts"
                #for o in cluster.accountsToModify:
                    #pp.pprint(o)
                #print "Volumes"
                #for o in cluster.volumesToModify:
                    #pp.pprint(o)
                #print "Nodes"
                #for o in cluster.nodesToModify:
                    #pp.pprint(o)

                for op in selected_ops:
                    mylog.step("Starting secondary operation " + op.Name())
                    op.Start()

                time.sleep(1)
                mylog.info("Waiting for secondary ops to finish")
                for op in selected_ops:
                    try:
                        op.End()
                    except Exception as e:
                        mylog.error(str(e))
                        return False
                mylog.info("All secondary operations have finished")

            try:
                current_action.End()
                mylog.info("Finished primary action " + op.Name())
            except Exception as e:
                mylog.error(str(e))

            iteration += 1
            if iteration >= 1:
                break




        return True




if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-l", "--list", action="store_true", dest="list", default=False, help="show the list of stress operations")
    parser.add_option("-s", "--seed", type="int", dest="seed", default=None, help="the random seed to use")
    parser.add_option("-r", "--run_time", type="float", dest="run_time", default=None, help="how long to run the test (hours)")
    parser.add_option("-e", "--email_to", action="list", dest="email_to", default=None, help="the list of email addresses to send results to")
    parser.add_option("--max_secondary", type="int", dest="max_secondary", default=10, help="the maximum number of secondary operations to run in parallel [%default]")
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management VIP for the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the username for the cluster [%default]")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the password for the cluster [%default]")
    parser.add_option("-c", "--client_ips", action="list", dest="client_ips", default=None, help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=sfdefaults.client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=sfdefaults.client_pass, help="the password for the clients [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if options.debug:
            mylog.showDebug()
        stress = StressRandomAction()
        if stress.Execute(options.seed, options.run_time, options.email_to, options.mvip, options.username, options.password, options.client_ips, options.client_user, options.client_pass, options.list, options.debug):
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
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

