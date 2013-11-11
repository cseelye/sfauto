#!/usr/bin/python

"""
This action will checkout nodes from AT2

It will loop and wait for the nodes to be available and then check them out

When run as a script, the following options/env variables apply:
    --user              AT2 username

    --pass              AT2 password

    --resource_ids      AT2 resource IDs of the nodes to checkout

    --checkout_note     The checkout note to use in AT2
"""

import sys
from optparse import OptionParser
import multiprocessing
import time
sys.path.append("..")
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
import logging
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class CheckoutNodesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def _CheckoutThread(self, resourceID, checkoutNote, username, password):
        mylog.info("Checking out resource " + str(resourceID))
        while True:
            try:
                libsf.CallApiMethod("autotest2.solidfire.net", username, password, "CheckOutNodes", {"note" : checkoutNote, "nodes" : [resourceID]}, ApiVersion=1.0)
                break
            except libsf.SfApiError as e:
                if "ResourceInUse" in e.name:
                    continue
                mylog.error("CheckOutNodes failed: " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return 1
            time.sleep(1)
        mylog.info("Successfully checked out resource " + str(resourceID))
        return 0

    def ValidateArgs(self, args):
        libsf.ValidateArgs({
                            "username" : None,
                            "password" : None,
                            "checkout_note" : None,
                            "resource_ids" : libsf.IsIntegerList,
                            },
            args)

    def Execute(self, resource_ids=None, checkout_note="", username=None, password=None, debug=False):
        """
        Checkout resources, waiting for them to be available if necessary
        """
        if resource_ids == None:
            resource_ids = []
        self.ValidateArgs(locals())
        if debug:
            mylog.showDeug()
        else:
            mylog.hideDebug()

        resource_ids = map(int, resource_ids)
        for rid in resource_ids:
            th = multiprocessing.Process(name="checkout-" + str(rid), target=self._CheckoutThread, args=(rid, checkout_note, username, password))
            th.daemon = True
            th.start()
            self._threads.append(th)

        # Wait for all threads to complete
        for th in self._threads:
            th.join()

        allgood = True
        for th in self._threads:
            if th.exitcode != 0:
                allgood = False
                break

        if allgood:
            mylog.passed("Successfully checked out " + str(len(resource_ids)) + " nodes");
            return True
        else:
            mylog.error("Failed to check out all resources")
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-u", "--user", type="string", dest="username", default=None, help="the username of the AT2 account to check out the nodes to")
    parser.add_option("-p", "--pass", type="string", dest="password", default=None, help="the password of the AT account")
    parser.add_option("--checkout_note", type="string", dest="note", default=None, help="the checkout note to use")
    parser.add_option("--resource_ids", action="list", dest="resource_ids", default=None, help="the IDs of the nodes to check out")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(resource_ids=options.resource_ids, checkout_note=options.note, username=options.username, password=options.password, debug=options.debug):
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
