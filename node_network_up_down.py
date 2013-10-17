"""
This action will take the provided network interface up or down on a node

When run as a script, the following options/env variables apply:

    --node_ip       The IP address of the node

    --username      The username for the node

    --password      The password for the node

    --interface     The interface to be taken up or down, '1g' or '10g'

    --action        The action to take on the interface, 'Up' or 'down'
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase



class NodeNetworkUpDownAction(ActionBase):
    class Events:
        FAILURE = "FAILURE"
        INTERFACE_DOWN = "INTERFACE_DOWN"
        INTERFACE_UP = "INTERFACE_UP_AND_RUNNING"

    def __init__(self):
        super(self.__class__,self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"nodeIP" : libsf.IsValidIpv4Address,
                        "username" : None,
                        "password" : None
                        },
        args)
        if(args["interface"] != "10g"):
            if(args["interface"] != "1g"):
                raise libsf.SfArgumentError("Invalid value for expected: wrong input for interface use: '10g' or '1g'")

        if(args["action"] != "up"):
            if(args["action"] != "down"):
                raise libsf.SfArgumentError("Invalid value for expected: wring input for action use: 'up' or 'down'")



    def Execute(self, nodeIP, username=sfdefaults.username, password=sfdefaults.password, interface=None, action=None, debug=False):
        """
        Take a network interface up or down on a node
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        #API calls for the 10G interface
        if(interface == "10g" and action == "down"):
            try:
                result = libsf.CallNodeApiMethod(nodeIP, username, password, "SetNetworkInterfaceStatus", {"interfaces":["Bond10G"], "status":"Down", "minutes": 5})
                self._RaiseEvent(self.Events.INTERFACE_DOWN)
            except libsf.SfError as e:
                mylog.error("Failed to take the Bond10G network down on: " + nodeIP + ": " + e.message)
                self._RaiseEvent(self.Events.FAILURE)
                return False
        elif(interface == "10g" and action == "up"):
            try:
                result = libsf.CallNodeApiMethod(nodeIP, username, password, "SetNetworkInterfaceStatus", {"interfaces":["eth0","eth1","Bond10G"], "status":"UpAndRunning", "minutes": 5})
                self._RaiseEvent(self.Events.INTERFACE_UP)
            except libsf.SfError as e:
                mylog.error("Failed to bring the Bond10G network up on: " + nodeIP + ": " + e.message)
                self._RaiseEvent(self.Events.FAILURE)
                return False

        #API calls for the 1G interface
        elif(interface == "1g" and action == "down"):
            try:
                result = libsf.CallNodeApiMethod(nodeIP, username, password, "SetNetworkInterfaceStatus", {"interfaces":["Bond1G"], "status":"Down", "minutes": 5})
                self._RaiseEvent(self.Events.INTERFACE_DOWN)
            except libsf.SfError as e:
                mylog.error("Failed to take the Bond1G network down on: " + nodeIP + ": " + e.message)
                self._RaiseEvent(self.Events.FAILURE)
                return False
        elif(interface == "1g" and action == "up"):
            try:
                result = libsf.CallNodeApiMethod(nodeIP, username, password, "SetNetworkInterfaceStatus", {"interfaces":["eth0", "eth1", "Bond1G"], "status":"UpAndRunning", "minutes": 5})
                self._RaiseEvent(self.Events.INTERFACE_UP)
            except libsf.SfError as e:
                mylog.error("Failed to bring the Bond1G network up on: " + nodeIP + ": " + e.message)
                self._RaiseEvent(self.Events.FAILURE)
                return False
        else:
            mylog.error("Wrong combination of Interfaces and Up or Down")
            self._RaiseEvent(self.Events.FAILURE)
            return False

        #print result
        mylog.info("The " + interface + " interface has been taken " + action + " on: " + nodeIP)
        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--node_ip", type="string", dest="nodeIP", default=None, help="the IP address of the node")
    parser.add_option("-u", "--username", type="string", dest="username", default=sfdefaults.username, help="the admin account for the node")
    parser.add_option("-p", "--password", type="string", dest="password", default=sfdefaults.password, help="the admin password for the node")
    parser.add_option("--interface", type="string", dest="interface", default=None, help="Which network interface to take up or down, use: '10g' or '1g'")
    parser.add_option("--action", type="string", dest="action", default=None, help="Take the network interface up or down, use: 'up' or 'down'")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.nodeIP, options.username, options.password, options.interface, options.action, options.debug):
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
