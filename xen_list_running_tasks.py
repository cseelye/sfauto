#!/usr/bin/python

"""
This action will show the VMs on a XenServer hypervisor

When run as a script, the following options/env variables apply:
    --vmhost            The IP address of the hypervisor host

    --host_user         The username for the hypervisor

    --host_pass         The password for the hypervisor

    --cancel_all        Cancel all the running tasks
"""

import sys
from datetime import datetime
from optparse import OptionParser
import re
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import datetime

class XenListRunningTasksAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None},
            args)

    def Get(self, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List Tasks
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        # Connect to the host/pool
        mylog.info("Connecting to " + vmhost)
        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        
        task_list = {}
        
        mylog.info("Searching for running tasks on the Host/Pool")
        try:
            task_list = libxen.GetAllTasks(session)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False
        
        return task_list
        
    def Execute(self, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, debug=False):
        """
        List Tasks
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()

        del self
        running_tasks = Get(**locals())
        
        if running_tasks == False:
            mylog.error("There was an error getting the list of Running Tasks")
            return False
        else:
            session = libxen.Connect(vmhost, host_user, host_pass)

            for uuid in running_tasks:
                
                date, time  = running_tasks[uuid]["created"].value.split("T")
                Date = list(date);
                Time = list(time)
                Date.insert(4, '/')
                Date.insert(7, '/')
                D = "".join(Date)
                hostname = session.xenapi.host.get_record(running_tasks[uuid]["resident_on"])["name_label"]
                mylog.info(" name of task: " + running_tasks[uuid]["name_label"] + "\nTask Info:\t status: " + running_tasks[uuid]["status"] + "\t UUID: " + running_tasks[uuid]["uuid"] + \
                            "\t resident on: "+ hostname +"\t created on: "+str(D)+" "+time+ "\t Progress: " + str("{0:.2f}%".format(running_tasks[uuid]["progress"]*100)+ "\n"))
        
        session.xenapi.session.logout()
        return True
    
# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the Xen hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vmhost=options.vmhost, host_user=options.host_user, host_pass=options.host_pass, debug=options.debug):
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


