#!/usr/bin/python
"""
This action will get a list of VM names from vSphere

When run as a script, the following options apply:
   --bash
      Display a minimal output that is formatted as a space separated list

   --cluster
      Name of ESX cluster to search

   --csv
      Display a minimal output that is formatted as a comma separated list

   --datacenter
      Name of the datacenter to search

   --debug
      Display more verbose messages

   --folder
      Name of vm folder to search

   --mgmt_server
      The hostname/IP of the vCenter Server

   --pool
      Name of resource pool to search

   --recurse
      Include VMs in subfolders/pools

   --result_address
      Address of a ZMQ server listening for results (when run as a child process)

   --vm_count
      The number of matching virtual machines

   --vm_name
      The name of the virtual machine

   --vm_power
      The power state to match VMs (on, off)

   --vm_regex
      The regex to match names of virtual machines
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults
from lib.libsf import ChildScript
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class VmwareListVmNamesAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        pass

    def Get(self, *args, **kwargs):
        """
        Get a list of VM names
        """
        if '--debug' in args or 'debug' in kwargs:
            mylog.console.setLevel(logging.DEBUG)
        if '--bash' in args or 'bash' in kwargs or '--csv' in args or 'csv' in kwargs:
            mylog.silence = True

        script_args = " ".join(args)
        script_args += libsf.kwargsToCommandlineArgs(kwargs)

        try:
            script = ChildScript("perl -Ivmware_perl vmware_perl/vmware_list_vm_names.pl " + script_args)
            result = script.Run()
        except KeyboardInterrupt:
            return False
        except Exception as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        return result

    def Execute(self, *args, **kwargs):
        """
        Show a list of VM names
        """
        del self
        vm_list = Get(*args, **kwargs)
        if vm_list is False:
            return False

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == "__main__":
    mylog.debug("Starting " + str(sys.argv))

    try:
        timer = libsf.ScriptTimer()
        if Execute(*sys.argv[1:]):
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

