import sys
from optparse import OptionParser
import multiprocessing
import re
import time
import lib.libsf as libsf
from lib.libsf import mylog
import lib.XenAPI as XenAPI
import lib.libxen as libxen
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues
import xen_list_sr_names

class XenDestroySrAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        FAILURE = "FAILURE"

    def __init__(self):
        super(self.__class__,self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"vmhost" : libsf.IsValidIpv4Address,
                            "host_user" : None,
                            "host_pass" : None},
            args)

    def _DestroyThread(self, vmhost, host_user, host_pass, sr_name, results, debug):
        results[sr_name] = False

        session = None
        try:
            session = libxen.Connect(vmhost, host_user, host_pass)
        except libxen.XenError as e:
            mylog.error(str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        # Find the requested SR
        try:
            sr_ref_list = session.xenapi.SR.get_by_name_label(sr_name)
        except XenAPI.Failure as e:
            mylog.error("Could not find SR " + sr_name + " - " + str(e))
            self.RaiseFailureEvent(message=str(e), exception=e)
            return False

        if len(sr_ref_list) > 0:
            sr_ref = sr_ref_list[0]
            try:
                sr = session.xenapi.SR.get_record(sr_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get SR record for " + sr_name + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
        else:
            mylog.error("Could not find SR matching " + sr_name)
            return False

     # Get a list of PBDs for the SR
        pbd_ref_list = sr['PBDs']
        pbd_list = dict()
        for pbd_ref in pbd_ref_list:
            try:
                pbd = session.xenapi.PBD.get_record(pbd_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get PBD record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False
            pbd_list[pbd_ref] = pbd

        # Unplug each PBD
        mylog.info("Unplugging all of the PBDs for SR " + sr['name_label'])
        for pbd_ref in pbd_list.keys():
            host_ref = pbd_list[pbd_ref]['host']
            try:
                host = session.xenapi.host.get_record(host_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not get host record - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            mylog.debug("Unplugging PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'])
            success = False
            retry = 3
            while retry > 0:
                try:
                    session.xenapi.PBD.unplug(pbd_ref)
                    success = True
                    break
                except XenAPI.Failure as e:
                    if e.details[0] == "CANNOT_CONTACT_HOST":
                        time.sleep(30)
                        retry -= 1
                        continue
                    else:
                        mylog.error("Failed to unplug PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'] + " - " + str(e))
                        self.RaiseFailureEvent(message=str(e), exception=e)
                        return False
            if not success:
                mylog.error("Failed to unplug PBD")
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

        mylog.passed("Successfully unplugged SR " + sr['name_label'])

        try:
            session.xenapi.SR.forget(sr_ref)
            #session.xenapi.SR.destroy(sr_ref)
        except XenAPI.Failure as e:
            mylog.error("Could not Destroy the SR " + sr_name + " Error: " + str(e))
            return False
        mylog.passed("The SR " + sr_name + " was Destroyed")
        results[sr_name] = True


    def Execute(self, vmhost=sfdefaults.vmhost_xen, host_user=sfdefaults.host_user, host_pass=sfdefaults.host_pass, sr_name=None, sr_regex=None, parallel_thresh=sfdefaults.xenapi_parallel_calls_thresh, parallel_max=sfdefaults.xenapi_parallel_calls_max, debug=False):

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        if sr_name == None and sr_regex == None:
            mylog.error("sr_name and sr_regex cannot both be blank")
            return False
        if sr_name:
            if sr_regex:
                mylog.error("Only specify sr_name or sr_regex")
                return False

        #if the sr_name is given then only detach the one SR
        if sr_name:
            session = None
            try:
                session = libxen.Connect(vmhost, host_user, host_pass)
            except libxen.XenError as e:
                mylog.error(str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            # Find the requested SR
            try:
                sr_ref_list = session.xenapi.SR.get_by_name_label(sr_name)
            except XenAPI.Failure as e:
                mylog.error("Could not find SR " + sr_name + " - " + str(e))
                self.RaiseFailureEvent(message=str(e), exception=e)
                return False

            if len(sr_ref_list) > 0:
                sr_ref = sr_ref_list[0]
                try:
                    sr = session.xenapi.SR.get_record(sr_ref)
                except XenAPI.Failure as e:
                    mylog.error("Could not get SR record for " + sr_name + " - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
            else:
                mylog.error("Could not find SR matching " + sr_name)
                return False

            # Get a list of PBDs for the SR
            pbd_ref_list = sr['PBDs']
            pbd_list = dict()
            for pbd_ref in pbd_ref_list:
                try:
                    pbd = session.xenapi.PBD.get_record(pbd_ref)
                except XenAPI.Failure as e:
                    mylog.error("Could not get PBD record - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False
                pbd_list[pbd_ref] = pbd

            # Unplug each PBD
            mylog.info("Unplugging all of the PBDs for SR " + sr['name_label'])
            for pbd_ref in pbd_list.keys():
                host_ref = pbd_list[pbd_ref]['host']
                try:
                    host = session.xenapi.host.get_record(host_ref)
                except XenAPI.Failure as e:
                    mylog.error("Could not get host record - " + str(e))
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

                mylog.debug("Unplugging PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'])
                success = False
                retry = 3
                while retry > 0:
                    try:
                        session.xenapi.PBD.unplug(pbd_ref)
                        success = True
                        break
                    except XenAPI.Failure as e:
                        if e.details[0] == "CANNOT_CONTACT_HOST":
                            time.sleep(30)
                            retry -= 1
                            continue
                        else:
                            mylog.error("Failed to unplug PBD " + pbd_list[pbd_ref]['uuid'] + " from host " + host['name_label'] + " - " + str(e))
                            self.RaiseFailureEvent(message=str(e), exception=e)
                            return False
                if not success:
                    mylog.error("Failed to unplug PBD")
                    self.RaiseFailureEvent(message=str(e), exception=e)
                    return False

            mylog.passed("Successfully unplugged SR " + sr['name_label'])

            try:
                session.xenapi.SR.forget(sr_ref)
                #session.xenapi.SR.destroy(sr_ref)
            except XenAPI.Failure as e:
                mylog.error("Could not Destroy the SR " + sr_name + " Error: " + str(e))
                return False
            mylog.passed("The SR " + sr_name + " was Destroyed")
            return True


        #if the sr_regex was specified
        else:
            srList = None
            #get a list of the SRs
            srList = xen_list_sr_names.Get(sr_regex=sr_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass, debug=debug)
            if srList is None:
                mylog.error("No SRs with the regex: " + sr_regex + " were found")
                return False

            mylog.info(str(len(srList)) + " SRs will be destroyed")
            if len(srList) <= parallel_thresh:
                parallel_calls = 1
            else:
                parallel_calls = parallel_max

            manager = multiprocessing.Manager()
            results = manager.dict()
            self._threads = []

            for sr in srList:
                results[sr] = False
                th = multiprocessing.Process(target=self._DestroyThread, args=(vmhost, host_user, host_pass, sr, results, debug))
                th.daemon = True
                self._threads.append(th)

            #run all the threads
            allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)
            if allgood:
                mylog.passed("All SRs were destroyed")
                return True
            else:
                mylog.error("Not all SRs were destroyed")
                if parallel_max == 1:
                    return False
                else:
                    mylog.info("Going to try again with 1 thread")
                    srList = None
                    #get a list of the SRs
                    srList = xen_list_sr_names.Get(sr_regex=sr_regex, vmhost=vmhost, host_user=host_user, host_pass=host_pass, debug=debug)
                    if srList is None:
                        mylog.error("No SRs with the regex: " + sr_regex + " were found")
                        return False

                    mylog.info("Trying to destroy "+ str(len(srList)) + " SRs again")
                    parallel_calls = 1
                    manager = multiprocessing.Manager()
                    results = manager.dict()
                    self._threads = []

                    for sr in srList:
                        results[sr] = False
                        th = multiprocessing.Process(target=self._DestroyThread, args=(vmhost, host_user, host_pass, sr, results, debug))
                        th.daemon = True
                        self._threads.append(th)

                    #run all the threads
                    allgood = libsf.ThreadRunner(self._threads, results, parallel_calls)
                    if allgood:
                        mylog.passed("All SRs were destroyed on 2nd run")
                        return True
                    else:
                        mylog.error("Not all SRs were destroyed on 2nd run")
                        return False



# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--vmhost", type="string", dest="vmhost", default=sfdefaults.vmhost_xen, help="the management IP of the hypervisor [%default]")
    parser.add_option("--host_user", type="string", dest="host_user", default=sfdefaults.host_user, help="the username for the hypervisor [%default]")
    parser.add_option("--host_pass", type="string", dest="host_pass", default=sfdefaults.host_pass, help="the password for the hypervisor [%default]")
    parser.add_option("--sr_name", type="string", dest="sr_name", default=None, help="the name of the single SR to destroy")
    parser.add_option("--sr_regex", type="string", dest="sr_regex", default=None, help="the regex to match names of SRs to destroy")
    parser.add_option("--parallel_thresh", type="int", dest="parallel_thresh", default=sfdefaults.xenapi_parallel_calls_thresh, help="do not use multiple threads unless there are more than this many [%default]")
    parser.add_option("--parallel_max", type="int", dest="parallel_max", default=sfdefaults.xenapi_parallel_calls_max, help="the max number of threads to use [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.vmhost, options.host_user, options.host_pass, options.sr_name, options.sr_regex, options.parallel_thresh, options.parallel_max, options.debug):
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