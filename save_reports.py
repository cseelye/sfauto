#!/usr/bin/env python

"""
This action will save the cluster reports periodically

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var

    --folder            The folder to save the reports in

    --label             A label to prepend to the report files

    --interval          How often to save reports (-1 to only save one time instead of periodically)

    --reports           A list of reports to save
"""

import sys
from optparse import OptionParser
import json
import time
import datetime
import tarfile
import os
import re
import signal
import platform
import logging
import inspect
import multiprocessing
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SaveReportsAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_GATHER_REPORTS = "BEFORE_GATHER_REPORTS"
        AFTER_GATHER_REPORTS = "AFTER_GATHER_REPORTS"
        REPORT_FAILED = "REPORT_FAILED"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                            "username" : None,
                            "password" : None,
                            "interval" : libsf.IsInteger},
            args)

    def Shutdown(self):
        self.Abort()
        sys.exit(0)

    def Execute(self, mvip, folder="reports", label = None, interval=-1, reports=None, username=sfdefaults.username, password=sfdefaults.password, debug=False):
        """
        Save the cluster reports
        """

        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        # create the reports directory
        if (folder == None):
            logdir = "reports_" + mvip + "_" + time.strftime("%Y-%m-%d-%H-%M-%S")
        else:
            logdir = folder
        if (not os.path.exists(logdir)):
            os.makedirs(logdir)

        if label != None and len(label) > 0:
            label = label + "_"

        def ReportThread(file_name, url):
            try:
                report_html = libsf.HttpRequest(url, username, password)
                if (report_html == None):
                    return
                f = open(file_name, 'w')
                f.write(report_html)
                f.close()
            except KeyboardInterrupt:
                return
            except Exception:
                mylog.error("Failed to get report from " + url)
                self._RaiseEvent(self.Events.REPORT_FAILED)
                return

        def ApiThread(file_name, mvip, username, password, method):
            try:
                result = libsf.CallApiMethod(mvip, username, password, method, {})
                if result == None:
                    return
                stats_json = json.dumps(result)
                f = open(file_name, 'w')
                f.write(stats_json)
                f.close()
            except KeyboardInterrupt:
                return
            except Exception:
                mylog.error("Failed to get API call " + method)
                self._RaiseEvent(self.Events.REPORT_FAILED)
                return

        while True:

            self._RaiseEvent(self.Events.BEFORE_GATHER_REPORTS)

            # Make a list of reports to gather
            reports_to_get = []
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            if reports == None or len(reports) <= 0 or reports[0] == None or len(reports[0]) <= 0:
                mylog.info("Getting a list of available reports")
                base_url = "https://" + str(mvip) + "/reports"
                base_html = None
                try:
                    base_html = libsf.HttpRequest(base_url, username, password)
                except libsf.SfError:
                    pass
                if (base_html == None):
                    time.sleep(interval)
                    continue
                for m in re.finditer("href=\"(.+)\"", base_html):
                    url = "https://" + str(mvip) + m.group(1)
                    if "events" in url: continue # skip the event report
                    if "mutexes" in url: continue # skip the mutex timing report
                    if "rpcTimeReset" in url: continue # skip the rpc timing reset
                    reports_to_get.append(url)
            else:
                for report in reports:
                    url = "https://" + str(mvip) + "/reports/" + report
                    reports_to_get.append(url)

            # Create one thread per report
            mylog.info("Pulling reports from %s" % (mvip))
            self._threads = []
            report_files = []
            for url in reports_to_get:
                report_name = url.split("/")[-1]
                file_name = logdir + "/" + label + "report_" + timestamp + "_" + report_name + ".html"
                th = multiprocessing.Process(target=ReportThread, args=(file_name, url))
                th.daemon = True
                self._threads.append(th)
                report_files.append(file_name)

            # Pull a snapshot of some API stats as well
            if reports == None or len(reports) <= 0 or reports[0] == None or len(reports[0]) <= 0:
                api_calls = [
                    "GetCompleteStats",
                    "GetClusterCapacity",
                    "GetClusterInfo",
                    "GetClusterVersionInfo",
                    "ListActiveNodes"
                ]
                for method in api_calls:
                    file_name = logdir + "/" + label + "api_" + timestamp + "_" + method + ".json"
                    th = multiprocessing.Process(target=ApiThread, args=(file_name, mvip, username, password, method))
                    self._threads.append(th)
                    report_files.append(file_name)

            # Start all threads
            for th in self._threads:
                th.start()

            # Wait for all threads to finish
            for th in self._threads:
                th.join()

            # Tarball all of the reports together
            if len(report_files) > 1:
                mylog.info("Creating tarball")
                tar_name = logdir + "/" + label + "reports_" + timestamp + "_" + mvip + ".tgz"
                tgz = tarfile.open(tar_name, "w:gz")
                for file_name in report_files:
                    if os.path.exists(file_name):
                        tgz.add(file_name)
                tgz.close()
                for file_name in report_files:
                    if os.path.exists(file_name):
                        os.unlink(file_name)

            self._RaiseEvent(self.Events.AFTER_GATHER_REPORTS)
            if interval < 0:
                break
            if interval > 0:
                mylog.info("Waiting for %d seconds..." % (interval))
                time.sleep(interval)

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
    parser.add_option("--folder", type="string", dest="folder", default="bundles", help="the name of the directory to store the reports in.")
    parser.add_option("--label", type="string", dest="label", default="bundle", help="a label to prepend to the name of the report file.")
    parser.add_option("--interval", type="int", dest="interval", default=-1, help="how long to wait between each round of gathering reports, in sec. Use -1 to only save one time instead of periodically [%default]")
    parser.add_option("--reports", action="list", dest="reports", default=None, help="list of reports to save.  Default is all except the event and mutex timing reports")
    parser.add_option("--debug", action="store_true", default=False, dest="debug", help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    # Install signal handlers so we can run this script in the background
    signal.signal(signal.SIGINT, Shutdown)
    signal.signal(signal.SIGTERM, Shutdown)
    if "windows" not in platform.system().lower():
        signal.signal(signal.SIGHUP, Shutdown)

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.folder, options.label, options.interval, options.reports, options.username, options.password, options.debug):
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

