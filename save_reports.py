#!/usr/bin/env python

# This script saves a copy of cluster reports to a file at a specified interval

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.154.1"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

interval = 10                   # How long to wait between each round of gathering reports
                                # --interval

folder = "reports"              # The name of the directory to store the reports in.

label = ""                      # A label to prepend to the name of the report file
                                # --label

reports = [                     # A list of reports to save.  If this is empty, save all reports
]
# ----------------------------------------------------------------------------

import sys
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import datetime
import tarfile
import os
import re
import shutil
import multiprocessing
import libsf
from libsf import mylog


def main():
    global mvip, username, password, interval, folder, label, reports

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster. Default is " + str(username))
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster. Default is " + str(password))
    parser.add_option("--interval", type="int", dest="interval", default=interval, help="how long to wait between each round of gathering reports. Default is " + str(interval))
    parser.add_option("--folder", type="string", dest="folder", default=folder, help="the name of the directory to store the reports in.  Default is " + str(folder))
    parser.add_option("--label", type="string", dest="label", default=label, help="a label to prepend to the name of the report file")
    parser.add_option("--reports", type="string", dest="reports", default=reports, help="list of reports to save.  Default is all except the event and mutex timing reports")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    interval = options.interval
    folder = options.folder
    label = options.label
    reports = options.reports
    if (type(options.reports) is list):
        reports = options.reports
    else:
        pieces = reports.split(",")
        reports = []
        for report in pieces:
            report = report.strip()
            reports.append(report)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    # create the reports directory
    if (folder == None):
        logdir = "reports_" + mvip + "_" + time.strftime("%Y-%m-%d-%H-%M-%S")
    else:
        logdir = folder

    if (not os.path.exists(logdir)):
        os.makedirs(logdir)

    def Shutdown():
        exit(0)

    # Install signal handlers so we can run this script in the background
    import signal
    def shutdown_handler(signal, frame):
        Shutdown()
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGHUP, shutdown_handler)


    if label != None and len(label) > 0:
        label = label + "_"

    def ReportThread(file_name, timestamp, url):
        report_html = libsf.HttpRequest(url, username, password)
        if (report_html == None): return
        f = open(file_name, 'w')
        f.write(report_html)
        f.close()

    def ApiThread(file_name, timestamp, mvip, username, password, method):
        result = libsf.CallApiMethod(mvip, username, password, method, {})
        if result == None: return
        stats_json = json.dumps(result)
        f = open(file_name, 'w')
        f.write(stats_json)
        f.close()

    done = False
    while True:
        # Make a list of reports to gather
        reports_to_get = []
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        if reports == None or len(reports) <= 0 or reports[0] == None or len(reports[0]) <= 0:
            mylog.info("Getting a list of available reports")
            base_url = "https://" + str(mvip) + "/reports"
            base_html = libsf.HttpRequest(base_url, username, password)
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
        report_threads = []
        report_files = []
        for url in reports_to_get:
            report_name = url.split("/")[-1]
            file_name = logdir + "/" + label + "report_" + timestamp + "_" + report_name + ".html"
            th = multiprocessing.Process(target=ReportThread, args=(file_name, timestamp, url))
            report_threads.append(th)
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
                file_name = logdir + "/" + label + "api_" + timestamp + "_" + method + ".html"
                th = multiprocessing.Process(target=ApiThread, args=(file_name, timestamp, mvip, username, password, method))
                report_threads.append(th)
                report_files.append(file_name)

        # Start all threads
        for th in report_threads:
            th.start()

        # Wait for all threads to finish
        for th in report_threads:
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

        if interval < 0:
            exit(0)
        if interval > 0:
            mylog.info("Waiting for %d seconds..." % (interval))
            time.sleep(interval)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)



