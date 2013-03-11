import sys
# little hack to get to libs in the parent directory
sys.path.insert(0, "..")
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


def ReportThread(file_name, url, username, password):
    report_html = libsf.HttpRequest(url, username, password)
    if (report_html == None): return
    f = open(file_name, 'w')
    f.write(report_html)
    f.close()

def ApiThread(file_name, mvip, username, password, method):
    result = libsf.CallApiMethod(mvip, username, password, method, {})
    if result == None: return
    stats_json = json.dumps(result)
    f = open(file_name, 'w')
    f.write(stats_json)
    f.close()

def main():
    mvip_list = []
    username = ""
    password = ""
    wait_time = 60
    folder = ""
    purge_threshold = 5
    while True:
        # Read configuration file
        config_file = "sfreporter.json"
        mylog.info("Reading config file " + config_file)
        config_handle = open(config_file, "r")

        # Remove comments from JSON before loading it
        config_lines = config_handle.readlines();
        new_config_text = ""
        for line in config_lines:
            line = re.sub("(//.+)", "", line)
            new_config_text += line
        config_json = json.loads(new_config_text)

        # Make sure all required params are present
        required_keys = ["mvips", "username", "password", "wait_time", "folder"]
        error = False
        for key in required_keys:
            if not key in config_json.keys():
                mylog.error("Missing required key '" + key + "' from config file")
                error = True
        if error: exit(1)

        # Read config and update/log any that are different
        if mvip_list != config_json["mvips"]:
            mvip_list = config_json["mvips"]
            mvip_list.sort()
            mylog.info("New MVIP list = " + ",".join(mvip_list))

        if username != config_json["username"]:
            username = config_json["username"]
            mylog.info("New username = " + username)

        if password != config_json["password"]:
            password = config_json["password"]
            mylog.info("New password  = " + password)

        if wait_time != config_json["wait_time"]:
            wait_time = config_json["wait_time"]
            mylog.info("New wait time = " + str(wait_time) + " sec")

        if folder != config_json["folder"]:
            folder = config_json["folder"]
            mylog.info("New output folder = " + folder)

        if purge_threshold != config_json["purge_threshold"]:
            purge_threshold = config_json["purge_threshold"]
            mylog.info("New purge threshold = " + str(purge_threshold) + " days")

        # Remove any reports older than the threshold
        libsf.RunCommand("find " + folder + " -type f -mtime +" + str(purge_threshold) + " -name \"report*\" | xargs rm")

        for mvip in mvip_list:
            # Make a list of reports to gather
            reports_to_get = []
            mylog.info("Getting a list of available reports")
            base_url = "https://" + str(mvip) + "/reports"
            base_html = libsf.HttpRequest(base_url, username, password)
            if (base_html == None):
                time.sleep(wait_time)
                continue
            for m in re.finditer("href=\"(.+)\"", base_html):
                url = "https://" + str(mvip) + m.group(1)
                if "events" in url: continue # skip the event report
                if "mutexes" in url: continue # skip the mutex timing report
                if "rpcTimeReset" in url: continue # skip the rpc timing reset
                if "socketservice" in url: continue # skip the socket service report
                if "slicebalance" in url: continue # skip the socket service report
                reports_to_get.append(url)
    
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            label = ""
            
            # Create one thread per report
            mylog.info("Pulling reports from %s" % (mvip))
            report_threads = []
            report_files = []
            for url in reports_to_get:
                report_name = url.split("/")[-1]
                file_name = folder + "/" + label + "report_" + timestamp + "_" + report_name + ".html"
                th = multiprocessing.Process(target=ReportThread, args=(file_name, url, username, password))
                report_threads.append(th)
                report_files.append(file_name)
    
            # Pull a snapshot of some API stats as well
            api_calls = [
                "GetCompleteStats",
                "GetClusterCapacity",
                "GetClusterInfo",
                "GetClusterVersionInfo",
                "ListActiveNodes"
            ]
            for method in api_calls:
                file_name = folder + "/" + label + "api_" + timestamp + "_" + method + ".html"
                th = multiprocessing.Process(target=ApiThread, args=(file_name, mvip, username, password, method))
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
                tar_name = folder + "/" + label + "reports_" + timestamp + "_" + mvip + ".tgz"
                tgz = tarfile.open(tar_name, "w:gz")
                for file_name in report_files:
                    if os.path.exists(file_name):
                        tgz.add(file_name)
                tgz.close()
                for file_name in report_files:
                    if os.path.exists(file_name):
                        os.unlink(file_name)







        # Wait for wait_time seconds
        mylog.info("Reporter waiting for " + str(wait_time) + " sec before gathering next report bundle")
        time.sleep(wait_time)

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
