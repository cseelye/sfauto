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
        config_file = "sfevents.json"
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
            mylog.info("New wait time = " + str(wait_time) + " min")

        if folder != config_json["folder"]:
            folder = config_json["folder"]
            mylog.info("New output folder = " + folder)

        if purge_threshold != config_json["purge_threshold"]:
            purge_threshold = config_json["purge_threshold"]
            mylog.info("New purge threshold = " + str(purge_threshold) + " days")

        # Remove any reports older than the threshold
        libsf.RunCommand("find " + folder + " -type f -mtime +" + str(purge_threshold) + " -name \"event*\" | xargs rm")

        for mvip in mvip_list:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            label = ""
            file_name = folder + "/" + "events_" + timestamp + "_" + mvip + ".html"
            url = "https://" + str(mvip) + "/reports/events"
            
            mylog.info("Getting event report from " + mvip)
            report_html = libsf.HttpRequest(url, username, password)
            if (report_html == None): return
            f = open(file_name, 'w')
            f.write(report_html)
            f.close()

        # Wait for wait_time minutes
        mylog.info("SFEvents waiting for " + str(wait_time) + " min before gathering next event report")
        time.sleep(wait_time * 60)

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
