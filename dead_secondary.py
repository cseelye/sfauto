#!/usr/bin/env python

# This script will look for failed primary/secondary slices and save the report when one is found

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.0"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "password"          # Admin password for the cluster
                                # --pass

interval = 0                   # How long to wait between each round of gathering reports
                                # --interval

folder = "reports"               # The name of the directory to store the reports in.
                                # --folder

# ----------------------------------------------------------------------------


import sys
from optparse import OptionParser
import json
import urllib2
import random
import platform
import time
import tarfile
import os
import shutil
import re
import libsf
from libsf import mylog

def main():
    global mvip, username, password, interval, folder

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--interval", type="int", dest="interval", default=interval, help="how long to wait between each round of gathering reports")
    parser.add_option("--folder", type="string", dest="folder", default=folder, help="the name of the directory to store the reports in.  Default is reports_timestamp")

    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    interval = options.interval
    folder = options.folder
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

    done = False
    while True:
        mylog.info("======================================================================================")
        try:
            #mylog.info("Getting the slice report from " + str(mvip))

            url = "https://" + str(mvip) + "/reports/slices"
            html = libsf.HttpRequest(url, username, password)
            if (html == None):
                time.sleep(interval)
                continue

            save = False
            lines = html.split("</tr>")
            for line in lines:
                pieces = line.split("</td>")
                m = re.search("<tr><td>(\d+)", pieces[0])
                if m:
                    slice_id = m.group(1)
                else:
                    continue

                if ("FFFF00" in pieces[1]):
                    mylog.warning("Volume " + str(slice_id) + " primary is degraded")
                    save = True
                if ("FA8072" in pieces[1]):
                    mylog.warning("Volume " + str(slice_id) + " primary is bad")
                    save = True

                if ("FFFF00" in pieces[2]):
                    mylog.warning("Volume " + str(slice_id) + " secondary is live/degraded")
                    save = True
                if ("FFFF00" in pieces[2]):
                    mylog.warning("Volume " + str(slice_id) + " secondary is live/bad")
                    save = True

                if ("90EE90" in pieces[3]):
                    mylog.warning("Volume " + str(slice_id) + " secondary is dead/good")
                    save = True
                if ("FFFF00" in pieces[3]):
                    mylog.warning("Volume " + str(slice_id) + " secondary is dead/degraded")
                    save = True
                if ("FFFF00" in pieces[3]):
                    mylog.warning("Volume " + str(slice_id) + " secondary is dead/bad")
                    save = True



            if save:
                file_name = logdir + "/" + timestamp + "-slices.html"
                file = open(file_name, 'w')
                file.write(report_html)
                file.close()

                tgz = tarfile.open(logdir + "/" + timestamp + "_slicereport_" + mvip + ".tgz", "w:gz")
                tgz.add(file_name)
                tgz.close()
                os.unlink(file_name)

            #mylog.info("Waiting for %d seconds..." % (interval))
            time.sleep(interval)
        except KeyboardInterrupt: break
        except Exception, e:
            continue


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

