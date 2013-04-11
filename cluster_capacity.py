#!/usr/bin/python

# This script will show and save the cluster capacity/fullness stats

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

name = ""

# ----------------------------------------------------------------------------

import sys,os
import time
from optparse import OptionParser
import libsf
from libsf import mylog

def main():
    global mvip, username, password, name

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
    parser.add_option("--name", type="string", dest="name", default=name, help="a name to tag the output file with")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    name = options.name
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    filename = "clusterCapacity_" + name + "_" + libsf.TimestampToStr(time.time(), "%Y-%m-%d-%H-%M-%S") + ".csv"
    header = "Timestamp,DateTime,ProvisionedSpace,UnprovisionedSpace,MaxProvisionedSpace,UsedSpace,UnusedSpace,MaxUsedSpace,sumTotalClusterBytes,sumUsedClusterBytes,fullness"
    global log
    log = open(filename, 'w')
    log.write(header + "\n")
    log.flush()

    linecount = 0
    while(True):
        linecount += 1
        timestamp = time.time()
        datetime = libsf.TimestampToStr(timestamp)

        capacity_obj = libsf.CallApiMethod(mvip, username, password, "GetClusterCapacity", {})
        full_obj = libsf.CallApiMethod(mvip, username, password, "GetClusterFullThreshold", {})

        max_prov = capacity_obj["clusterCapacity"]["maxProvisionedSpace"]
        max_used = capacity_obj["clusterCapacity"]["maxUsedSpace"]
        prov = capacity_obj["clusterCapacity"]["provisionedSpace"]
        used = capacity_obj["clusterCapacity"]["usedSpace"]
        unprov = max_prov - prov
        unused = max_used - used

        sum_total = full_obj["sumTotalClusterBytes"]
        sum_used = full_obj["sumUsedClusterBytes"]
        fullness = full_obj["fullness"]

        outline = str(timestamp) + "," + str(datetime) + "," + str(prov) + "," + str(unprov) + "," + str(max_prov) + "," + str(used) + "," + str(unused) + "," + str(max_used) + "," + str(sum_total) + "," + str(sum_used) + "," + str(fullness)

        if (linecount == 1 or linecount%20 == 0):
            sys.stdout.write(header + "\n")
        sys.stdout.write(outline + "\n")
        sys.stdout.flush()

        log.write(outline + "\n")
        log.flush()

        time.sleep(10)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = libsf.ScriptTimer()
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        global log
        log.flush()
        log.close()
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)
