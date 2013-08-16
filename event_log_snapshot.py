#!/usr/bin/python

# This script will extract the current Event Log from an SF cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

#mvip = "192.168.138.2"        # The management VIP of the cluster
mvip = "192.168.139.116"        # The management VIP of the cluster
                                # --mvip

username = "admin"              # Admin account for the cluster
                                # --user

password = "solidfire"          # Admin password for the cluster
                                # --pass

outputfile = "EventLog_3"       # Name of the output file for the Event Log
                                # --output

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import datetime
import lib.libsf
from lib.libsf import mylog


def main():
    global mvip, username, password, outputfile

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "mvip", "username", "password", "outputfile" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--output", type="string", dest="outputfile", default=outputfile, help="the output file for the Event Log")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")

    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    outputfile = options.outputfile
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not lib.libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    mylog.info("Management VIP = " + mvip)
    mylog.info("Username       = " + username)
    mylog.info("Password       = " + password)
    mylog.info("Event Log file = " + outputfile)

    # Open the file that will contain the Event Log
    log = open(outputfile, 'w')

    # Indicate that a GCStarted entry has not been found
    gcstarted = 0

    # Get Event Log
    event_list = lib.libsf.CallApiMethod(mvip, username, password, "ListEvents", {})
    print("Length of Event Log = " + str(len(event_list['events'])))

    # Go through the Event Log from the oldest to the most recent time entry and examine each event
    for i in range(len(event_list['events'])-1, -1, -1):
        event = event_list['events'][i]
        details = str(event["details"])

        # If a GCStarted entry has been seen, set the flag
        if "GCStarted" in event["message"]:
            gcstarted = 1

        # When GCCompleted entries appear, write out the first one and ignore the others (keeps the output file shorter)
        if "GCCompleted" in event["message"]:
            if gcstarted == 1:
                gcstarted = 0
                log.write(str(event["timeOfReport"]) + "  " + event["message"] + "  NodeID=" + str(event["nodeID"]) +  "  DriveID=" + str(event["driveID"]) + "  ServiceID=" + str(event["serviceID"]) +"  "+ event["eventInfoType"] + "  ...plus multiple GCCompleted messages\n")
                continue
            else:
                continue

        # Strip the "u'" characters out of each event before writing to the output file
        if "u" in str(event["details"]):
            details = details.replace("u\'", "")
            details = details.replace("\'", "")

        log.write(str(event["timeOfReport"]) + "  " + event["message"] + "  NodeID=" + str(event["nodeID"]) +  "  DriveID=" + str(event["driveID"]) + "  ServiceID=" + str(event["serviceID"]) + "  " + event["eventInfoType"] + "  " + details + "\n")


    log.close()
    exit(0)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = lib.libsf.ScriptTimer()
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


