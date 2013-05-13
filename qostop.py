#!/usr/bin/python

"""
This action will watch QoS on active volumes

When run as a script, the following options/env variables apply:
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
import re
import subprocess
import platform
import time
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import lib.sfdefaults as sfdefaults

class VolumeDetails:
    def __init__(self):
        self.Name = ""
        self.Size = 0
        self.ID = ""
        self.enable512e = False
        self.AccountID = 0
        self.AccountName = ""
        self.ssLoad = 0
        self.ActualIOPS = 0
        self.AverageIOPSize = 0
        self.QosThrottle = 0
        self.ResultingAction = 0
        self.MaxIOPS = 0
        self.MinIOPS = 0
        self.BurstIOPS = 0

def clear():
    p = subprocess.Popen( "cls" if platform.system() == "Windows" else "clear", shell=True)
    p.wait()

def ValidateArgs(args):
    libsf.ValidateArgs({"mvip" : libsf.IsValidIpv4Address,
                        "username" : None,
                        "password" : None},
        args)

def Execute(mvip, username=sfdefaults.username, password=sfdefaults.password, debug=False):
    """
    Watch QoS on active volumes
    """
    ValidateArgs(locals())
    if debug:
        mylog.console.setLevel(logging.DEBUG)

    # Get the version of the cluster
    result = libsf.CallApiMethod(mvip, username, password, 'GetClusterVersionInfo', {})
    cluster_version = float(result["clusterVersion"])

    while True:
        # Get a list of accounts
        account_list = dict()
        acc_obj = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        for account in acc_obj["accounts"]:
            account_list[int(account["accountID"])] = account["username"]

        # Get the list of all volumes
        volume_list = dict()
        vols_obj = libsf.CallApiMethod(mvip, username, password, "ListActiveVolumes", {})
        for vol in vols_obj["volumes"]:
            v = VolumeDetails()
            v.Name = vol["name"]
            v.ID = int(vol["volumeID"])
            v.enable512e = vol["enable512e"]
            v.Size = int(vol["totalSize"])
            v.AccountID = int(vol["accountID"])
            v.AccountName = account_list[v.AccountID]
            if vol["qos"] != None:
                v.BurstIOPS = int(vol["qos"]["burstIOPS"])
                v.MaxIOPS = int(vol["qos"]["maxIOPS"])
                v.MinIOPS = int(vol["qos"]["minIOPS"])
            volume_list[v.ID] = v

        # Get the volume stats for all volumes
        if (cluster_version >= 4):
            stats_obj = libsf.CallApiMethod(mvip, username, password, "ListVolumeStatsByVolume", {})
            for vol in stats_obj["volumeStats"]:
                vol_id = int(vol["volumeID"])
                if vol_id not in volume_list.keys():
                    continue # skip deleted volumes
                if "averageIOPSize" in vol:
                    volume_list[vol_id].AverageIOPSize = int(vol["averageIOPSize"])
                    volume_list[vol_id].ActualIOPS = int(vol["actualIOPS"])
                    #volume_list[vol_id].QosThrottle = int(vol["throttle"])
        qos_report = libsf.HttpRequest("https://" + str(mvip) + "/reports/qos", username, password)
        #qos_report = qos_report.replace("<br />", "\n")
        for line in qos_report.split("\n"):
            m = re.search(r"volumeID=(\d+).+ssLoad\.GetServiceLoad\(\)=(\d+).+iopserror=(-?\d+).+bwerror=(-?\d+).+latency=(\d+).+resultingAction=(\d+)", line)
            if m:
                vol_id = int(m.group(1))
                volume_list[vol_id].ssLoad = int(m.group(2))
                volume_list[vol_id].ResultingAction = int(m.group(6))
                volume_list[vol_id].QosThrottle = float(m.group(6)) / volume_list[vol_id].maxIOPS

        # Display volumes that are being throttled
        throttled_volumes = []
        vol_name_len = 0
        #for vol_id in sorted(volume_list.keys(), key=lambda(k): volume_list[k].QosThrottle, reverse=True):
        for vol_id in sorted(volume_list.keys(), key=lambda(k): volume_list[k].QosThrottle):
            volume = volume_list[vol_id]
            if volume.QosThrottle > 0:
                throttled_volumes.append(volume)
                if len(volume.Name) > vol_name_len:
                    vol_name_len = len(volume.Name)
        clear()
        for volume in throttled_volumes:
            print (' %-' + str(vol_name_len) + 's') % volume.Name,
            print " throttle %5s" % str(volume.QosThrottle * 100) + "%" + "  resultingAction=%-3s" % str(volume.ResultingAction) + " ssLoad=" + str(volume.ssLoad),
            if volume.MaxIOPS > 0:
                print "maxIOPS=" + str(volume.MaxIOPS),
            if volume.ActualIOPS > 0:
                print "actualIOPS=" + str(volume.ActualIOPS),
            print

        print
        time.sleep(1)


def Abort():
    pass

if __name__ == '__main__':
    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
    parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
    parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.mvip, options.username, options.password, options.debug):
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

