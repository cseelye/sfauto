#!/usr/bin/python

# This script will display unaligned IOPS per volume on a cluster

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = "192.168.000.000"            # The management VIP of the cluster
                                    # --mvip

username = "admin"                  # Admin account for the cluster
                                    # --user

password = "password"              # Admin password for the cluster
                                    # --pass

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import time
import subprocess
import platform
import libsf
from libsf import mylog


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
        self.ReadOps = 0
        self.ReadIOPS = 0
        self.UnalignedReads = 0
        self.UnalignedReadIOPS = 0
        self.UnalignedReadPercent = 0
        self.WriteOps = 0
        self.WriteIOPS = 0
        self.UnalignedWrites = 0
        self.UnalignedWriteIOPS = 0
        self.UnalignedWritePercent = 0
        self.Timestamp = 0

def clear():
    p = subprocess.Popen( "cls" if platform.system() == "Windows" else "clear", shell=True)
    p.wait();

def main():
    # Parse command line arguments
    parser = OptionParser()
    global mvip, username, password
    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the management IP of the cluster")
    parser.add_option("--user", type="string", dest="username", default=username, help="the admin account for the cluster")
    parser.add_option("--pass", type="string", dest="password", default=password, help="the admin password for the cluster")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    mvip = options.mvip
    username = options.username
    password = options.password
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)
    if not libsf.IsValidIpv4Address(mvip):
        mylog.error("'" + mvip + "' does not appear to be a valid MVIP")
        sys.exit(1)

    previous_sample = dict()
    
    while True:
        # Get the list of accounts
        account_list = dict()
        acc_obj = libsf.CallApiMethod(mvip, username, password, "ListAccounts", {})
        for account in acc_obj["accounts"]:
            account_list[int(account["accountID"])] = account["username"]
    
        # Get the list of volumes
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
        stats_obj = libsf.CallApiMethod(mvip, username, password, "ListVolumeStatsByVolume", {})
        for vol in stats_obj["volumeStats"]:
            vol_id = int(vol["volumeID"])
            if vol_id not in volume_list.keys(): continue # skip deleted volumes
            volume_list[vol_id].AverageIOPSize = int(vol["averageIOPSize"])
            volume_list[vol_id].ActualIOPS = int(vol["actualIOPS"])
            volume_list[vol_id].ReadOps = int(vol["readOps"])
            volume_list[vol_id].WriteOps = int(vol["writeOps"])
            volume_list[vol_id].UnalignedReads = int(vol["unalignedReads"])
            volume_list[vol_id].UnalignedWrites = int(vol["unalignedWrites"])
            volume_list[vol_id].Timestamp = libsf.ParseDateTime(vol["timestamp"])
            if volume_list[vol_id].Timestamp == None: 
                raise Exception("Could not parse timestamp '" + vol["timestamp"] + "'")
            if volume_list[vol_id].ReadOps > 0: volume_list[vol_id].UnalignedReadPercent = (volume_list[vol_id].UnalignedReads * 100) / volume_list[vol_id].ReadOps
            if volume_list[vol_id].WriteOps > 0: volume_list[vol_id].UnalignedWritePercent = (volume_list[vol_id].UnalignedWrites * 100) / volume_list[vol_id].WriteOps
        
        # Calculate the differences
        for vol_id in volume_list.keys():
            if vol_id in previous_sample:
                previous = previous_sample[vol_id]
                current = volume_list[vol_id]
                volume_list[vol_id].ReadIOPS = (current.ReadOps - previous.ReadOps) / (current.Timestamp - previous.Timestamp).total_seconds()
                volume_list[vol_id].WriteIOPS = (current.WriteOps - previous.WriteOps) / (current.Timestamp - previous.Timestamp).total_seconds()
                volume_list[vol_id].UnalignedReadIOPS = (current.UnalignedReads - previous.UnalignedReads) / (current.Timestamp - previous.Timestamp).total_seconds()
                volume_list[vol_id].UnalignedWriteIOPS = (current.UnalignedWrites - previous.UnalignedWrites) / (current.Timestamp - previous.Timestamp).total_seconds()
        
        # Display the results
        clear()
        print "---------------------- Current Sample ----------------------------- | --------------------------- To Date ---------------------------"
        print "                                               Unaligned  Unaligned                        Unaligned  Unaligned  Unaligned  Unaligned"
        print "    VolumeName  VolumeID  ReadIOPS  WriteIOPS   ReadIOPS  WriteIOPS      Reads     Writes      Reads     Writes     Read %    Write %"
        for vol_id in volume_list.keys():            
            volume = volume_list[vol_id]
            print "%-14s  %8d  %8d  %9d  %9d  %9d  %9d  %9d  %9d  %9d  %9d  %9d" % (volume.Name, vol_id, volume.ReadIOPS, volume.WriteIOPS, volume.UnalignedReadIOPS, volume.UnalignedWriteIOPS, volume.ReadOps, volume.WriteOps, volume.UnalignedReads, volume.UnalignedWrites, volume.UnalignedReadPercent, volume.UnalignedWritePercent)
            
        
        # Save the results
        for vol_id in volume_list.keys():
            previous_sample[vol_id] = volume_list[vol_id]
        
        time.sleep(5)



if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print "\n\n"
        #mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)

