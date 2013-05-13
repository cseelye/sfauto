#!/usr/bin/env python
"""
SolidFire cluster objects and data structures
"""
import json
import libsf
from libsf import mylog
import re
import time
import libsfvolgroup
import libsfaccount
import libsfnode
import sfdefaults

class GCInfo(object):
    """
    Data structure containing information about a GC cycle
    """
    def __init__(self):
        self.StartTime = 0
        self.EndTime = 0
        self.DiscardedBytes = 0
        self.Rescheduled = False
        self.Generation = 0
        self.ParticipatingSSSet = set()
        self.EligibleBSSet = set()
        self.CompletedBSSet = set()

class SFCluster(object):
    """
    Common interactions with a SolidFire cluster
    """
    def __init__(self, mvip, username, password):
        self.mvip = mvip
        self.username = username
        self.password = password

    def GetLastGCInfo(self):
        """
        Get some information about the most recent garbage collection

        Args:
            mvip: the management VIP of the cluster
            username: the admin username of the cluster
            password: the admin password of the cluster

        Returns:
            A GCInfo object
        """

        gc_list = self.GetAllGCInfo()
        if gc_list:
            return gc_list[-1]
        else:
            return GCInfo()

        #event_list = libsf.CallApiMethod(mvip, username, password, 'ListEvents', {})
        #gc_info = GCInfo()
        #blocks_discarded = 0
        #for i in range(len(event_list['events'])):
        #    event = event_list['events'][i]
        #    if ("GCStarted" in event["message"]):
        #        details = event["details"]
        #        m = re.search(r"GC generation:(\d+).+eligibleBSs={(.+)}", details)
        #        if (m):
        #            if (int(m.group(1)) == gc_info.Generation):
        #                gc_info.StartTime = libsf.ParseTimestamp(event['timeOfReport'])
        #                gc_info.EligibleBSSet = set(map(int, m.group(2).split(",")))
        #                break
        #    if ("GCCompleted" in event["message"]):
        #        details = event["details"]
        #        pieces = details.split()
        #        if (gc_info.Generation <= 0):
        #            gc_info.Generation = int(pieces[0])
        #        if (int(pieces[0]) == gc_info.Generation):
        #            gc_info.CompletedBSSet.add(event["serviceID"])
        #            blocks_discarded += int(pieces[1])
        #            end_time = libsf.ParseTimestamp(event['timeOfReport'])
        #            if (end_time > gc_info.EndTime):
        #                gc_info.EndTime = end_time
        #gc_info.DiscardedBytes = blocks_discarded * 4096
        #return gc_info
        ""

    def GetAllGCInfo(self):
        """
        Get information about all of the recent garbage collections (all that are still in the cluster event list)

        Returns:
            A sorted list of GCInfo objects
        """

        gc_objects = dict()
        gc_info = GCInfo()
        event_list = libsf.CallApiMethod(self.mvip, self.username, self.password, 'ListEvents', {})
        # go through the list in chronological order
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if ("GCStarted" in event["message"]):
                gc_info = GCInfo()
                gc_info.StartTime = libsf.ParseTimestamp(event['timeOfReport'])
                m = re.search(r"GC generation:(\d+).+participatingSServices={(.+)}.+eligibleBSs={(.+)}", event["details"])
                if m:
                    gc_info.Generation = int(m.group(1))
                    gc_info.ParticipatingSSSet = set(map(int, m.group(2).split(",")))
                    gc_info.EligibleBSSet = set(map(int, m.group(3).split(",")))
                    gc_objects[gc_info.Generation] = gc_info

            if ("GCRescheduled" in event["message"]):
                m = re.search(r"GC rescheduled:(\d+)", event["details"])
                if m:
                    generation = int(m.group(1))
                    if generation in gc_objects:
                        gc_objects[generation].Rescheduled = True
                        gc_objects[generation].EndTime = libsf.ParseTimestamp(event['timeOfReport'])
                    else:
                        gc_info = GCInfo()
                        gc_info.Generation = generation
                        gc_info.StartTime = libsf.ParseTimestamp(event['timeOfReport'])
                        gc_info.Rescheduled = True
                        gc_objects[gc_info.Generation] = gc_info

            if ("GCCompleted" in event["message"]):
                pieces = event["details"].split(" ")
                generation = int(pieces[0])
                blocks_discarded = int(pieces[1])
                service_id = int(event["serviceID"])
                end_time = libsf.ParseTimestamp(event['timeOfReport'])
                if generation in gc_objects:
                    gc_objects[generation].CompletedBSSet.add(service_id)
                    gc_objects[generation].DiscardedBytes += (blocks_discarded * 4096)
                    if end_time > gc_objects[generation].EndTime:
                        gc_objects[generation].EndTime = end_time

        gc_list = []
        for gen in sorted(gc_objects.keys()):
            gc_list.append(gc_objects[gen])
        return gc_list

    def StartGC(self, force=False):
        """
        Start a GC cycle.  If one is already in progress, do not start another unless the force argument is True

        Args:
            force: start GC even if one is already in progress
        """

        # Find the most recent non-rescheduled GC
        mylog.info("Checking if GC is in progress")
        gc_in_progress = False
        gc_list = self.GetAllGCInfo()
        for gc_info in reversed(gc_list):
        #for i in range(len(gc_list)-1, -1, -1):
        #    gc_info = gc_list[i]
            if gc_info.Rescheduled:
                continue
            elif gc_info.EndTime <= 0:
                if time.time() - gc_info.StartTime > 60 * 30: # If it has been more than 30 min assume GC is not going to complete
                    gc_in_progress = False
                    break
                mylog.warning("GC generation " + str(gc_info.Generation) + " started at " + libsf.TimestampToStr(gc_info.StartTime) + " has not completed")
                gc_in_progress = True
                break
            else:
                gc_in_progress = False
                break

        if gc_in_progress and not force:
            mylog.info("Not starting a GC cycle because one is already in progresss")
            return

        # Ask the cluster to start GC
        mylog.info("Starting GC on " +self. mvip)
        request_time = time.time()
        time.sleep(2)
        libsf.CallApiMethod(self.mvip, self.username, self.password, "StartGC", {})

        # Wait for GC to start
        mylog.info("Waiting for GC to start")
        wait_start = time.time()
        while True:
            time.sleep(3)
            if time.time() - wait_start > 120:
                raise libsf.SfError("Timeout waiting for GC to start")

            event_list = libsf.CallApiMethod(self.mvip, self.username, self.password, 'ListEvents', {})
            for event in event_list["events"]:
                event_time = libsf.ParseTimestamp(event['timeOfReport'])
                if event_time < request_time:
                    continue

                if ("GCStarted" in event["message"]) and event_time > request_time:
                    break

                if ("GCRescheduled" in event["message"]) and event_time > request_time:
                    break

    def WaitForGC(self, timeout=60):
        """
        Wait for a GC cycle to complete.

        Args:
            timeout: stop waiting after this many minutes and consider the GC incomplete

        Returns:
            A GCInfo object describing the completed GC cycle
        """

        # Find the most recent non-rescheduled GC and wait for it to be complete
        while True:
            gc_list = self.GetAllGCInfo()
            for gc_info in reversed(gc_list):
                if gc_info.Rescheduled:
                    continue
                elif gc_info.EndTime <= 0:
                    if time.time() - gc_info.StartTime > 60 * timeout:
                        raise libsf.SfTimeoutError("Timeout waiting for GC to finish")
                    continue
                else:
                    return gc_info

    def IsBinSyncing(self):
        """
        Check if the cluster is bin syncing

        Returns:
            A boolean indicating if the cluster is syncing (True) or not (False)
        """
        version = libsf.CallApiMethod(self.mvip, self.username, self.password, "GetClusterVersionInfo", {})
        cluster_version = float(version["clusterVersion"])
        if cluster_version >= 5.0:
            # Get the bin assignments report
            result = libsf.HttpRequest("https://" + self.mvip + "/reports/bins.json", self.username, self.password)
            bin_report = json.loads(result)

            # Make sure that all bins are active and not syncing
            for bsbin in bin_report:
                for service in bsbin["services"]:
                    if service["status"] != "bsActive":
                        mylog.debug("Bin sync - one or more bins are not active")
                        return True
        else:
            # Get the bin syncing report
            result = libsf.HttpRequest("https://" + self.mvip + "/reports/binsyncing", self.username, self.password)
            if "<table>" in result:
                mylog.debug("Bin sync - entries in bin syncing report")
                return True

        # Make sure there are no block related faults
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListClusterFaults", {'faultTypes' : 'current'})
        for fault in result["faults"]:
            if fault["code"] == "blockServiceUnhealthy":
                mylog.debug("Bin sync - block related faults are present")
                return True

        return False

    def IsSliceSyncing(self):
        """
        Check if the cluster is slice syncing

        Returns:
            A boolean indicating if the cluster is syncing (True) or not (False)
        """
        version = libsf.CallApiMethod(self.mvip, self.username, self.password, "GetClusterVersionInfo", {})
        cluster_version = float(version["clusterVersion"])
        if cluster_version >= 5.0:
            # Get the slice assignments report
            result = libsf.HttpRequest("https://" + self.mvip + "/reports/slices.json", self.username, self.password)
            slice_report = json.loads(result)

            # Make sure there are no unhealthy services
            if "service" in slice_report:
                for ss in slice_report["services"]:
                    if ss["health"] != "good":
                        mylog.debug("Slice sync - one or more SS are unhealthy")
                        return True

            # Make sure there are no volumes with multiple live secondaries or dead secondaries
            if "slice" in slice_report:
                for vol in slice_report["slices"]:
                    if "liveSecondaries" not in vol:
                        mylog.debug("Slice sync - one or more volumes have no live secondaries")
                        return True
                    if len(vol["liveSecondaries"]) > 1:
                        mylog.debug("Slice sync - one or more volumes have multiple live secondaries")
                        return True
                    if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                        mylog.debug("Slice sync - one or more volumes have dead secondaries")
                        return True
        else:
            # Get the slice syncing report
            result = libsf.HttpRequest("https://" + self.mvip + "/reports/slicesyncing", self.username, self.password)
            if "<table>" in result:
                mylog.debug("Slice sync - entries in slice syncing report")
                return True

        # Make sure there are no slice related faults
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListClusterFaults", {'faultTypes' : 'current'})
        for fault in result["faults"]:
            if fault["code"] == "sliceServiceUnhealthy" or fault["code"] == "volumesDegraded":
                mylog.debug("Slice sync - slice related faults are present")
                return True

        return False

    def GetCurrentFaultSet(self):
        """
        Get a list of the current cluster faults

        Returns:
            A set of cluster fault codes (strings)
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListClusterFaults", {"exceptions": 1, "faultTypes": "current"})
        current_faults = set()
        if (len(result["faults"]) > 0):
            for fault in result["faults"]:
                if fault["code"] not in current_faults:
                    current_faults.add(fault["code"])
        return current_faults

    def CheckForEvent(self, eventString, since = 0):
        """
        Check if an event is present in the cluster event log

        Args:
            eventString: the event message to check for
            since: only check for events that were created after this time (integer unix timestamp)

        Returns:
            A boolean indicating if the event was found (True) or not (False)
        """
        event_list = libsf.CallApiMethod(self.mvip, self.username, self.password, 'ListEvents', {})

        for i in range(len(event_list['events'])):
            event = event_list['events'][i]
            if eventString in event['message']:
                event_time = libsf.ParseTimestamp(event['timeOfReport'])
                if (event_time > since):
                    return True
        return False

    def ListActiveNodeIPs(self):
        """
        Get a list of the active node IPs

        Returns:
            A list of active node management IPs (strings)
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveNodes", {})
        node_ips = []
        for node in result["nodes"]:
            node_ips.append(node["mip"])

        return sorted(node_ips)

    def ListActiveNodeIDs(self):
        """
        Get a list of the active node IDs

        Returns:
            A list of active node IDs (ints)
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveNodes", {})
        node_ids = []
        for node in result["nodes"]:
            node_ids.append(node["nodeID"])

        return sorted(node_ids)

    def ListActiveNodes(self):
        """
        Get a list of the active nodes

        Returns:
            A list of node dictionaries
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveNodes", {})
        return result["nodes"]

    def GetNode(self, nodeIP, sshUser=sfdefaults.ssh_user, sshPass=sfdefaults.ssh_pass):
        """
        Get the node with the given IP

        Args:
            nodeIP: the management IP addres of the node

        Returns:
            An SFNode object
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveNodes", {})
        for node in result["nodes"]:
            if node["mip"] == nodeIP:
                return libsfnode.SFNode(node["mip"], sshUser, sshPass, self.mvip, self.username, self.password)

        raise libsf.SfUnknownObjectError("Could not find node " + nodeIP)

    def FindAccount(self, accountName=None, accountID=None):
        """
        Find an account with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            accountName: the name of the account to find (string)
            accountID: the ID of the account to find (int)

        Returns:
            An SFAccount object
        """
        if not accountName and not accountID:
            raise libsf.SfArgumentError("Please specify either acountName or accountID")

        account_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListAccounts", {})
        if accountName:
            for account in account_list["accounts"]:
                if (account["username"].lower() == str(accountName).lower()):
                    return libsfaccount.SFAccount(account, self.mvip, self.username, self.password)
            raise libsf.SfError("Could not find account with name " + str(accountName))

        else:
            try:
                accountID = int(accountID)
            except TypeError:
                raise libsf.SfError("Please specify an integer for accountID")
            for account in account_list["accounts"]:
                if account["accountID"] == accountID:
                    return libsfaccount.SFAccount(account, self.mvip, self.username, self.password)
            raise libsf.SfError("Could not find account with ID " + str(accountID))

    def FindVolumeAccessGroup(self, volgroupName=None, volgroupID=None):
        """
        Find a volume access group with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            volgroupName: the name of the group to find (string)
            volgroupID: the ID of the group to find (int)

        Returns:
            An SFVolGroup object
        """
        if not volgroupName and not volgroupID:
            raise libsf.SfArgumentError("Please specify either volgroupName or volgroupID")

        vag_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListVolumeAccessGroups", {}, ApiVersion=5.0)
        if volgroupName:
            for vag in vag_list["volumeAccessGroups"]:
                if vag["name"].lower() == volgroupName.lower():
                    return libsfvolgroup.SFVolGroup(vag, self.mvip, self.username, self.password)
            raise libsf.SfError("Could not find group with name " + str(volgroupName))

        else:
            try:
                volgroupID = int(volgroupID)
            except ValueError:
                raise libsf.SfError("Please specify an integer for VagId")
            for vag in vag_list["volumeAccessGroups"]:
                if vag["volumeAccessGroupID"] == volgroupID:
                    return libsfvolgroup.SFVolGroup(vag, self.mvip, self.username, self.password)
            raise libsf.SfError("Could not find group with ID " + str(volgroupID))

    def GetActiveVolumes(self):
        """
        Get a list of all of the active volumes on the cluster

        Returns:
            A dictionary of volumeID (int) => volume info (dict)
        """

        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveVolumes", {})
        all_volumes = dict()
        for vol in result["volumes"]:
            all_volumes[vol["volumeID"]] = vol
        return all_volumes

    def SearchForVolumes(self, volumeID=None, volumeName=None, volumeRegex=None, volumePrefix=None, accountName=None, accountID=None, volumeCount=0):
        """
        Search for volumes with the given criteria

        Args:
            volumeID: a single volume ID (int) or list of volume IDs (ints)
            volumeName: a single volume name (string) or list of volume names (strings)
            volumeRegex: a regex to match volume names against (string)
            volumePrefix: a prefix to match volume names starting with (string)
            accountName: only match volumes that are in the account with this name (string)
            accountID : only match volumes that are in the account with this ID (iny)
            volumeCount: match at most this many volumes (int)

        Returns:
            A dictionary of volumeID (int) => volume info (dict)
        """

        all_volumes = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveVolumes", {})

        # Find the source account if the user specified one
        source_account_id = 0
        if accountName or accountID:
            if accountID:
                source_account_id = int(accountID)
            elif accountName:
                account_info = self.FindAccount(accountName=accountName)
                source_account_id = account_info["accountID"]
            params = {}
            params["accountID"] = source_account_id
            account_volumes = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListVolumesForAccount", params)

        found_volumes = dict()
        count = 0

        # Search for specific volume id or list of ids
        if volumeID:
            # Convert to a list if it is a scalar
            volume_id_list = []
            if isinstance(volumeID, basestring):
                volume_id_list = volumeID.split(",")
                volume_id_list = map(int, volume_id_list)
            else:
                try:
                    volume_id_list = list(volumeID)
                except ValueError:
                    volume_id_list.append(volumeID)

            for vid in volume_id_list:
                found = False
                for volume in all_volumes["volumes"]:
                    if int(volume["volumeID"]) == vid:
                        found = True
                        found_volumes[vid] = volume
                        break
                if not found:
                    raise libsf.SfError("Could not find volume '" + str(vid) + "'")

        # Search for a single volume name (or list of volume names) associated with a specific account
        elif volumeName and accountName:
            # Convert to a list if it is a scalar
            volume_name_list = []
            if isinstance(volumeName, basestring):
                volume_name_list = volumeName.split(",")
            else:
                try:
                    volume_name_list = list(volumeName)
                except ValueError:
                    volume_name_list.append(volumeName)

            for vname in volume_name_list:
                volume_id = 0
                found = False
                for volume in account_volumes["volumes"]:
                    if volume["name"] == vname:
                        if found:
                            mylog.warning("Duplicate volume name " + vname)
                        volume_id = int(volume["volumeID"])
                        found_volumes[volume_id] = volume
                        found = True
                if volume_id == None:
                    raise libsf.SfError("Could not find volume '" + vname + "' on account '" + accountName + "'")

        # Search for a single volume name (or list of volume names) across all volumes.
        # If there are duplicate volume names, the first match is taken
        elif volumeName:
            # Convert to a list if it is a scalar
            volume_name_list = []
            if isinstance(volumeName, basestring):
                volume_name_list = volumeName.split(",")
            else:
                try:
                    volume_name_list = list(volumeName)
                except ValueError:
                    volume_name_list.append(volumeName)

            for vname in volume_name_list:
                volume_id = 0
                found = False
                for volume in all_volumes["volumes"]:
                    if volume["name"] == vname:
                        if found:
                            mylog.warning("Duplicate volume name " + vname)
                        volume_id = int(volume["volumeID"])
                        found_volumes[volume_id] = volume
                        found = True
                if volume_id == None:
                    raise libsf.SfError("Could not find volume '" + vname + "'")

        # Search for regex match across volumes associated with a specific account
        elif volumeRegex and accountName:
            for volume in account_volumes["volumes"]:
                vol_id = int(volume["volumeID"])
                vol_name = volume["name"]
                m = re.search(volumeRegex, vol_name)
                if m:
                    found_volumes[vol_id] = volume
                    count += 1
                    if volumeCount > 0 and count >= volumeCount:
                        break

        # Search for regex match across all volumes
        elif volumeRegex:
            for volume in all_volumes["volumes"]:
                vol_id = int(volume["volumeID"])
                vol_name = volume["name"]
                m = re.search(volumeRegex, vol_name)
                if m:
                    found_volumes[vol_id] = volume
                    count += 1
                    if volumeCount > 0 and count >= volumeCount:
                        break

        # Search for matching volumes on an account
        elif volumePrefix and accountName:
            for volume in account_volumes["volumes"]:
                if volume["name"].lower().startswith(volumePrefix):
                    vol_id = int(volume["volumeID"])
                    vol_name = volume["name"]
                    found_volumes[vol_id] = volume
                    count += 1
                    if volumeCount > 0 and count >= volumeCount:
                        break

        # Search for all matching volumes
        elif volumePrefix:
            for volume in all_volumes["volumes"]:
                if volume["name"].lower().startswith(volumePrefix):
                    vol_id = int(volume["volumeID"])
                    found_volumes[vol_id] = volume
                    count += 1
                    if volumeCount > 0 and count >= volumeCount:
                        break

        # Search for all volumes on an account
        elif accountName:
            for volume in account_volumes["volumes"]:
                vol_id = int(volume["volumeID"])
                found_volumes[vol_id] = volume
                count += 1
                if volumeCount > 0 and count >= volumeCount:
                    break

        return found_volumes

    def CreateVolumeGroup(self, volgroupName):
        """
        Create a volume access group

        Args:
            name: the name of the group

        Returns:
            An SFVolGroup object
        """
        params = {}
        params["name"] = volgroupName
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "CreateVolumeAccessGroup", params, ApiVersion=5.0)

        return self.FindVolumeAccessGroup(volgroupID=result["volumeAccessGroupID"])

    def ListAvailableDrives(self):
        """
        Get a list of all the available drives
        """
        mylog.info("Searching for available drives...")
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListDrives", {})

        available = []
        for drive in result["drives"]:
            if drive["status"] == "available":
                mylog.debug("Found available driveID " + str(drive["driveID"]) + " (slot " + str(drive["slot"]) + ") from nodeID " + str(drive["nodeID"]))
                available.append(drive)

        return available

    def AddAvailableDrives(self):
        """
        Add all of the available drives to the cluster
        """
        available = self.ListAvailableDrives()
        if not available:
            mylog.info("There are no available drives to add")
            return 0

        params = dict()
        params["drives"] = []
        for drive in available:
            newdrive = {}
            newdrive["driveID"] = drive["driveID"]
            newdrive["type"] = "automatic"
            params["drives"].append(newdrive)

        mylog.info("Adding " + str(len(params["drives"])) + " drives to cluster")
        libsf.CallApiMethod(self.mvip, self.username, self.password, "AddDrives", params)
        return len(params["drives"])

    def StartSliceRebalancing(self):
        """
        Start slice rebalancing on the cluster
        """
        libsf.CallApiMethod(self.mvip, self.username, self.password, "RebalanceSlices", {})

    def ListPendingNodes(self):
        """
        Get a list of the pending nodes in the cluster
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListPendingNodes", {})
        return result["pendingNodes"]

    def GetClusterUsedSpace(self):
        """
        Get the usedSpace of the cluster
        """
        result = libsf.CallApiMethod(self.mvip, self.username, self.password, "GetClusterCapacity", {})
        return result["clusterCapacity"]["usedSpace"]

