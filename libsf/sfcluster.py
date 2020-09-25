#!/usr/bin/env python2.7
"""
SolidFire cluster objects and data structures
"""

import copy
import json
import re
import time
from . import sfdefaults
from . import util
from . import SolidFireClusterAPI, GetHighestAPIVersion, SolidFireError, TimeoutError, UnknownObjectError
from .sfvolgroup import SFVolGroup
from .sfaccount import SFAccount
from .sfnode import DriveType, SFNode
from .sfclusterpair import SFClusterPair
from .logutil import GetLogger
import six

class StartClusterPairInfo(object):
    def __init__(self, jsonResult=None):
        self.ID = 0
        self.key = ""
        if jsonResult:
            self.key = jsonResult["key"]
            self.ID = jsonResult["clusterPairID"]

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

class DriveState(object):
    """State of drives in cluster"""
    Any = "any"
    Active = "active"
    Available = "available"
    Failed = "failed"
    Removing = "removing"

class SFCluster(object):
    """Common interactions with a SolidFire cluster"""

    def __init__(self, mvip, username, password):
        self.mvip = mvip
        self.username = username
        self.password = password
        self.log = GetLogger()

        self.api = SolidFireClusterAPI(self.mvip,
                                       self.username,
                                       self.password,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)
        self._unpicklable = ["log", "api"]

    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.items():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()
        self.api = SolidFireClusterAPI(self.mvip,
                                       self.username,
                                       self.password,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)
        for key in self._unpicklable:
            assert hasattr(self, key)

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

    def GetAllGCInfo(self):
        """
        Get information about all of the recent garbage collections (all that are still in the cluster event list)

        Returns:
            A sorted list of GCInfo objects
        """

        gc_objects = dict()
        gc_info = GCInfo()
        event_list = self.api.CallWithRetry('ListEvents', {})
        # go through the list in chronological order
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if ("GCStarted" in event["message"]):
                gc_info = GCInfo()
                gc_info.StartTime = util.ParseTimestamp(event['timeOfReport'])
                if isinstance(event["details"], six.string_types):
                    m = re.search(r"GC generation:(\d+).+participatingSServices={(.+)}.+eligibleBSs={(.+)}", event["details"])
                    if m:
                        gc_info.Generation = int(m.group(1))
                        gc_info.ParticipatingSSSet = set([int(ssid) for ssid in m.group(2).split(",")])
                        gc_info.EligibleBSSet = set([int(bsid) for bsid in m.group(3).split(",")])
                else:
                    gc_info.Generation = event["details"]["generation"]
                    gc_info.ParticipatingSSSet = set(event["details"]["participatingSS"])
                    gc_info.EligibleBSSet = set(event["details"]["eligibleBS"])
                gc_objects[gc_info.Generation] = gc_info

            if ("GCRescheduled" in event["message"]):
                m = re.search(r"GC rescheduled:(\d+)", event["details"])
                if m:
                    generation = int(m.group(1))
                    if generation in gc_objects:
                        gc_objects[generation].Rescheduled = True
                        gc_objects[generation].EndTime = util.ParseTimestamp(event['timeOfReport'])
                    else:
                        gc_info = GCInfo()
                        gc_info.Generation = generation
                        gc_info.StartTime = util.ParseTimestamp(event['timeOfReport'])
                        gc_info.Rescheduled = True
                        gc_objects[gc_info.Generation] = gc_info

            if ("GCCompleted" in event["message"]):
                if isinstance(event["details"], six.string_types):
                    pieces = event["details"].split(" ")
                    generation = int(pieces[0])
                    blocks_discarded = int(pieces[1])
                else:
                    generation = event["details"]["generation"]
                    blocks_discarded = event["details"]["discardedBlocks"]
                service_id = int(event["serviceID"])
                end_time = util.ParseTimestamp(event['timeOfReport'])
                if generation in gc_objects:
                    gc_objects[generation].CompletedBSSet.add(service_id)
                    gc_objects[generation].DiscardedBytes += (blocks_discarded * 4096)
                    if end_time > gc_objects[generation].EndTime:
                        gc_objects[generation].EndTime = end_time

        gc_list = []
        for gen in sorted(gc_objects.keys()):
            gc_list.append(gc_objects[gen])
        return gc_list

    def IsGCInProgress(self):
        """
        Checks to see if GC is currently in running

        Returns:
            boolean indicating if GC is running or not
        """

        self.log.debug("Checking if GC is in progress")
        gc_in_progress = False
        gc_list = self.GetAllGCInfo()
        for gc_info in reversed(gc_list):

            if gc_info.Rescheduled:
                continue
            elif gc_info.EndTime <= 0:
                if time.time() - gc_info.StartTime > 60 * 90: # If it has been more than 90 min assume GC is not going to complete
                    gc_in_progress = False
                    break
                self.log.warning("GC generation {} started at {} has not completed".format(gc_info.Generation, util.TimestampToStr(gc_info.StartTime)))
                gc_in_progress = True
                break
            else:
                gc_in_progress = False
                break

        return gc_in_progress

    def StartGC(self, force=False):
        """
        Start a GC cycle.  If one is already in progress, do not start another unless the force argument is True

        Args:
            force: start GC even if one is already in progress
        """

        # Find the most recent non-rescheduled GC
        self.log.info("Checking if GC is in progress")
        gc_in_progress = False
        gc_list = self.GetAllGCInfo()
        for gc_info in reversed(gc_list):
            if gc_info.Rescheduled:
                continue
            elif gc_info.EndTime <= 0:
                if time.time() - gc_info.StartTime > 60 * 90: # If it has been more than 90 min assume GC is not going to complete
                    gc_in_progress = False
                    break
                self.log.warning("GC generation {} started at {} has not completed".format(gc_info.Generation, util.TimestampToStr(gc_info.StartTime)))
                gc_in_progress = True
                break
            else:
                gc_in_progress = False
                break

        if gc_in_progress and not force:
            self.log.info("Not starting a GC cycle because one is already in progress")
            return

        # Ask the cluster to start GC
        self.log.info("Starting GC on {}".format(self. mvip))
        request_time = time.time()
        time.sleep(sfdefaults.TIME_SECOND * 2)
        self.api.CallWithRetry("StartGC", {})

        # Wait for GC to start
        self.log.info("Waiting for GC to start")
        wait_start = time.time()
        started = False
        while not started:
            time.sleep(sfdefaults.TIME_SECOND * 3)
            if time.time() - wait_start > 120:
                raise TimeoutError("Timeout waiting for GC to start")

            event_list = self.api.CallWithRetry('ListEvents', {})
            for event in event_list["events"]:
                event_time = util.ParseTimestamp(event['timeOfReport'])
                if event_time < request_time:
                    continue

                if ("GCStarted" in event["message"]):
                    started = True
                    break

                if ("GCRescheduled" in event["message"]):
                    started = True
                    break

    def WaitForGC(self, timeout=90):
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
                        raise TimeoutError("Timeout waiting for GC to finish")
                    break
                else:
                    return gc_info
            time.sleep(sfdefaults.TIME_SECOND * 30)

    def ListReports(self):
        """
        Get a list of the available reports on this cluster

        Returns:
            A list of reports (list of str)
        """
        reports = []
        html = self.api.HttpDownload("/reports")
        for m in re.finditer("href=\"(.+)\"", html):
            pieces = m.group(1).split("/")
            reports.append(pieces[-1])
        return reports

    def GetReport(self, report):
        """
        Get the specified cluster report

        Args:
            report:     the relative URL of the report (after MVIP/reports/...)

        Returns:
            the contents of the report (str)
        """
        return self.api.HttpDownload("/reports/{}".format(report))

    def IsBinSyncing(self):
        """
        Check if the cluster is bin syncing

        Returns:
            A boolean indicating if the cluster is syncing (True) or not (False)
        """
        if GetHighestAPIVersion(self.mvip, self.username, self.password) >= 5.0:
            # Get the bin assignments report
            result = self.api.HttpDownload("/reports/bins.json")
            bin_report = json.loads(result)

            # Make sure that all bins are active and not syncing
            for bsbin in bin_report:
                for service in bsbin["services"]:
                    if service["status"] != "bsActive":
                        self.log.debug("Bin sync - one or more bins are not active")
                        return True
        else:
            # Get the bin syncing report
            result = self.api.HttpDownload("/reports/binsyncing")
            if "<table>" in result:
                self.log.debug("Bin sync - entries in bin syncing report")
                return True

        # Make sure there are no block related faults
        result = self.api.CallWithRetry("ListClusterFaults", {'faultTypes' : 'current'})
        for fault in result["faults"]:
            if fault["code"] == "blockServiceUnhealthy":
                self.log.debug("Bin sync - block related faults are present")
                return True

        return False

    def IsSliceSyncing(self):
        """
        Check if the cluster is slice syncing

        Returns:
            A boolean indicating if the cluster is syncing (True) or not (False)
        """
        if GetHighestAPIVersion(self.mvip, self.username, self.password)>= 5.0:
            # Get the slice assignments report
            result = self.api.HttpDownload("/reports/slices.json")
            slice_report = json.loads(result)

            # Make sure there are no unhealthy services
            if "service" in slice_report:
                for ss in slice_report["services"]:
                    if ss["health"] != "good":
                        self.log.debug("Slice sync - one or more SS are unhealthy")
                        return True

            # Make sure there are no volumes with multiple live secondaries or dead secondaries
            if "slices" in slice_report:
                for vol in slice_report["slices"]:
                    if "liveSecondaries" not in vol:
                        self.log.debug("Slice sync - one or more volumes have no live secondaries")
                        return True
                    if len(vol["liveSecondaries"]) > 1:
                        self.log.debug("Slice sync - one or more volumes have multiple live secondaries")
                        return True
                    if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                        self.log.debug("Slice sync - one or more volumes have dead secondaries")
                        return True
            if "slice" in slice_report:
                for vol in slice_report["slice"]:
                    if "liveSecondaries" not in vol:
                        self.log.debug("Slice sync - one or more volumes have no live secondaries")
                        return True
                    if len(vol["liveSecondaries"]) > 1:
                        self.log.debug("Slice sync - one or more volumes have multiple live secondaries")
                        return True
                    if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                        self.log.debug("Slice sync - one or more volumes have dead secondaries")
                        return True
        else:
            # Get the slice syncing report
            result = self.api.HttpDownload("/reports/slicesyncing")
            if "<table>" in result:
                self.log.debug("Slice sync - entries in slice syncing report")
                return True

        # Make sure there are no slice related faults
        result = self.api.CallWithRetry("ListClusterFaults", {'faultTypes' : 'current'})
        for fault in result["faults"]:
            if fault["code"] == "sliceServiceUnhealthy" or fault["code"] == "volumesDegraded":
                self.log.debug("Slice sync - slice related faults are present")
                return True

        return False

    def GetCurrentFaultSet(self, forceUpdate=False):
        """
        Get a list of the current cluster faults

        Returns:
            A set of cluster fault codes (strings)
        """
        result = self.api.CallWithRetry("ListClusterFaults", {"exceptions": 1, "faultTypes": "current", "update": forceUpdate})
        current_faults = set()
        if (len(result["faults"]) > 0):
            for fault in result["faults"]:
                if fault["code"] not in current_faults:
                    current_faults.add(fault["code"])
        return current_faults

    def CheckForEvent(self, eventString, since=0):
        """
        Check if an event is present in the cluster event log

        Args:
            eventString: the event message to check for
            since: only check for events that were created after this time (integer unix timestamp)

        Returns:
            A boolean indicating if the event was found (True) or not (False)
        """
        event_list = self.api.CallWithRetry('ListEvents', {})

        for i in range(len(event_list['events'])):
            event = event_list['events'][i]
            if eventString in event['message']:
                event_time = util.ParseTimestamp(event['timeOfReport'])
                if event_time > since:
                    return True
        return False

    def GetActiveNodeObjects(self):
        """
        Get the active nodes in the cluster, as SFNodes
        
        Returns:
            A list of node objects (list of SFNode)
        """
        return [SFNode(node_ip,
                       clusterUsername=self.username,
                       clusterPassword=self.password,
                       clusterMvip=self.mvip) for node_ip in self.ListActiveNodeIPs()]

    def ListActiveNodeIPs(self):
        """
        Get a list of the active node IPs

        Returns:
            A list of active node management IPs (strings)
        """
        result = self.api.CallWithRetry("ListActiveNodes", {})
        return sorted([node["mip"] for node in result["nodes"]])

    def ListActiveNodeIDs(self):
        """
        Get a list of the active node IDs

        Returns:
            A list of active node IDs (ints)
        """
        result = self.api.CallWithRetry("ListActiveNodes", {})
        return sorted([node["nodeID"] for node in result["nodes"]])

    def ListActiveNodes(self):
        """
        Get a list of the active nodes

        Returns:
            A list of node dictionaries
        """
        result = self.api.CallWithRetry("ListActiveNodes", {})
        return result["nodes"]

    def ListAllNodes(self):
        """
        Get a list of all the nodes

        Returns:
            A list of node dictionaries
        """
        return self.api.CallWithRetry("ListAllNodes", {})

    def ListVLANs(self):
        """
        Get a list of VLANs on the cluster

        Returns:
            A list of VLAN dictionaries
        """
        result = self.api.CallWithRetry("ListVirtualNetworks", {}, apiVersion=7.0)
        return result["virtualNetworks"]

    def ListVolumeAccessGroups(self):
        """
        Get a list of volume access groups on the cluster

        Returns:
            A list of SFVolGroup objects (list of SFVolGroup)
        """
        result = self.api.CallWithRetry("ListVolumeAccessGroups", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return [SFVolGroup(volgroup, self.mvip, self.username, self.password) for volgroup in result["volumeAccessGroups"]]

    def ListAccounts(self):
        """
        Get a list of accounts on the cluster

        Returns:
            A list of SFAccount objects (list of SFAccount)
        """
        result = self.api.CallWithRetry("ListAccounts", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return [SFAccount(account, self.mvip, self.username, self.password) for account in result["accounts"]]

    def ListActiveVolumes(self):
        """
        Get a list of volumes on the cluster

        Returns:
            A list of volume dictionaries (list of dict)
        """
        result = self.api.CallWithRetry("ListActiveVolumes", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return result["volumes"]

    def ListDeletedVolumes(self):
        """
        Get a list of the deleted volumes on the cluster

        Returns:
            A list of volume dictionaries (list of dict)
        """
        result = self.api.CallWithRetry("ListDeletedVolumes", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return result["volumes"]

    def ListVolumePairs(self):
        """
        Get a list of the active paired volumes on the cluster
        
        Returns:
            A list of volume pair dictionaries (list of dict)
        """
        result = self.api.CallWithRetry("ListActivePairedVolumes", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return result["volumePairs"]

    def ListServices(self):
        """
        Get a list of all services in the cluster
        
        Returns:
            A list of service dictionaries (list of dict)
        """
        result = self.api.CallWithRetry("ListServices", {}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))
        return result["services"]
        

    def FindNode(self, nodeIP=None, nodeID=0, sshUser=sfdefaults.ssh_user, sshPass=sfdefaults.ssh_pass):
        """
        Get the node with the given IP or ID

        Args:
            nodeIP: the management IP address of the node (string)
            nodeID: the nodeID of the node (int)

        Returns:
            An SFNode object
        """
        result = self.api.CallWithRetry("ListActiveNodes", {})
        for node in result["nodes"]:
            if nodeIP and node["mip"] == nodeIP:
                return SFNode(node["mip"], sshUser, sshPass, self.mvip, self.username, self.password)
            if nodeID and node["nodeID"] == nodeID:
                return SFNode(node["mip"], sshUser, sshPass, self.mvip, self.username, self.password)

        if nodeIP:
            raise UnknownObjectError("Could not find node {}".format(nodeIP))
        else:
            raise UnknownObjectError("Could not find node {}".format(nodeID))

    def GetNodeIDs(self, nodeIPs):
        """
        Get the node IDs corresponding to the node IPs

        Args:
            nodeIPs:    the list of node MIPs to find

        Returns:
            a list of integer node IDs
        """
        nodeIDs = []
        result = self.api.CallWithRetry("ListAllNodes", {})
        for mip in nodeIPs:
            for node in result["nodes"] + result["pendingNodes"]:
                if node["mip"] == mip:
                    nodeIDs.append(node["nodeID"])
        return nodeIDs

    def FindAccount(self, accountName=None, accountID=None):
        """
        Find an account with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            accountName: the name of the account to find (string)
            accountID: the ID of the account to find (int)

        Returns:
            An SFAccount object
        """
        return SFAccount.Find(self.mvip, self.username, self.password, accountName, accountID)

    def FindVolumeAccessGroup(self, volgroupName=None, volgroupID=None):
        """
        Find a volume access group with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            volgroupName: the name of the group to find (string)
            volgroupID: the ID of the group to find (int)

        Returns:
            An SFVolGroup object
        """
        return SFVolGroup.Find(self.mvip, self.username, self.password, volgroupName, volgroupID)

    def GetActiveVolumes(self):
        """
        Get a list of all of the active volumes on the cluster

        Returns:
            A dictionary of volumeID (int) => volume info (dict)
        """

        result = self.api.CallWithRetry("ListActiveVolumes", {})
        all_volumes = dict()
        for vol in result["volumes"]:
            all_volumes[vol["volumeID"]] = vol
        return all_volumes

    def SearchForVolumes(self, volumeID=None, volumeName=None, volumeRegex=None, volumePrefix=None, accountName=None, accountID=None, volgroupName=None, volgroupID=0, volumeCount=0):
        """
        Search for volumes with the given criteria

        Args:
            volumeID:           a single volume ID (int) or list of volume IDs (list of ints)
            volumeName:         a single volume name (string) or list of volume names (list of strings)
            volumeRegex:        a regex to match volume names against (string)
            volumePrefix:       a prefix to match volume names starting with (string)
            accountName:        only match volumes that are in the account with this name (string)
            accountID:          only match volumes that are in the account with this ID (int)
            volgroupName:       only match volumes that are in the volume group with this name (string)
            volgroupID:         onyl match volumes that are in the volume group with this ID (int)
            volumeCount:        match at most this many volumes (int)
            message:            qdd this string to the log message indicating what the volumes were selected for (str)

        Returns:
            A dictionary of volumeID (int) => volume info (dict)
        """

        util.AtLeastOneOf(volume_names=volumeName, volume_ids=volumeID, volume_prefix=volumePrefix, volume_regex=volumeRegex, volume_count=volumeCount, source_account=accountName, source_account_id=accountID)

        # Make sure the regex is valid
        if volumeRegex:
            try:
                vol_regex = re.compile(volumeRegex)
            except re.error:
                raise SolidFireError("Invalid regex")

        options = copy.copy(locals())
        options.pop("self", None)
        self.log.debug2("SearchForVolumes {}".format(options))

        # Get list of source volumes to filter
        source_volumes = {vol["volumeID"] : vol for vol in self.api.CallWithRetry("ListActiveVolumes", {})["volumes"]}

        # Narrow down to just an account
        if accountName or accountID:
            source_account = self.FindAccount(accountName=accountName,
                                              accountID=accountID)
            # Only active, undeleted volumes in this account
            source_volumes = {vid : source_volumes[vid] for vid in source_account.volumes if vid in list(source_volumes.keys()) and source_volumes[vid]["status"] == "active"}

        # Narrow down to just a volume group
        if volgroupName or volgroupID:
            source_group = self.FindVolumeAccessGroup(volgroupName=volgroupName,
                                                      volgroupID=volgroupID)
            # Only active, undeleted volumes in this group
            source_volumes = {vid : source_volumes[vid] for vid in source_group.volumes if vid in list(source_volumes.keys()) and source_volumes[vid]["status"] == "active"}

        found_volumes = {}

        if volumeID:
            volume_ids = util.ItemList(int)(volumeID)
            found_volumes = {vol["volumeID"] : vol for vol in source_volumes.values() if vol["volumeID"] in volume_ids}
            if len(list(found_volumes.keys())) != len(volume_ids):
                raise UnknownObjectError("Could not find all specified volume IDs")

        elif volumeName:
            volume_names = util.ItemList(str)(volumeName)
            found_volumes = {vol["volumeID"] : vol for vol in source_volumes.values() if vol["name"] in volume_names}
            if len(list(found_volumes.keys())) != len(volume_names):
                raise UnknownObjectError("Could not find all specified volume names")

        elif volumeRegex:
            found_volumes = {vol["volumeID"] : vol for vol in source_volumes.values() if vol_regex.search(vol["name"])}

        elif volumePrefix:
            found_volumes = {vol["volumeID"] : vol for vol in source_volumes.values() if vol["name"].startswith(volumePrefix)}

        else:
            found_volumes = source_volumes

        if volumeCount:
            for count, vid in enumerate(sorted(found_volumes.keys())):
                if count >= volumeCount:
                    del found_volumes[vid]

        return found_volumes

    def CreateVolumeGroup(self, volgroupName, iqns=None, volumeIDs=None):
        """
        Create a volume access group

        Args:
            volgroupName: the name of the group (string)
            iqns:         the list of initiator IQNs to add to the group (list of strings)
            volumeIDs:    the list of volume IDs to add to the group (list of ints)

        Returns:
            An SFVolGroup object
        """
        return SFVolGroup.Create(mvip=self.mvip,
                                 username=self.username, password=self.password,
                                 volgroupName=volgroupName,
                                 iqns=iqns,
                                 volumeIDs=volumeIDs)

    def CreateAccount(self, accountName, initiatorSecret=None, targetSecret=None):
        """
        Create an account
    
        Args:
            account_name:       the name for the new account
            initiator_secret:   the initiator CHAP secret
            target secret:      the target CHAP secret
    
        Returns:
            An SFAccount object
        """
        return SFAccount.Create(mvip=self.mvip,
                                username=self.username,
                                password=self.password,
                                accountName=accountName,
                                initiatorSecret=initiatorSecret,
                                targetSecret=targetSecret)

    def ListAvailableDrives(self):
        """
        Get a list of all the available drives

        Returns:
            A list of drive dictionaries (list of dict)
        """
        self.log.debug("Searching for available drives...")
        return self.ListDrives(driveState=DriveState.Available)

    def ListDrives(self, driveType=DriveType.Any, driveState=DriveState.Any, nodeID=0, nodeIP=None):
        """
        Get a list of the drives in the cluster

        Args:
            driveType:  only list drives of this type (DriveType) - this may be a scalar or a list
            driveState: only list drives in this state (DriveState) - this may be a scalar or a list
            nodeID:     only list drives from this node (int)
            nodeIP:     only list drives from the node with this MIP (str)

        Returns:
            A list of drive dictionaries (list of dict)
        """
        if nodeIP:
            nodeID = self.GetNodeIDs([nodeIP])[0]

        result = self.api.CallWithRetry("ListDrives", {})
        if driveType == DriveType.Any and driveState == DriveState.Any:
            return result["drives"]

        if not isinstance(driveType, list):
            driveType = [driveType]
        if not isinstance(driveState, list):
            driveState = [driveState]

        drive_list = []
        for drive in result["drives"]:
            include = True
            if DriveType.Any not in driveType and drive["type"] not in driveType:
                include = False
            if DriveState.Any not in driveState and drive["status"] not in driveState:
                include = False
            if nodeID > 0 and drive["nodeID"] != nodeID:
                include = False

            if include:
                drive_list.append(drive)

        return drive_list

    def WaitForAvailableDrives(self, driveCount=0, nodeIP=None, timeout=sfdefaults.available_drives_timeout):
        """
        Wait for drives to be in the available state

        Args:
            driveCount:     wait for this many drives of any type from any node (int)
            nodeIP:         wait for the expected number of drives from this node (string)
            timeout:        how long to wait before giving up (int)

        Returns:
            A list of drive dictionaries that were waited for and are now present (list of dict)
        """
        node_id = 0
        expected_drives = driveCount
        if nodeIP:
            node = self.FindNode(nodeIP=nodeIP)
            node_id = node.GetNodeID()
            expected_drives = node.GetExpectedDriveCount()

        start_time = time.time()
        drives = []
        while True:
            drives = self.ListDrives(driveState=DriveState.Available, nodeID=node_id)
            if len(drives) == expected_drives:
                break

            if time.time() - start_time >= timeout:
                raise TimeoutError("Timed out waiting for available drives [timeout={}s]".format(timeout))

            time.sleep(sfdefaults.TIME_SECOND * 10)

        return drives

    def AddAvailableDrives(self, waitForSync=True):
        """
        Add all of the available drives to the cluster

        Args:
            waitForSync:    whether or not to wait for syncing to complete (bool)

        Returns:
            The number of drives that were added (int)
        """
        available = self.ListAvailableDrives()
        if not available:
            self.log.info("There are no available drives to add")
            return 0

        self.log.info("Adding {} drives to cluster...".format(len(available)))
        self.AddDrives(available, waitForSync)
        return len(available)

    def AddDrives(self, driveList, waitForSync=True):
        """
        Add drives to the cluster

        Args:
            driveList:      the list of drive objects to add (list of dict)
            waitForSync:    whether or not to wait for syncing before returning (bool)
        """
        if not driveList:
            return

        params = {}
        params["drives"] = driveList
        result = self.api.CallWithRetry("AddDrives", params)
        self.WaitForAsyncHandle(result["asyncHandle"])

        if waitForSync:
            # self.log.info("Waiting a little while to make sure syncing has started")
            # time.sleep(sfdefaults.TIME_MINUTE * 2)

            self.log.info("Waiting for slice syncing")
            while self.IsSliceSyncing():
                time.sleep(sfdefaults.TIME_SECOND * 20)
            self.log.info("Slice syncing is complete")

            self.log.info("Waiting for bin syncing")
            while self.IsBinSyncing():
                time.sleep(sfdefaults.TIME_SECOND * 20)
            self.log.info("Bin syncing is complete")

    def RemoveDrives(self, driveList, waitForSync=True):
        """
        Remove drive from the cluster

        Args:
            driveList:      the list of drive IDs or drive objects to remove (list of int or list of dict)
            waitForSync:    whether or not to wait for syncing before returning (bool)
        """
        if not driveList:
            return

        if isinstance(driveList[0], dict):
            driveList = [drive["driveID"] for drive in driveList]

        params = {}
        params["drives"] = driveList
        result = self.api.CallWithRetry("RemoveDrives", params)

        if waitForSync:
            self.WaitForAsyncHandle(result["asyncHandle"])

            # self.log.info("Waiting a little while to make sure syncing has started")
            # time.sleep(sfdefaults.TIME_MINUTE * 2)

            self.log.info("Waiting for slice syncing")
            while self.IsSliceSyncing():
                time.sleep(sfdefaults.TIME_SECOND * 20)
            self.log.info("Slice syncing is complete")

            self.log.info("Waiting for bin syncing")
            while self.IsBinSyncing():
                time.sleep(sfdefaults.TIME_SECOND * 20)
            self.log.info("Bin syncing is complete")

    def RemoveNodes(self, nodeIPList):
        """
        Remove nodes from the cluster

        Args:
            nodeIPList:     the list of active node IPs to remove (list of string)
        """
        node_ids = self.GetNodeIDs(nodeIPList)
        params = {}
        params["nodes"] = node_ids
        self.api.CallWithRetry("RemoveNodes", params)

    def AddNodes(self, nodeIPList, autoRTFI=True):
        """
        Add nodes to the cluster
        
        Args:
            nodeIPList:     the list of pending node IPs to add (list of string)
            autoRTFI:       auto RTFI the nodes when adding to the cluster (bool)
        """
        result = self.ListAllNodes()
        node_ids = []
        for node in result["pendingNodes"]:
            if node["mip"] in nodeIPList:
                node_ids.append(node["pendingNodeID"])
        if len(node_ids) != len(nodeIPList):
            raise UnknownObjectError("Could not find all requested pending nodes")
        
        params = {}
        params["pendingNodes"] = node_ids
        params["autoInstall"] = autoRTFI
        self.api.CallWithRetry("AddNodes", params)

    def StartSliceRebalancing(self):
        """
        Start slice rebalancing on the cluster
        """
        self.api.CallWithRetry("RebalanceSlices", {})

    def ListPendingNodes(self):
        """
        Get a list of the pending nodes in the cluster

        Returns:
            A list of nodes (list of dict)
        """
        result = self.api.CallWithRetry("ListPendingNodes", {})
        return result["pendingNodes"]

    def GetClusterUsedSpace(self):
        """
        Get the usedSpace of the cluster
        
        Returns:
            The used bytes of the cluster (long int)
        """
        result = self.api.CallWithRetry("GetClusterCapacity", {})
        return result["clusterCapacity"]["usedSpace"]

    def StartClusterPairing(self):
        """
        Start a cluster pair

        Returns:
            A StartClusterPairInfo object
        """
        result = self.api.CallWithRetry("StartClusterPairing", {}, apiVersion=6.0)
        return StartClusterPairInfo(jsonResult=result)

    def CompleteClusterPairing(self, pairingKey):
        """
        Complete a cluster pair
        
        Args:
            pairingKey: the pairing key to use (string)
        """
        self.api.CallWithRetry("CompleteClusterPairing", {"key" : pairingKey}, apiVersion=6.0)
        return True

    def RemoveClusterPairing(self, clusterPairID):
        """
        Delete a cluster pair
        
        Args:
            clusterPairID:  the ID of the pair to remove (int)
        """
        self.api.CallWithRetry("RemoveClusterPair", {"clusterPairID" : clusterPairID}, apiVersion=6.0)

    def FindClusterPairID(self, clusterPairUUID=None, remoteClusterName=None):
        """
        Get the ID of a cluster pair with a given UUID or cluster name

        Args:
            clusterPairUUID: the UUID of the cluster pair to find (string)
            remoteClusterName: the name of paired cluster to find (string)

        Returns:
            The cluster pair ID (int)
        """
        pair = self.FindClusterPair(clusterPairUUID, remoteClusterName)
        return pair.ID

    def FindClusterPair(self, clusterPairID=0, clusterPairUUID=None, remoteClusterMVIP=None, remoteClusterName=None):
        """
        Find the cluster pair with the given ID, UUID or cluster name

        Args:
            clusterPairID: the ID of the cluster pair to find (int)
            clusterPairUUID: the UUID of the cluster pair to find (string)
            remoteClusterName: the name of paired cluster to find (string)

        Returns:
            An SFClusterPair object
        """
        return SFClusterPair.Find(self.mvip, self.username, self.password, clusterPairID, clusterPairUUID, remoteClusterMVIP, remoteClusterName)

    def ListClusterPairs(self):
        """
        Get the list of all cluster pairs in this cluster

        Returns:
            A list of SFClusterPair objects
        """
        result = self.api.CallWithRetry("ListClusterPairs", {}, apiVersion=6.0)
        return [SFClusterPair(pair_json, self.mvip, self.username, self.password) for pair_json in result["clusterPairs"]]

    def GetClusterInfo(self):
        """
        Get the basic info about this cluster

        Returns:
            A cluster info dictionary (dict)
        """
        result = self.api.CallWithRetry("GetClusterInfo", {})
        return result["clusterInfo"]

    def GetLimits(self):
        """
        Get the cluster limits for this cluster
        
        Returns:
            A dictionary of limits (dict)
        """
        return self.api.CallWithRetry("GetLimits", {})

    def GetClusterMasterNode(self):
        """
        Get the cluster master node

        Returns:
            The current cluster master node (SFNode object)
        """
        result = self.api.CallWithRetry("GetClusterMasterNodeID", {})
        return self.FindNode(nodeID=result["nodeID"])

    def ModifyVolume(self, volumeID, volumeProperties):
        """
        Modify a volume
        
        Args:
            volumeID:           the ID of the volume to modify
            volumeProperties:   volume properties to change
        
        Returns:
            The new volume dictionary after the update (dict)
        """
        params = copy.deepcopy(volumeProperties)
        params["volumeID"] = volumeID
        result = self.api.CallWithRetry("ModifyVolume", params, apiVersion=5.0)
        return result["volume"]

    def ModifyVolumePair(self, volumeID, pairProperties):
        """
        Modify a volume pair

        Args:
            volumeID:       the ID of the volume to modify
            pairProperties: pair properties to change
        """
        params = copy.deepcopy(pairProperties)
        params["volumeID"] = volumeID
        self.api.CallWithRetry("ModifyVolumePair", params, apiVersion=6.0)

    def CloneVolume(self, volumeID, cloneName, access="readWrite", newSize=0, newAccountID=0):
        """
        Clone a volume

        Args:
            volumeID:       the ID of the volume to clone
            cloneName:      the name of the clone to create
            access:         what access to assign to the clone
            newSize:        the new size for the clone
            newAccountID:  the account ID to clone to

        Returns:
            An async handle for the clone operation (int)
        """
        params = {}
        params["volumeID"] = volumeID
        params["name"] = cloneName
        params["access"] = access
        if newSize:
            params["newSize"] = newSize
        if newAccountID:
            params["newAccountID"] = newAccountID
        result = self.api.CallWithRetry("CloneVolume", params, apiVersion=5.0)
        return result["asyncHandle"]

    def GetAsyncResult(self, asyncHandle):
        """
        Get the result of an async API call
        
        Args:
            asyncHandle:    the handle for this call
        """
        params = {}
        params["asyncHandle"] = asyncHandle
        params["keepResult"] = True
        return self.api.CallWithRetry("GetAsyncResult", params, apiVersion=5.0)

    def CreateVolume(self, volumeName, volumeSize, accountID, enable512e=False, minIOPS=100, maxIOPS=100000, burstIOPS=100000):
        """
        Create a single volume

        Args:
            volumeNames:    the names of the volumes
            volumeSize:     the size of the volumes in bytes
            accountID:      the account for the volumes
            enable512e:     use 512 byte sector emulation on the volumes
            minIOPS:        the min IOPS guarantee for the volumes
            maxIOPS:        the max sustained IOPS for the volumes
            burstIOPS:      the max burst IOPS for the volumes
        """
        params = {}
        params["name"] = volumeName
        params["totalSize"]= volumeSize
        params["accountID"] = accountID
        params["enable512e"] = enable512e
        params["qos"] = {}
        params["qos"]["minIOPS"] = minIOPS
        params["qos"]["maxIOPS"] = maxIOPS
        params["qos"]["burstIOPS"] = burstIOPS

        return self.api.CallWithRetry("CreateVolume", params)

    def CreateVolumes(self, volumeNames, volumeSize, accountID, enable512e=False, minIOPS=100, maxIOPS=100000, burstIOPS=100000):
        """
        Create a list of volumes

        Args:
            volumeNames:    the names of the volumes
            volumeSize:     the size of the volumes in bytes
            accountID:      the account for the volumes
            enable512e:     use 512 byte sector emulation on the volumes
            minIOPS:        the min IOPS guarantee for the volumes
            maxIOPS:        the max sustained IOPS for the volumes
            burstIOPS:      the max burst IOPS for the volumes
        """
        params = {}
        params["names"] = volumeNames
        params["totalSize"]= volumeSize
        params["accountID"] = accountID
        params["enable512e"] = enable512e
        params["qos"] = {}
        params["qos"]["minIOPS"] = minIOPS
        params["qos"]["maxIOPS"] = maxIOPS
        params["qos"]["burstIOPS"] = burstIOPS

        return self.api.CallWithRetry("CreateMultipleVolumes", params, apiVersion=6.0)

    def DeleteVolumes(self, volumeIDs, purge=False):
        """
        Delete a list of volumes
        
        Args:
            volumeIDs:  the list of volumes IDs to delete (list of int)
            purge:      purge the volumes after deleting them (bool)
        """
        version = GetHighestAPIVersion(self.mvip, self.username, self.password)

        if version >= 9.0:
            params = {}
            params["volumeIDs"] = volumeIDs
            self.api.CallWithRetry("DeleteVolumes", params, apiVersion=version)
        else:
            for vol_id in volumeIDs:
                params = {}
                params["volumeID"] = vol_id
                self.api.CallWithRetry("DeleteVolume", params)

        if purge:
            self.PurgeVolumes(volumeIDs)

    def PurgeVolumes(self, volumeIDs):
        """
        Purge a list of volumes
        
        Args:
            volumeIDs:  the list of volumes to purge (list of int)
        """
        version = GetHighestAPIVersion(self.mvip, self.username, self.password)
        if version >= 9.0:
            params = {}
            params["volumeIDs"] = volumeIDs
            self.api.CallWithRetry("PurgeDeletedVolumes", params, apiVersion=version)
        else:
            for vol_id in volumeIDs:
                params = {}
                params["volumeID"] = vol_id
                self.api.CallWithRetry("PurgeDeletedVolume", params)

    def GetVolumeSliceServices(self, volumeID):
        """
        Get the primary and one or more secondary slice services for a volume

        Args:
            volumeID:   the ID of the volume

        Returns:
            A dictionary with primary and secondary service IDs for the volume (dict)
        """
        stats = self.api.CallWithRetry("GetVolumeStats", {"volumeID" : volumeID}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))["volumeStats"]
        return {"primary" : stats["metadataHosts"]["primary"], "secondaries" : stats["metadataHosts"]["liveSecondaries"] + stats["metadataHosts"]["deadSecondaries"]}

    def IsVolumeSyncing(self, volumeID):
        """
        Check if a volume is slice syncing

        Args:
            volumeID:   the ID of the volume to check (int)

        Returns:
            True if it is syncing, False otherwise (bool)
        """
        stats = self.api.CallWithRetry("GetVolumeStats", {"volumeID" : volumeID}, apiVersion=GetHighestAPIVersion(self.mvip, self.username, self.password))["volumeStats"]
        
        # Special case for single node clusters - there are no secondaries
        if not stats["metadataHosts"]["liveSecondaries"] and not stats["metadataHosts"]["deadSecondaries"]:
            return True

        if len(stats["metadataHosts"]["deadSecondaries"]) > 0:
            return True
        if len(stats["metadataHosts"]["liveSecondaries"]) > 1:
            return True

    def ForceWholeFileSync(self, volumeID, waitForSyncing=False, timeout=300):
        """
        Force a full sync from the primary to each of its secondaries
        
        Args:
            volumeID:           the ID of the volume to sync (int)
            waitForSyncing:     wait for slice syncing to complete (bool)
            timeout:            how long to wait for syncing, in seconds (int)
        """
        services = self.GetVolumeSliceServices(volumeID)

        # Special case for single node clusters
        if not services["secondaries"]:
            return

        for service_id in services["secondaries"]:
            params = {}
            params["sliceID"] = volumeID
            params["primary"] = services["primary"]
            params["primary"] = service_id
            self.api.CallWithRetry("ForceWholeFileSync", params, apiVersion=5.0)

        if waitForSyncing:
            self.log.info("Waiting for volume {} to sync".format(volumeID))
            start_time = time.time()
            while True:
                if not self.IsVolumeSyncing(volumeID):
                    break
                time.sleep(sfdefaults.TIME_SECOND)
                if time.time() - start_time > timeout:
                    raise TimeoutError("Timeout waiting for syncing on volume {}".format(volumeID))

    def CreateVLAN(self, tag, addressStart, addressCount, netmask, svip, namespace=False):
        """
        Create a VLAN on the cluster
        
        Args:
            tag:                the tag for the VLAN
            addressStart:       the starting address for the nodes on the VLAN
            addressCount:       the number of addresses for nodes on the VLAN
            netmask:            the netmask for the nodes on the VLAN
            svip:               the SVIP for the VLAN
            namespace:          put this VLAN in a namespace
        """
        params = {}
        params['virtualNetworkTag'] = tag
        params['name'] = "vlan-{}".format(tag)
        params['addressBlocks'] = []
        params['addressBlocks'].append({'start' : addressStart, 'size' : addressCount})
        params['netmask'] = netmask
        params['svip'] = svip
        params['namespace'] = namespace
        
        self.api.CallWithRetry("AddVirtualNetwork", params, apiVersion=8.0)

    def SetSSLCertificate(self, cert, key):
        """
        Set the SSL certificate/key for this cluster
        
        Args:
            cert:   the certificate, in PEM encoded form (str)
            key:    the private key, in PEM encoded form (str)
        """
        params = {}
        params["certificate"] = cert
        params["privateKey"] = key
        self.api.CallWithRetry("SetSSLCertificate", params, apiVersion=10.0)

    def WaitForAsyncHandle(self, asyncHandle):
        """
        Wait for an async handle to complete
        """
        while True:
            result = self.GetAsyncResult(asyncHandle)
            if result["status"] == "complete":
                break
            time.sleep(sfdefaults.TIME_SECOND * 10)
        return result

    def RemoveSSLCertificate(self):
        """
        Remove the user SSL certificate and return the cluster to the default SSL certificate
        """
        self.api.CallWithRetry("RemoveSSLCertificate", {}, apiVersion=10.0)

    def GetSSLCertificate(self):
        """
        Get the active SSL certificate for this cluster

        Returns:
            A dictionary with the SSL certificate info (dict)
        """
        return self.api.CallWithRetry("GetSSLCertificate", {}, apiVersion=10.0)
