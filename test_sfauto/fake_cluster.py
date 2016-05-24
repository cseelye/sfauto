#!/usr/bin/env python2.7
"""
Fake cluster API
"""

#pylint: disable=missing-docstring,protected-access, unused-argument, not-context-manager, attribute-defined-outside-init

import base64
import copy
import datetime
import json
import multiprocessing
import os
import random
import socket
import string
import threading
import time
import urllib2
from urlparse import urlparse
import uuid

from . import globalconfig
from .testutil import RandomIP, RandomString, RandomSequence
from libsf import SolidFireAPIError as SolidFireApiError # compat with sfinstall version
from libsf import SolidFireError
from libsf.logutil import GetLogger
from libsf.util import TimestampToStr, UTCTimezone

def fake_urlopen(request, *args, **kwargs):
    """Fake out urllib2.urlopen and return fake but consistent results as if they came from a SF endpoint"""

    username = None
    password = None
    if "Authorization" in request.headers:
        authType, authHash = request.headers["Authorization"].split()
        if authType == "Basic":
            username, password = base64.b64decode(authHash).split(":")

    # JSON API call
    if "json-rpc" in request.get_selector():
        url = urlparse(request.get_full_url())
        req = json.loads(request.get_data())
        api_version = float(url.path.split("/")[-1])
        method_name = req["method"]
        method_params = req["params"]
        response = globalconfig.cluster.Call(method_name,
                                             method_params,
                                             ip=url.hostname,
                                             endpoint=request.get_full_url(),
                                             apiVersion=api_version,
                                             username=username,
                                             password=password)
        return FakeResponse(json.dumps({"result" : response}))

    # Regular download
    else:
        response = globalconfig.cluster.HttpDownload(request.get_selector())
        return FakeResponse(response)

class FakeResponse(object):
    """Response object that fake_urlopen returns, acts like a urllib2.Response object"""

    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass

def fake_socket(*args, **kwargs):
    """Fake a call to socket.socket()"""
    return FakeSocket(*args, **kwargs)

class FakeSocket():
    """Fake a socket object"""

    def __init__(self, *args, **kwargs):
        self.sock = socket.socket_original(*args, **kwargs)

    def settimeout(self, *args, **kwargs):
        pass

    def connect(self, *args, **kwargs):
        pass

    def close(self):
        self.sock.close()

    def fileno(self):
        return self.sock.fileno()

    def send(self, *args, **kwargs):
        pass

class APIFailure(object):
    """Context manager to cause API calls to fail"""

    ALWAYS_FAIL = -1
    ALL_METHODS = "all"

    def __init__(self, methodName, exceptionThrown=None, retryable=False, failCount=ALWAYS_FAIL, preSuccessCount=0, random=False):
        """
        Args:
            methodName:         name of the method to fail
            exceptionThrown:    the exception to throw when the method is called
            failCount:          how many times to fail the method before letting it succeed
            preSuccessCount:    how many times to let the method succeed before failing
            random:             randomly fail or not, up to failCount times
        """
        self.methodName = methodName
        self.exception = exceptionThrown
        self.failCount = failCount
        self.preSuccessCount = preSuccessCount
        self.random = random
        if not self.exception:
            extype = "xRetryableFakeError" if retryable else "xFakeError"
            exmsg = "Random fake unit test error" if random else "Fake unit test error"
            self.exception = SolidFireApiError(methodName, {}, "0.0.0.0", "https://0.0.0.0:443/json-rpc/0.0", extype, 500, exmsg)

    def __enter__(self):
        globalconfig.cluster.AddAPIFailure(self.methodName, self.exception, self.failCount, self.preSuccessCount, self.random)
        return self

    def __exit__(self, ex_type, ex_value, traceback):
        globalconfig.cluster.RemoveAPIFailure(self.methodName)

class APIVersion(object):

    def __init__(self, highestVersion, removeVersions=None):
        removeVersions = removeVersions or []
        self.versions = [ver for ver in globalconfig.all_api_versions if ver <= highestVersion and ver not in removeVersions]
        self.oldEndpoints = []

    def __enter__(self):
        self.oldEndpoints = globalconfig.cluster.GetAPIEndpoints()
        globalconfig.cluster.SetAPIEndpoints(self.versions)
        return self

    def __exit__(self, ex_type, ex_value, traceback):
        globalconfig.cluster.SetAPIEndpoints(self.oldEndpoints)

class ClusterVersion(object):

    def __init__(self, version):
        self.version = version
        self.oldVersion = None

    def __enter__(self):
        self.oldVersion = globalconfig.cluster.GetClusterVersion()
        globalconfig.cluster.SetClusterVersion(self.version)

    def __exit__(self, ex_type, ex_value, traceback):
        globalconfig.cluster.SetClusterVersion(self.oldVersion)

class SolidFireVersion(object):
    """Easily compare SolidFire version strings"""

    def __init__(self, versionString):
        self.rawVersion = versionString
        pieces = versionString.split(".")
        if len(pieces) == 4:
            self.major = int(pieces[0])
            self.minor = int(pieces[1])
            self.patch = int(pieces[2])
            self.build = int(pieces[3])
        elif len(pieces) == 2:
            self.major = int(pieces[0])
            self.minor = 0
            self.patch = 0
            self.build = int(pieces[1])
        self.apiVersion = float(self.major) + float(self.minor)/10

    @staticmethod
    def FromParts(major, minor, patch=None, build=None):
        return SolidFireVersion("{}.{}.{}.{}".format(major, minor, patch, build))

    def parts(self):
        return self.major, self.minor, self.patch, self.build

    def __str__(self):
        return self.rawVersion

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.major == other.major and \
               self.minor == other.minor and \
               self.patch == other.patch and \
               self.build == other.build

    def __gt__(self, other):
        return self.major > other.major or \
               (self.major == other.major and self.minor > other.minor) or \
               (self.major == other.major and self.minor == other.minor and self.patch > other.patch) or \
               (self.major == other.major and self.minor == other.minor and self.patch == other.patch and self.build > other.build)

    def __ne__(self, other):
        return not self == other

    def __ge__(self, other):
        return self == other or self > other

    def __lt__(self, other):
        return self != other and not self > other

    def __le__(self, other):
        return self == other or self < other


ACCOUNT_PATH = "accounts"
INITIATORS_PATH = "initiators"
VOLGROUP_PATH = "volgroups"
VOLGROUP_LUNS_PATH = "volgroup_luns"
VOLUME_PATH = "volumes"
DELETED_VOLUMES_PATH = "deletedvolumes"
API_FAILURES_PATH = "apifailures"
DRIVES_PATH = "drives"
ACTIVE_NODES_PATH = "activenodes"
PENDING_NODES_PATH = "pendingnodes"
NEXTID_PATH = "nextid"
ADMINS_PATH = "admins"
ENSEMBLE_NODES_PATH = "ensemblenodes"
CLUSTER_MASTER_PATH = "clustermaster"
NODE_VERSION_PATH = "nodeversions"
VERSION_INFO_PATH = "versioninfo"
CONSTANTS_PATH = "constants"
STARTUP_FLAGS_PATH = "startupflags"
REPOS_PATH = "repositories"
UPGRADE_ELEMENT_PATH = "upgradeelement"
CLUSTER_ID_PATH ="clusterid"
CLUSTER_VERSION_PATH = "clusterversion"
DRIVES_PER_NODE_PATH = "drivespernode"
SLICE_REPORT_HEALTHY_PATH = "slicereporthealthy"
SLICE_REPORT_UNHEALTHY_PATH = "slicereportunhealthy"
BIN_REPORT_HEALTHY_PATH = "binreporthealthy"
BIN_REPORT_UNHEALTHY_PATH = "bnreportunhealthy"
INSTALLED_PACKAGES_PATH = "installedpackages"
AVAILABLE_PACKAGES_PATH = "availablepackages"
CLUSTER_NAME_PATH = "clustername"
SVIP_PATH = "clustersvip"
API_ENDPOINTS_PATH = "apiendpoints"
ASYNC_HANDLES_PATH ="asynchandles"

ISCSI_NODE_TYPES = {
    "SF3010" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2640 0 @ 2.50GHz",
        "nodeMemoryGB": 72,
        "nodeType": "SF3010"
    },
    "SF6010" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2640 0 @ 2.50GHz",
        "nodeMemoryGB": 144,
        "nodeType": "SF6010"
    },
    "SF9010" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2670 0 @ 2.60GHz",
        "nodeMemoryGB": 256,
        "nodeType": "SF9010"
    },
    "SF2405" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2620 v2 @ 2.10GHz",
        "nodeMemoryGB": 64,
        "nodeType": "SF2405"
    },
    "SF4805" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2620 v2 @ 2.10GHz",
        "nodeMemoryGB": 128,
        "nodeType": "SF4805"
    },
    "SF9605" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2620 v2 @ 2.10GHz",
        "nodeMemoryGB": 256,
        "nodeType": "SF9605"
    },
    "SF9608" : {
        "chassisType": "C220M4",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2640 v3 @ 2.60GHz",
        "nodeMemoryGB": 256,
        "nodeType": "SF9608"
    },
    "SF19210" : {
        "chassisType": "R630",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz",
        "nodeMemoryGB": 384,
        "nodeType": "SF19210"
    },
}

FC_NODE_TYPES = {
    "FC0025" : {
        "chassisType": "R620",
        "cpuModel": "Intel(R) Xeon(R) CPU E5-2640 0 @ 2.50GHz",
        "nodeMemoryGB": 32,
        "nodeType": "FC0025"
    },
}

HEALTHY_FAULTS = [
    {
        "clusterFaultID": 1,
        "code": "nodeHardwareFault",
        "data": None,
        "date": "2016-02-23T20:31:43.330101Z",
        "details": "NVRAM device warning={excessiveCurrent (2x)}",
        "driveID": 0,
        "nodeHardwareFaultID": 1,
        "nodeID": 2,
        "resolved": False,
        "resolvedDate": "",
        "serviceID": 0,
        "severity": "warning",
        "type": "node"
    },
    {
        "clusterFaultID": 39,
        "code": "nodeHardwareFault",
        "data": None,
        "date": "2016-02-23T08:59:21.923308Z",
        "details": "Power supply PS1 input missing or out of range.",
        "driveID": 0,
        "nodeHardwareFaultID": 9,
        "nodeID": 18,
        "resolved": False,
        "resolvedDate": "2016-02-23T09:33:00.656876Z",
        "serviceID": 0,
        "severity": "warning",
        "type": "node"
    },
    {
        "clusterFaultID": 1,
        "code": "nodeHardwareFault",
        "data": None,
        "date": "2016-02-23T20:31:43.330101Z",
        "details": "NVRAM device warning={timeToRunRestore: 28.76 Seconds}",
        "driveID": 0,
        "nodeHardwareFaultID": 1,
        "nodeID": 2,
        "resolved": False,
        "resolvedDate": "",
        "serviceID": 0,
        "severity": "warning",
        "type": "node"
      },
    {
        "clusterFaultID": 33,
        "code": "driveFailed",
        "data": None,
        "date": "2016-02-23T21:16:35.295801Z",
        "details": "Node ID 1 has 1 failed drive(s).",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 1,
        "resolved": False,
        "resolvedDate": "2016-02-23T21:18:36.972364Z",
        "serviceID": 0,
        "severity": "warning",
        "type": "drive"
    },
    {
        "clusterFaultID": 4,
        "code": "driveAvailable",
        "data": None,
        "date": "2016-02-23T22:17:29.417182Z",
        "details": "Node ID 1 has 1 available drive(s).",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 1,
        "resolved": False,
        "resolvedDate": "2016-02-23T22:27:37.047430Z",
        "serviceID": 0,
        "severity": "warning",
        "type": "drive"
    },
    {
        "clusterFaultID": 17,
        "code": "nodeHardwareFault",
        "data": None,
        "date": "2016-02-23T21:01:19.850205Z",
        "details": "Network interface eth3 is down or cable is unplugged.",
        "driveID": 0,
        "nodeHardwareFaultID": 27,
        "nodeID": 4,
        "resolved": False,
        "resolvedDate": "",
        "serviceID": 0,
        "severity": "warning",
        "type": "node"
    },
]
UNHEALTHY_FAULTS = [
    {
        "clusterFaultID": 2,
        "code": "metadataClusterFull",
        "data": {
          "failingNodeIDCombination": [
            1,
            2
          ],
          "maxNodeFailures": 1,
          "reason": "nodeFailures",
          "recentHistory": [
            {
              "date": "2016-02-23T20:44:06.772336Z",
              "severity": "Warning"
            }
          ]
        },
        "date": "2016-02-23T20:44:06.772336Z",
        "details": "Add additional capacity or free up capacity as soon as possible.",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 0,
        "resolved": False,
        "resolvedDate": "",
        "serviceID": 0,
        "severity": "warning",
        "type": "cluster"
    },
    {
        "clusterFaultID": 32,
        "code": "blockServiceUnhealthy",
        "data": None,
        "date": "2016-02-23T21:16:19.276705Z",
        "details": "The SolidFire Application cannot communicate with a Block Service. If this condition persists Double Helix will relocate a second copy of the data to another drive. This message will clear when communication is reestablished or the data has been relocated.",
        "driveID": 4,
        "nodeHardwareFaultID": 0,
        "nodeID": 1,
        "resolved": False,
        "resolvedDate": "2016-02-23T21:19:32.759459Z",
        "serviceID": 67,
        "severity": "warning",
        "type": "service"
    },
    {
        "clusterFaultID": 1,
        "code": "sliceServiceUnhealthy",
        "data": None,
        "date": "2016-02-23T21:32:03.131782Z",
        "details": "SolidFire Application cannot communicate with a metadata service.",
        "driveID": 34,
        "nodeHardwareFaultID": 0,
        "nodeID": 3,
        "resolved": False,
        "resolvedDate": "2016-02-23T21:38:19.056011Z",
        "serviceID": 30,
        "severity": "warning",
        "type": "service"
    },
    {
        "clusterFaultID": 4,
        "code": "blockServiceTooFull",
        "data": None,
        "date": "2016-02-19T22:42:41.956947Z",
        "details": "A Block Service is using 95% of the available space and space is getting critically low. Reads and writes can become disabled if this condition persists. You should immediately delete and purge volumes and snapshots or add more nodes.",
        "driveID": 4,
        "nodeHardwareFaultID": 0,
        "nodeID": 1,
        "resolved": False,
        "resolvedDate": "2016-02-19T22:44:02.251048Z",
        "serviceID": 49,
        "severity": "critical",
        "type": "service"
    },
    {
        "clusterFaultID": 11,
        "code": "volumesDegraded",
        "data": {
          "degradedVolumes": [
            1,
            3,
            4,
            5
        ]},
        "date": "2016-02-22T15:05:13.306555Z",
        "details": "Local replication and synchronization of the following volumes is in progress. Progress of the synchronization can be seen in the Running Tasks window. [1, 3, 4, 5]",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 0,
        "resolved": False,
        "resolvedDate": "2016-02-22T15:07:37.685179Z",
        "serviceID": 0,
        "severity": "error",
        "type": "cluster"
    },
    {
        "clusterFaultID": 12,
        "code": "nodeOffline",
        "data": None,
        "date": "2016-02-22T15:06:41.436437Z",
        "details": "The SolidFire Application cannot communicate with node ID 4.",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 4,
        "resolved": False,
        "resolvedDate": "2016-02-22T15:07:37.687049Z",
        "serviceID": 0,
        "severity": "error",
        "type": "node"
    },
    {
        "clusterFaultID": 3,
        "code": "blockClusterFull",
        "data": {
          "maxNodeFailures": 0,
          "nodeCapacities": [
            {
              "driveRawCapacityBytes": 300069052416,
              "nodeID": 1,
              "totalBytes": 3000690524160,
              "usedBytes": 2244972140407
            },
            {
              "driveRawCapacityBytes": 300069052416,
              "nodeID": 2,
              "totalBytes": 3000690524160,
              "usedBytes": 2211231244780
            },
            {
              "driveRawCapacityBytes": 300069052416,
              "nodeID": 4,
              "totalBytes": 3000690524160,
              "usedBytes": 2198362967399
            },
            {
              "driveRawCapacityBytes": 300069052416,
              "nodeID": 3,
              "totalBytes": 3000690524160,
              "usedBytes": 2195047926252
            }
          ],
          "recentHistory": [
            {
              "date": "2016-02-19T22:32:21.332276Z",
              "severity": "Error"
            },
            {
              "date": "2016-02-19T22:30:16.387271Z",
              "severity": "Warning"
            }
          ]
        },
        "date": "2016-02-19T22:30:16.387271Z",
        "details": "Due to high capacity consumption Helix data protection will not recover if a node fails. Creating new Volumes or Snapshots is not permitted until additional capacity is available. Add additional capacity or free up capacity immediately.",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 0,
        "resolved": False,
        "resolvedDate": "",
        "serviceID": 0,
        "severity": "error",
        "type": "cluster"
    },
    {
        "clusterFaultID": 4,
        "code": "ensembleDegraded",
        "data": None,
        "date": "2016-02-23T23:52:55.752797Z",
        "details": "Ensemble degraded: 2/5 database servers not connectable: {4:10.10.64.61,5:10.10.64.66}",
        "driveID": 0,
        "nodeHardwareFaultID": 0,
        "nodeID": 0,
        "resolved": False,
        "resolvedDate": "2016-02-23T23:55:25.180825Z",
        "serviceID": 0,
        "severity": "error",
        "type": "cluster"
    },
]

class FakeCluster(object):
    """Fake object that acts like a SF cluster"""

    def __init__(self):
        self.data = {}
        self.data[NEXTID_PATH] = 10000
        self.dataLock = threading.RLock()

    def LoadConfig(self, config):
        """Load a cluster configuration"""

        with self.dataLock:
            for path in [ADMINS_PATH,
                         DRIVES_PATH,
                         ACTIVE_NODES_PATH,
                         PENDING_NODES_PATH,
                         ACCOUNT_PATH,
                         VOLUME_PATH,
                         DELETED_VOLUMES_PATH,
                         VOLGROUP_PATH,
                         VOLGROUP_LUNS_PATH,
                         INITIATORS_PATH,
                         CLUSTER_MASTER_PATH,
                         ENSEMBLE_NODES_PATH,
                         NODE_VERSION_PATH,
                         VERSION_INFO_PATH,
                         CONSTANTS_PATH,
                         STARTUP_FLAGS_PATH,
                         REPOS_PATH,
                         UPGRADE_ELEMENT_PATH,
                         CLUSTER_ID_PATH,
                         CLUSTER_VERSION_PATH,
                         DRIVES_PER_NODE_PATH,
                         INSTALLED_PACKAGES_PATH,
                         AVAILABLE_PACKAGES_PATH,
                         CLUSTER_NAME_PATH,
                         SVIP_PATH,
                         API_ENDPOINTS_PATH,
                         API_FAILURES_PATH,
                         ASYNC_HANDLES_PATH]:
                if path in config:
                    self.data[path] = config[path]
                else:
                    self.data[path] = {}

            active_node_count = len(self.data[ACTIVE_NODES_PATH].keys())

            # Create slice and bin reports
            self.data[SLICE_REPORT_HEALTHY_PATH] = {
                "services": [],
                "slices" : []
            }
            self.data[SLICE_REPORT_UNHEALTHY_PATH] = {
                "services": [],
                "slices" : []
            }
            for idx in xrange(1, active_node_count+1):
                self.data[SLICE_REPORT_HEALTHY_PATH]["services"].append({
                    "health" : "good",
                    "ip" : str(idx + 100),
                    "nodeID" : idx + 50,
                    "serviceID" : idx + 10
                })
                self.data[SLICE_REPORT_UNHEALTHY_PATH]["services"].append({
                    "health" : "dead",
                    "ip" : str(idx + 100),
                    "nodeID" : idx + 50,
                    "serviceID" : idx + 10
                })

            for idx in xrange(1, 101):
                self.data[SLICE_REPORT_HEALTHY_PATH]["slices"].append({
                    "liveSecondaries" : [ random.randint(10, active_node_count + 10)]
                })
                self.data[SLICE_REPORT_UNHEALTHY_PATH]["slices"].append({
                    "deadSecondaries" : [ random.randint(10, active_node_count + 10)]
                })

            self.data[BIN_REPORT_HEALTHY_PATH] = []
            self.data[BIN_REPORT_UNHEALTHY_PATH] = []
            for idx in xrange(0, 10): # Only create the first few bins, it takes too long to make all of them and we don't need them
                self.data[BIN_REPORT_HEALTHY_PATH].append({
                    "binID" : idx,
                    "services" : [
                        { "serviceID" : random.randint(20, active_node_count * 10 + 20), "status" : "bsActive"},
                        { "serviceID" : random.randint(20, active_node_count * 10 + 20), "status" : "bsActive"}
                    ]
                })
                self.data[BIN_REPORT_UNHEALTHY_PATH].append({
                    "binID" : idx,
                    "services" : [
                        { "serviceID" : random.randint(20, active_node_count * 10 + 20), "status" : "bsActive"},
                        { "serviceID" : random.randint(20, active_node_count * 10 + 20), "status" : "bsUpdating"},
                        { "serviceID" : random.randint(20, active_node_count * 10 + 20), "status" : "bsPendingRemovalActive"}
                    ]
                })

    def GenerateRandomConfig(self, seed, includeFCNodes=False):
        """Create a random cluster configuration"""
        config = {}

        config["seed"] = seed
        config[CLUSTER_ID_PATH] = RandomString(4)
        config[CLUSTER_NAME_PATH] = RandomString(8)
        config[SVIP_PATH] = RandomIP()
        config[CLUSTER_VERSION_PATH] = "9.0.0.{}".format(random.randint(1000, 2000))
        config[DRIVES_PER_NODE_PATH] = random.randint(8, 11)
        config[API_ENDPOINTS_PATH] = globalconfig.all_api_versions

        active_node_count = random.randint(4, 20)
        pending_node_count = random.randint(1, 3)

        config[ADMINS_PATH] = {}
        admin_ids = RandomSequence(random.randint(3, 10))
        for admin_id in admin_ids:
            config[ADMINS_PATH][admin_id] = {
                "access" : ["read"] + random.sample(["drives", "volumes", "accounts", "nodes"], random.randint(1, 2)),
                "attributes" : {},
                "authMethod" : "Cluster",
                "clusterAdminID" : admin_id,
                "username" : RandomString(random.randint(5, 16))
            }
        # Make a cluster admin 1
        config[ADMINS_PATH][1] = {
            "access" : ["administrator"],
            "attributes" : {},
            "authMethod" : "Cluster",
            "clusterAdminID" : 1,
            "username" : RandomString(random.randint(5, 16))
        }

        drive_ids = RandomSequence(active_node_count * config[DRIVES_PER_NODE_PATH])
        node_ids = RandomSequence(active_node_count)
        pending_node_ids = RandomSequence(pending_node_count)

        # Create active and available drives
        config[DRIVES_PATH] = {}
        for idx in xrange(len(drive_ids)):
            drive_id = drive_ids[idx]
            config[DRIVES_PATH][drive_id] = {
                "attributes": { },
                "capacity": 300069052416,
                "driveID": drive_id,
                "nodeID": node_ids[idx / config[DRIVES_PER_NODE_PATH]],
                "serial": "scsi-SATA_INTEL_SSD{}".format(random.randint(10000000,99999999)),
                "slot": -1 if idx % config[DRIVES_PER_NODE_PATH] == 0 else idx % config[DRIVES_PER_NODE_PATH] - 1,
                "status": "active" if idx < config[DRIVES_PER_NODE_PATH] * (active_node_count-1) else "available",
                "type": "volume" if idx % config[DRIVES_PER_NODE_PATH] == 0 else "block"
            }

        # Create active and pending nodes
        config[ACTIVE_NODES_PATH] = {}
        for node_id in node_ids:
            config[ACTIVE_NODES_PATH][node_id] = {
                    "associatedFServiceID": 0,
                    "associatedMasterServiceID": node_id+1,
                    "attributes": { },
                    "cip": RandomIP(),
                    "cipi": "Bond10G",
                    "fibreChannelTargetPortGroup": None,
                    "mip": RandomIP(),
                    "mipi": "Bond1G",
                    "name": RandomString(8),
                    "nodeID": node_id,
                    "platformInfo": random.choice(ISCSI_NODE_TYPES.values()),
                    "sip": RandomIP(),
                    "sipi": "Bond10G",
                    "softwareVersion": config[CLUSTER_VERSION_PATH],
                    "uuid": str(uuid.uuid4()),
                    "virtualNetworks": [ ]
                }

        if includeFCNodes:
            fc_node_ids = RandomSequence(2, distinctFrom=node_ids)
            for idx, node_id in enumerate(fc_node_ids):
                config[ACTIVE_NODES_PATH][node_id] = {
                        "associatedFServiceID": node_id+2,
                        "associatedMasterServiceID": node_id+1,
                        "attributes": { },
                        "cip": RandomIP(),
                        "cipi": "Bond10G",
                        "fibreChannelTargetPortGroup": idx,
                        "mip": RandomIP(),
                        "mipi": "Bond1G",
                        "name": RandomString(8),
                        "nodeID": node_id,
                        "platformInfo": random.choice(FC_NODE_TYPES.values()),
                        "sip": RandomIP(),
                        "sipi": "Bond10G",
                        "softwareVersion": config[CLUSTER_VERSION_PATH],
                        "uuid": str(uuid.uuid4()),
                        "virtualNetworks": [ ]
                    }
            node_ids.extend(fc_node_ids)

        config[PENDING_NODES_PATH] = {}
        for node_id in pending_node_ids:
            config[PENDING_NODES_PATH][node_id] = {
                "assignedNodeID": 0,
                "cip": RandomIP(),
                "cipi": "Bond10G",
                "compatible": True,
                "mip": RandomIP(),
                "mipi": "Bond1G",
                "name": RandomString(8),
                "pendingNodeID": node_id,
                "platformInfo": random.choice(ISCSI_NODE_TYPES.values()),
                "sip": RandomIP(),
                "sipi": "Bond10G",
                "softwareVersion": config[CLUSTER_VERSION_PATH],
                "uuid": str(uuid.uuid4())
            }

        # Select nodes for ensemble
        if len(node_ids) < 5:
            config[ENSEMBLE_NODES_PATH] = random.sample(node_ids, 3)
        else:
            config[ENSEMBLE_NODES_PATH] = random.sample(node_ids, 5)
        
        # Pick a cluster master
        config[CLUSTER_MASTER_PATH] = random.choice(node_ids)

        # Version info
        config[NODE_VERSION_PATH] = {}
        for node_id in node_ids + pending_node_ids:
            config[NODE_VERSION_PATH][node_id] = config[CLUSTER_VERSION_PATH]
        config[VERSION_INFO_PATH] = {
            "currentVersion" : config[CLUSTER_VERSION_PATH],
            "nodeID" : 0,
            "packageName" : "",
            "pendingVersion" : config[CLUSTER_VERSION_PATH],
            "startTime" : ""
        }

        # Software packages
        config[INSTALLED_PACKAGES_PATH] = {}
        for node_id in node_ids:
            config[INSTALLED_PACKAGES_PATH][node_id] = []
            config[INSTALLED_PACKAGES_PATH][node_id].append("solidfire-san-unobtanium-{}".format(config[CLUSTER_VERSION_PATH]))
        # Create some packages before and after this version
        cluster_ver = SolidFireVersion(config[CLUSTER_VERSION_PATH])
        config[AVAILABLE_PACKAGES_PATH] = []
        config[AVAILABLE_PACKAGES_PATH].extend(["solidfire-san-unobtanium-{}.{}.{}.{}".format(cluster_ver.major - idx, random.randint(0, 5), random.randint(0,1), random.randint(1000, 2000)) for idx in xrange(1, 4)])
        config[AVAILABLE_PACKAGES_PATH].extend(["solidfire-san-unobtanium-{}.{}.{}.{}".format(cluster_ver.major, cluster_ver.minor + idx, random.randint(0,1), random.randint(1000, 2000)) for idx in xrange(1, 4)])
        config[AVAILABLE_PACKAGES_PATH].extend(["solidfire-san-unobtanium-{}.{}.{}.{}".format(cluster_ver.major + idx, random.randint(0, 5), random.randint(0,1), random.randint(1000, 2000)) for idx in xrange(1, 4)])

        # Startup flags for nodes
        config[STARTUP_FLAGS_PATH] = {}
        with open(os.path.join(os.path.dirname(__file__), "startupflags.json"), "r") as f:
            flags = json.load(f)
        for node_id in node_ids:
            config[STARTUP_FLAGS_PATH][node_id] = flags

        # Create accounts
        account_ids = RandomSequence(random.randint(5, 15))
        config[ACCOUNT_PATH] = {}
        for account_id in account_ids:
            config[ACCOUNT_PATH][account_id] = {
                "accountID": account_id,
                "attributes": {},
                "initiatorSecret": RandomString(12),
                "status": "active",
                "targetSecret": RandomString(12),
                "username": RandomString(random.randint(1, 64)),
                "volumes": []
            }

        # Create active volumes
        volume_ids = RandomSequence(len(account_ids) * 10)
        config[VOLUME_PATH] = {}
        for volume_id in volume_ids:
            account_id = random.choice(account_ids)
            config[VOLUME_PATH][volume_id] = self._NewVolumeJSON(clusterID=config[CLUSTER_ID_PATH],
                                                                 accountID=account_id,
                                                                 volumeName=RandomString(random.randint(6, 64)),
                                                                 volumeID=volume_id,
                                                                 volumeSize=random.randint(int(0.25*1024*1024), 2*1024*int(0.25*1024*1024)) * 4096,
                                                                 enable512e=random.choice([True, False]),
                                                                 access=random.choice(["readWrite", "readOnly", "locked", "replicationTarget"]),
                                                                 status="active")
            config[ACCOUNT_PATH][account_id]["volumes"].append(volume_id)

        # Create volume pairs for some of the volumes
        for pair_id, volume_id in enumerate(random.sample(volume_ids, random.randint(5, min(15, len(volume_ids)))), start=1):
            config[VOLUME_PATH][volume_id]["access"] = "readWrite"
            config[VOLUME_PATH][volume_id]["volumePairs"] = [
                {
                    "clusterPairID": pair_id,
                    "remoteReplication": {
                        "mode": "Async",
                        "pauseLimit": 3145728000,
                        "remoteServiceID": random.randint(10, 100),
                        "resumeDetails": "",
                        "snapshotReplication": {
                            "state": "Idle",
                            "stateDetails": ""
                        },
                        "state": "Active",
                        "stateDetails": ""
                    },
                    "remoteSliceID": random.randint(300, 400),
                    "remoteVolumeID": random.randint(300, 400),
                    "remoteVolumeName": RandomString(random.randint(6, 64)),
                    "volumePairUUID": str(uuid.uuid4())
                }]

        # Create deleted volumes
        volume_ids = RandomSequence(len(account_ids) * 2)
        config[DELETED_VOLUMES_PATH] = {}
        for volume_id in volume_ids:
            account_id = random.choice(account_ids)
            config[DELETED_VOLUMES_PATH][volume_id] = self._NewVolumeJSON(clusterID=config[CLUSTER_ID_PATH],
                                                                          accountID=account_id,
                                                                          volumeName=RandomString(random.randint(6, 64)),
                                                                          volumeID=volume_id,
                                                                          volumeSize=random.randint(int(0.25*1024*1024), 2*1024*int(0.25*1024*1024)) * 4096,
                                                                          enable512e=random.choice([True, False]),
                                                                          access=random.choice(["readWrite", "readOnly", "locked", "replicationTarget"]),
                                                                          status="deleted")
            config[ACCOUNT_PATH][account_id]["volumes"].append(volume_id)

        # Create volume access groups
        volgroup_ids = RandomSequence(random.randint(3, 10))
        config[VOLGROUP_PATH] = {}
        for volgroup_id in volgroup_ids:
            config[VOLGROUP_PATH][volgroup_id] = {
                "name" : RandomString(random.randint(6, 64)),
                "initiators" : [],
                "initiatorIDs" : [],
                "volumes" : random.sample(config[VOLUME_PATH].keys(), random.randint(1, 5)),
                "deletedVolumes" : [],
                "attributes" : {},
                "volumeAccessGroupID" : volgroup_id
            }
        config[VOLGROUP_LUNS_PATH] = {}

        # Create initiators and put into groups
        init_ids = RandomSequence(random.randint(8, 15))
        config[INITIATORS_PATH] = {}
        for init_id in init_ids:
            config[INITIATORS_PATH][init_id] = RandomString(random.randint(6, 64))
            volgroup_id = random.choice(volgroup_ids)
            config[VOLGROUP_PATH][volgroup_id]["initiators"].append(config[INITIATORS_PATH][init_id])
            config[VOLGROUP_PATH][volgroup_id]["initiatorIDs"].append(init_id)

        # Make sure we have some empty volgroups
        volgroup_ids = RandomSequence(3, volgroup_ids)
        for volgroup_id in volgroup_ids:
            config[VOLGROUP_PATH][volgroup_id] = {
                "name" : RandomString(random.randint(6, 64)),
                "initiators" : [],
                "initiatorIDs" : [],
                "volumes" : [],
                "deletedVolumes" : [],
                "attributes" : {},
                "volumeAccessGroupID" : volgroup_id
            }

        # Make sure we have a bunch of small volumes
        volume_ids = []
        for _ in xrange(random.randint(20, 40)):
            while True:
                volume_id = random.randint(1, 40000)
                if volume_id not in config[VOLUME_PATH].keys() and volume_id not in config[DELETED_VOLUMES_PATH].keys():
                    volume_ids.append(volume_id)
                    break
        for volume_id in volume_ids:
            account_id = random.choice(config[ACCOUNT_PATH].keys())
            config[VOLUME_PATH][volume_id] = self._NewVolumeJSON(clusterID=config[CLUSTER_ID_PATH],
                                                                 accountID=account_id,
                                                                 volumeName=RandomString(random.randint(6, 64)),
                                                                 volumeID=volume_id,
                                                                 volumeSize=random.randint(1, 100) * 4096,
                                                                 enable512e=random.choice([True, False]),
                                                                 access=random.choice(["readWrite", "readOnly", "locked", "replicationTarget"]),
                                                                 status="active")
            config[ACCOUNT_PATH][account_id]["volumes"].append(volume_id)

        # Make sure we have one volume group where all the volumes have lower IDs
        volume_ids = []
        for _ in xrange(random.randint(3, 15)):
            while True:
                volume_id = random.randint(1, 8000)
                if volume_id not in config[VOLUME_PATH].keys() and volume_id not in config[DELETED_VOLUMES_PATH].keys():
                    volume_ids.append(volume_id)
                    break
        for volume_id in volume_ids:
            account_id = random.choice(config[ACCOUNT_PATH].keys())
            config[VOLUME_PATH][volume_id] = self._NewVolumeJSON(clusterID=config[CLUSTER_ID_PATH],
                                                                 accountID=account_id,
                                                                 volumeName=RandomString(random.randint(6, 64)),
                                                                 volumeID=volume_id,
                                                                 volumeSize=random.randint(1, 20) * 4096 * 244140,
                                                                 enable512e=random.choice([True, False]),
                                                                 access=random.choice(["readWrite", "readOnly", "locked", "replicationTarget"]),
                                                                 status="active")
            config[ACCOUNT_PATH][account_id]["volumes"].append(volume_id)
        volgroup_ids = RandomSequence(1, config[VOLGROUP_PATH].keys())
        for volgroup_id in volgroup_ids:
            config[VOLGROUP_PATH][volgroup_id] = {
                "name" : RandomString(random.randint(6, 64)),
                "initiators" : [],
                "initiatorIDs" : [],
                "volumes" : volume_ids,
                "deletedVolumes" : [],
                "attributes" : {},
                "volumeAccessGroupID" : volgroup_id
            }


        # Make sure we have some empty accounts
        account_ids = RandomSequence(3, config[ACCOUNT_PATH].keys())
        for account_id in account_ids:
            config[ACCOUNT_PATH][account_id] = {
                "accountID": account_id,
                "attributes": {},
                "initiatorSecret": RandomString(12),
                "status": "active",
                "targetSecret": RandomString(12),
                "username": RandomString(random.randint(1, 64)),
                "volumes": []
            }

        # Cluster constants
        with open(os.path.join(os.path.dirname(__file__), "constants.json"), "r") as f:
            config[CONSTANTS_PATH] = json.load(f)

        # Software repos
        config[REPOS_PATH] = []
        config[REPOS_PATH].append({"description" : "Default SolidFire Repository",
                                   "host" : "archive",
                                   "port" : 0})
        config[UPGRADE_ELEMENT_PATH] = ""

        self.LoadConfig(config)

    def DumpClusterConfig(self, fileName):
        with self.dataLock:
            with open(fileName, "w") as outfile:
                json.dump(self.data, outfile)

    def SetClusterInstallPartiallyInstalled(self):
        """Make the state of the cluster look like we are in the middle of an install"""
        version = RandomVersion()
        package_name = "solidfire-san-unobtanium-" + version
        with self.dataLock:
            self.data[VERSION_INFO_PATH]["packageName"] = package_name
            self.data[VERSION_INFO_PATH]["pendingVersion"] = version
            self.data[VERSION_INFO_PATH]["startTime"] = TimestampToStr(time.time() - 60, formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())

            # Stage all of the nodes
            for node_id in self.data[ACTIVE_NODES_PATH].keys():
                self.data[INSTALLED_PACKAGES_PATH][node_id].append(package_name)

            # Install some of the nodes
            installed_nodes = random.sample(self.data[ACTIVE_NODES_PATH].keys(), random.randint(1, len(self.data[ACTIVE_NODES_PATH].keys())/2))
            for node_id in installed_nodes:
                self.data[NODE_VERSION_PATH][node_id] = version

            # Set the install node ID to one of the nodes that has not already been installed
            self.data[VERSION_INFO_PATH]["nodeID"] =  [node_id for node_id in self.data[ACTIVE_NODES_PATH].keys() if node_id not in installed_nodes][0]

    def SetClusterInstallPartiallyStaged(self):
        version = RandomVersion()
        package_name = "solidfire-san-unobtanium-" + version
        with self.dataLock:
            self.data[VERSION_INFO_PATH]["nodeID"] = 0
            self.data[VERSION_INFO_PATH]["packageName"] = package_name
            self.data[VERSION_INFO_PATH]["pendingVersion"] = version
            self.data[VERSION_INFO_PATH]["startTime"] = TimestampToStr(time.time() - 60, formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())

            # Stage some of the nodes
            for node_id in random.sample(self.data[ACTIVE_NODES_PATH].keys(), random.randint(1, len(self.data[ACTIVE_NODES_PATH].keys())/2)):
                self.data[INSTALLED_PACKAGES_PATH][node_id].append(package_name)

    def SetClusterInstallFullyStaged(self):
        version = RandomVersion()
        package_name = "solidfire-san-unobtanium-" + version
        with self.dataLock:
            self.data[VERSION_INFO_PATH]["nodeID"] = 0
            self.data[VERSION_INFO_PATH]["packageName"] = package_name
            self.data[VERSION_INFO_PATH]["pendingVersion"] = version
            self.data[VERSION_INFO_PATH]["startTime"] = TimestampToStr(time.time() - 60, formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())

            # Stage all of the nodes
            for node_id in self.data[ACTIVE_NODES_PATH].keys():
                self.data[INSTALLED_PACKAGES_PATH][node_id].append(package_name)

    def GetClientVisibleVolumes(self, clientIQN=None, chapUsername=None, chapPassword=None):
        with self.dataLock:
            volume_ids = []
            if clientIQN:
                for volgroup in self.data[VOLGROUP_PATH].values():
                    if clientIQN in volgroup["initiators"]:
                        volume_ids.extend(volgroup["volumes"])

            if chapUsername:
                for account in self.data[ACCOUNT_PATH].values():
                    if account["username"] == chapUsername and account["initiatorSecret"] == chapPassword:
                        volume_ids.extend(account["volumes"])
                        break

            return [self.data[VOLUME_PATH][vid]["iqn"] for vid in volume_ids]

    def CreateRandomVolumes(self, volumeCount, accountName, groupName=None):
        """
        Create some random volumes in an acount and volume group
        """
        with self.dataLock:
            account_id = None
            for account in self.data[ACCOUNT_PATH].values():
                if account["username"] == accountName:
                    account_id = account["accountID"]
                    break
            if not account_id:
                raise SolidFireError("Could not find account {}".format(accountName))

            volume_ids = RandomSequence(volumeCount, self.data[VOLUME_PATH].keys())
            for volume_id in volume_ids:
                self.data[VOLUME_PATH][volume_id] = self._NewVolumeJSON(clusterID=self.data[CLUSTER_ID_PATH],
                                                                     accountID=account_id,
                                                                     volumeName=RandomString(random.randint(6, 64)),
                                                                     volumeID=volume_id,
                                                                     volumeSize=random.randint(int(0.25*1024*1024), 2*1024*int(0.25*1024*1024)) * 4096,
                                                                     enable512e=random.choice([True, False]),
                                                                     access=random.choice(["readWrite", "readOnly", "locked", "replicationTarget"]),
                                                                     status="active")
                self.data[ACCOUNT_PATH][account_id]["volumes"].append(volume_id)

            if groupName:
                group_id = None
                for group in self.data[VOLGROUP_PATH].values():
                    if group["name"] == groupName:
                        group_id = group["volumeAccessGroupID"]
                if not group_id:
                    raise SolidFireError("Could not find group {}".format(groupName))
                self.data[VOLGROUP_PATH][group_id]["volumes"].extend(volume_ids)


    def _GetNextID(self):
        with self.dataLock:
            return self._GetNextIDUnlocked()

    def _GetNextIDUnlocked(self):
        next_id = self.data[NEXTID_PATH]
        self.data[NEXTID_PATH] += 1
        return next_id

    def _NewVolumeJSON(self, clusterID, accountID, volumeName, volumeID, volumeSize, enable512e=False, access="readWrite", minIOPS=100, maxIOPS=100000, burstIOPS=100000, status="active"):
        return {
            "access": access,
            "accountID": accountID,
            "attributes": { },
            "blockSize": 4096,
            "createTime": TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone()),
            "deleteTime": "",
            "enable512e": enable512e,
            "iqn": "iqn.2010-01.com.solidfire:{}.{}.{}".format(clusterID, volumeName, volumeID),
            "name": volumeName,
            "purgeTime": "",
            "qos": {
                "burstIOPS": burstIOPS,
                "burstTime": 60,
                "curve": {
                    4096: 100,
                    8192: 160,
                    16384: 270,
                    32768: 500,
                    65536: 1000,
                    131072: 1950,
                    262144: 3900,
                    524288: 7600,
                    1048576: 15000
                },
                "maxIOPS": maxIOPS,
                "minIOPS": minIOPS
            },
            "scsiEUIDeviceID": "7862316900000001f47acc0100000000",
            "scsiNAADeviceID": "6f47acc1000000007862316900000001",
            "sliceCount": 1,
            "status": status,
            "totalSize": volumeSize,
            "virtualVolumeID": None,
            "volumeAccessGroups": [ ],
            "volumeID": volumeID,
            "volumePairs": [ ]
        }

    def _GetOrAddInitiatorID(self, initiator):
        with self.dataLock:
            initiators = self.data[INITIATORS_PATH]
            for init_id, init_str in initiators.iteritems():
                if init_str == initiator:
                    return init_id
            init_id = self._GetNextIDUnlocked()
            initiators[init_id] = copy.copy(initiator)
            self.data[INITIATORS_PATH] = initiators
            return init_id

    def _ThrowIfAnyInitiatorInVolgroup(self, skipGroupID, initiatorList, ex):
        """Check that a list of initiators are not in any groups, other than the one referenced by skipGroupID"""
        with self.dataLock:
            volgroups = self.data[VOLGROUP_PATH]
            for init in initiatorList:
                for group in volgroups.values():
                    if group["volumeAccessGroupID"] == skipGroupID:
                        continue
                    if init in group["initiators"]:
                        raise ex

    def _ThrowIfAnyVolumeDoesNotExist(self, volumeIDList, ex):
        with self.dataLock:
            existing_volumes = self.data[VOLUME_PATH]
            unknown = set(volumeIDList).difference(existing_volumes.keys())
            if unknown:
                raise ex

    def AddAPIFailure(self, methodName, exceptionThrown, failCount, preSuccessCount, random):
        """Make methodName throw an exception instead of executing"""
        with self.dataLock:
            self.data[API_FAILURES_PATH][methodName] = {"ex" : exceptionThrown,
                                                        "failCount" : failCount,
                                                        "preSuccessCount" : preSuccessCount,
                                                        "random" : random}

    def RemoveAPIFailure(self, methodName):
        """Remove a previously added API failure method"""
        with self.dataLock:
            self.data[API_FAILURES_PATH].pop(methodName, None)

    def GetAPIFailure(self, methodName):
        with self.dataLock:
            failure_spec = self.data[API_FAILURES_PATH].get(methodName, None)
            if not failure_spec:
                return None

            if failure_spec["preSuccessCount"] > 0:
                failure_spec["preSuccessCount"] -= 1
                self.data[API_FAILURES_PATH][methodName] = failure_spec
                return None

            if failure_spec["random"]:
                if random.choice([True, False]):
                    # print "Generating a random error"
                    pass
                else:
                    # print "No random error"
                    return None

            if failure_spec["failCount"] == APIFailure.ALWAYS_FAIL:
                return copy.deepcopy(failure_spec["ex"])

            if failure_spec["failCount"] > 0:
                failure_spec["failCount"] -= 1
                self.data[API_FAILURES_PATH][methodName] = failure_spec
                return copy.deepcopy(failure_spec["ex"])

    def SetAPIEndpoints(self, endpoints):
        """Set the list of endpoints this cluster will support"""
        with self.dataLock:
            self.data[API_ENDPOINTS_PATH] = copy.deepcopy(endpoints)

    def GetAPIEndpoints(self):
        """Get the list of endpoints this cluster will support"""
        with self.dataLock:
            return copy.deepcopy(self.data[API_ENDPOINTS_PATH])

    def GetClusterVersion(self):
        with self.dataLock:
            return self.data[CLUSTER_VERSION_PATH]

    def SetClusterVersion(self, newVersion):
        with self.dataLock:
            self.data[CLUSTER_VERSION_PATH] = newVersion

    def Call(self, methodName, methodParams=None, *args, **kwargs):
        """Pretend to call an API function"""

        ip = kwargs.get("ip", "0.0.0.0")
        port = kwargs.get("port", 443)
        apiVersion = kwargs.get("apiVersion", 1.0)
        endpoint = kwargs.get("endpoint", "https://{}:{}/json-rpc/{}".format(ip, port, apiVersion))
        username = kwargs.get("username", None)
        password = kwargs.get("password", None)

        # Raise an exception if this method is set to fail
        ex = self.GetAPIFailure(APIFailure.ALL_METHODS) or self.GetAPIFailure(methodName)
        if ex:
            setattr(ex, "ip", ip)
            setattr(ex, "methodName", methodName)
            raise ex

        with self.dataLock:
            # Raise a 404 error if a per-node endpoint is requested on a cluster too old
            if port == 442 and SolidFireVersion(self.data[CLUSTER_VERSION_PATH]).major < 5:
                raise SolidFireApiError(methodName, methodParams, ip, endpoint, 'xUnknownAPIVersion', 500, 'HTTP Error 404: Not Found - url=[{}]'.format(endpoint))

            # Raise a 404 error if the requested API endpoint is greater than cluster version
            elif SolidFireVersion(self.data[CLUSTER_VERSION_PATH]).apiVersion < apiVersion:
                raise SolidFireApiError(methodName, methodParams, ip, endpoint, 'xUnknownAPIVersion', 500, 'HTTP Error 404: Not Found - url=[{}]'.format(endpoint))

        apiResponse = {}
        func = getattr(self, methodName, None)
        if func and callable(func):
            apiResponse = func(methodParams, ip, endpoint, apiVersion)
        else:
            raise NotImplementedError("'{}' call has not been faked".format(methodName))

        apiResponse = apiResponse or {}
        return apiResponse

    def HttpDownload(self, url, *args, **kwargs):
        """Pretend to download a URL from a cluster"""

        if url.endswith("slices.json"):
            return json.dumps(random.choice([self.data[SLICE_REPORT_HEALTHY_PATH], self.data[SLICE_REPORT_UNHEALTHY_PATH]]))
        elif url.endswith("bins.json"):
            return json.dumps(random.choice([self.data[BIN_REPORT_HEALTHY_PATH], self.data[BIN_REPORT_UNHEALTHY_PATH]]))
        else:
            raise NotImplementedError("{} URL has not been faked".format(url))


    #================
    # Cluster methods
    #================

    def GetAPI(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return { "supportedVersions": copy.deepcopy(self.data[API_ENDPOINTS_PATH]),
                     "currentVersion" : ".".join(self.data[CLUSTER_VERSION_PATH].split(".")[0:2])
                    }

    def ListActiveVolumes(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            volumes = copy.deepcopy(self.data[VOLUME_PATH].values())
            return { "volumes" : volumes }

    def ListVolumeAccessGroups(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            volgroups = copy.deepcopy(self.data[VOLGROUP_PATH].values())
            return { "volumeAccessGroups" : volgroups }

    def CreateVolumeAccessGroup(self, methodParams, ip="", endpoint="", apiVersion=""):
        name = methodParams.get("name", None)
        if not name:
            raise SolidFireApiError("CreateVolumeAccessGroup", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[name]")

        initiators = copy.deepcopy([init.lower() for init in methodParams.get("initiators", [])])
        init_ids = []
        if initiators:
            self._ThrowIfAnyInitiatorInVolgroup(0, initiators, SolidFireApiError("CreateVolumeAccessGroup", methodParams, ip, endpoint, "xExceededLimit", 500, "Exceeded maximum number of VolumeAccessGroups per Initiator"))
            init_ids = [self._GetOrAddInitiatorID(init) for init in initiators]

        volumes = copy.deepcopy(methodParams.get("volumes", []))
        luns = []
        if volumes:
            self._ThrowIfAnyVolumeDoesNotExist(volumes, SolidFireApiError("CreateVolumeAccessGroup", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "VolumeID xx does not exist."))
            luns = [ {"lun":lun, "volumeID":vid} for lun,vid in enumerate(volumes)]

        newid = self._GetNextID()
        newgroup = {"name" : name, "initiators" : initiators, "initiatorIDs" : init_ids, "volumes" : volumes, "deletedVolumes" : [], "attributes" : {}, "volumeAccessGroupID" : newid}
        with self.dataLock:
            self.data[VOLGROUP_PATH][newid] = newgroup
            self.data[VOLGROUP_LUNS_PATH][newid] = luns

        return { "volumeAccessGroup" : copy.deepcopy(newgroup), "volumeAccessGroupID" : newgroup["volumeAccessGroupID"] }

    def ModifyVolumeAccessGroup(self, methodParams, ip="", endpoint="", apiVersion=""):
        # Find the group
        groupid = methodParams.get("volumeAccessGroupID", None)
        if not groupid:
            raise SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeAccessGroupID]")

        with self.dataLock:
            if groupid not in self.data[VOLGROUP_PATH]:
                raise SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xVolumeAccessGroupIDDoesNotExist", 500, "VolumeAccessGroupID {} does not exist.".format(groupid))
            group = self.data[VOLGROUP_PATH][groupid]

            # Check the volumes
            volumes = methodParams.get("volumes", None)
            if volumes is not None:
                self._ThrowIfAnyVolumeDoesNotExist(volumes, SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "VolumeID xx does not exist."))
                volumes = copy.deepcopy(volumes)

            # Check the initiators
            initiators = methodParams.get("initiators", None)
            if initiators is not None:
                initiators = copy.deepcopy(initiators)
                self._ThrowIfAnyInitiatorInVolgroup(groupid, initiators, SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xExceededLimit", 500, "Exceeded maximum number of VolumeAccessGroups per Initiator"))

            # Update volumes
            if volumes is not None:
                group["volumes"] = copy.deepcopy(volumes)
                self.data[VOLGROUP_LUNS_PATH][groupid] = [ {"lun":lun, "volumeID":vid} for lun,vid in enumerate(volumes) ]
            # Update initiators
            if initiators is not None:
                group["initiators"] = initiators
                group["initiatorIDs"] = [self._GetOrAddInitiatorID(init) for init in group["initiators"]]
            self.data[VOLGROUP_PATH][groupid] = group

        return { "volumeAccessGroup" : copy.deepcopy(group) }

    def ModifyVolumeAccessGroupLunAssignments(self, methodParams, ip="", endpoint="", apiVersion=""):
        groupid = methodParams.get("volumeAccessGroupID", None)
        if not groupid:
            raise SolidFireApiError("ModifyVolumeAccessGroupLunAssignments", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeAccessGroupID]")
        luns = methodParams.get("lunAssignments", None)
        if not luns:
            raise SolidFireApiError("ModifyVolumeAccessGroupLunAssignments", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[lunAssignments]")

        with self.dataLock:
            if groupid not in self.data[VOLGROUP_PATH]:
                raise SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xVolumeAccessGroupIDDoesNotExist", 500, "VolumeAccessGroupID {} does not exist.".format(groupid))

            self.data[VOLGROUP_LUNS_PATH][groupid] = copy.deepcopy(luns)

            return { "volumeAccessGroupLunAssignments" : { "deletedLunAssignments" : [], "lunAssignments" : copy.deepcopy(luns) } }

    def DeleteVolumeAccessGroup(self, methodParams, ip="", endpoint="", apiVersion=""):
        groupid = methodParams.get("volumeAccessGroupID", None)
        if not groupid:
            raise SolidFireApiError("DeleteVolumeAccessGroup", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeAccessGroupID]")
        with self.dataLock:
            if groupid not in self.data[VOLGROUP_PATH]:
                raise SolidFireApiError("ModifyVolumeAccessGroup", methodParams, ip, endpoint, "xVolumeAccessGroupIDDoesNotExist", 500, "VolumeAccessGroupID {} does not exist.".format(groupid))
            del self.data[VOLGROUP_PATH][groupid]
        return {}

    def ListAccounts(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return { "accounts" : copy.deepcopy(self.data[ACCOUNT_PATH].values()) }

    def AddAccount(self, methodParams, ip="", endpoint="", apiVersion=""):
        name = methodParams.get("username", None)
        if not name:
            raise SolidFireApiError("AddAccount", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[username]")
        init_secret = methodParams.get("initiatorSecret", RandomString(12))
        targ_secret = methodParams.get("targetSecret", RandomString(12))

        with self.dataLock:
            newid = self._GetNextIDUnlocked()
            newaccount = { "accountID": newid, "attributes": {}, "initiatorSecret": init_secret, "status": "active", "targetSecret": targ_secret, "username": name, "volumes": [] }
            self.data[ACCOUNT_PATH][newid] = newaccount

            return {"account" : copy.deepcopy(newaccount), "accountID" : newid}

    def RemoveAccount(self, methodParams, ip="", endpoint="", apiVersion=""):
        account_id = methodParams.get("accountID", None)
        if not account_id:
            raise SolidFireApiError("RemoveAccount", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[accountID]")
        with self.dataLock:
            if account_id not in self.data[ACCOUNT_PATH]:
                raise SolidFireApiError("RemoveAccount", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(account_id))
            del self.data[ACCOUNT_PATH][account_id]
        return {}

    def GetClusterInfo(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return { "clusterInfo": { "attributes": {},
                                      "encryptionAtRestState": "disabled",
                                      "ensemble": [node["sip"] for node in self.data[ACTIVE_NODES_PATH].values() if node["nodeID"] in self.data[ENSEMBLE_NODES_PATH]],
                                        "mvip": ip,
                                        "mvipNodeID": self.data[CLUSTER_MASTER_PATH],
                                        "name": self.data[CLUSTER_NAME_PATH],
                                        "repCount": 2,
                                        "svip": self.data[SVIP_PATH],
                                        "svipNodeID": self.data[CLUSTER_MASTER_PATH],
                                        "uniqueID": self.data[CLUSTER_ID_PATH],
                                        "uuid": "0557bed4-5374-4369-a56a-38e3d7ec5328"} }

    def GetLimits(self, methodParams, ip="", endpoint="", apiVersion=""):
        return {
            "accountCountMax": 5000,
            "accountNameLengthMax": 64,
            "accountNameLengthMin": 1,
            "bulkVolumeJobsPerNodeMax": 8,
            "bulkVolumeJobsPerVolumeMax": 2,
            "cloneJobsPerVolumeMax": 2,
            "clusterPairsCountMax": 4,
            "initiatorAliasLengthMax": 224,
            "initiatorCountMax": 0,
            "initiatorNameLengthMax": 224,
            "initiatorsPerVolumeAccessGroupCountMax": 64,
            "iscsiSessionsFromFibreChannelNodesMax": 4096,
            "secretLengthMax": 16,
            "secretLengthMin": 12,
            "snapshotNameLengthMax": 64,
            "snapshotsPerVolumeMax": 32,
            "volumeAccessGroupCountMax": 1000,
            "volumeAccessGroupLunMax": 16383,
            "volumeAccessGroupNameLengthMax": 64,
            "volumeAccessGroupNameLengthMin": 1,
            "volumeAccessGroupsPerInitiatorCountMax": 1,
            "volumeAccessGroupsPerVolumeCountMax": 4,
            "volumeBurstIOPSMax": 100000,
            "volumeBurstIOPSMin": 100,
            "volumeCountMax": 4000,
            "volumeMaxIOPSMax": 100000,
            "volumeMaxIOPSMin": 100,
            "volumeMinIOPSMax": 15000,
            "volumeMinIOPSMin": 50,
            "volumeNameLengthMax": 64,
            "volumeNameLengthMin": 1,
            "volumeSizeMax": 8000000491520,
            "volumeSizeMin": 1000000000,
            "volumesPerAccountCountMax": 2000,
            "volumesPerGroupSnapshotMax": 32,
            "volumesPerVolumeAccessGroupCountMax": 2000
        }

    def ModifyVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")

        with self.dataLock:
            if volume_id not in self.data[VOLUME_PATH]:
                raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "VolumeID {} does not exist.".format(volume_id))
            volume = self.data[VOLUME_PATH][volume_id]

            # Move volume from one account to another
            if "accountID" in methodParams and methodParams["accountID"] != volume["accountID"]:
                new_account_id = methodParams["accountID"]
                # Get the old and new accounts
                if volume["accountID"] not in self.data[ACCOUNT_PATH]:
                    raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(volume["accountID"]))
                oldaccount = self.data[ACCOUNT_PATH][volume["accountID"]]
                if new_account_id not in self.data[ACCOUNT_PATH]:
                    raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(new_account_id))
                account = self.data[ACCOUNT_PATH][new_account_id]

                # Remove the volumeID from the original account and add it to the new account
                oldaccount["volumes"].remove(volume_id)
                self.data[ACCOUNT_PATH][oldaccount["accountID"]] = oldaccount
                account["volumes"].append(volume_id)
                self.data[ACCOUNT_PATH][new_account_id] = account

                # Update the accountID of the volume
                volume["accountID"] = new_account_id

            # Increase the size of the volume
            if "totalSize" in methodParams and methodParams["totalSize"] != volume["totalSize"]:
                newsize = methodParams["totalSize"]
                if newsize < volume["totalSize"]:
                    raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xVolumeShrinkProhibited", 500, "Lowering a volume size is not allowed.")
                if newsize % 4096 != 0:
                    newsize = (newsize/4096 + 1) * 4096
                assert newsize % 4096 == 0
                volume["totalSize"] = newsize

            # Change the volume access
            if "access" in methodParams and methodParams["access"] != volume["access"]:
                newaccess = methodParams["access"]
                if newaccess not in ["readWrite", "readOnly", "locked", "replicationTarget"]:
                    raise SolidFireApiError("ModifyVolume", methodParams, ip, endpoint, "xUnrecognizedEnumString", 500, newaccess)
                volume["access"] = newaccess

            # Change QoS settings
            if "qos" in methodParams and methodParams["qos"]:
                for key in ["minIOPS", "maxIOPS", "burstIOPS"]:
                    if key in methodParams["qos"] and methodParams["qos"][key]:
                        volume["qos"][key] = methodParams["qos"][key]

            # Set attributes
            if "attributes" in methodParams and methodParams["attributes"]:
                volume["attributes"] = copy.deepcopy(methodParams["attributes"])

            # Update the volume
            self.data[VOLUME_PATH][volume_id] = volume

        return {"volume": copy.deepcopy(volume)}

    def CloneVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("CloneVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")
        clone_name = methodParams.get("name", None)
        if not volume_id:
            raise SolidFireApiError("CloneVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[name]")
        
        with self.dataLock:
            if volume_id not in self.data[VOLUME_PATH]:
                raise SolidFireApiError("CloneVolume", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "VolumeID {} does not exist.".format(volume_id))
            volume = self.data[VOLUME_PATH][volume_id]

            account_id = methodParams.get("newAccountID", volume["accountID"])
            if account_id not in self.data[ACCOUNT_PATH]:
                raise SolidFireApiError("CloneVolume", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(account_id))
            account = self.data[ACCOUNT_PATH][account_id]

            size = methodParams.get("newSize", volume["totalSize"])
            access = methodParams.get("access", "readWrite")

            clone_id = self._GetNextIDUnlocked()
            if account:
                account["volumes"].append(clone_id)
                self.data[ACCOUNT_PATH][account_id] = account

            clone = self._NewVolumeJSON(clusterID=self.data[CLUSTER_ID_PATH],
                                        accountID=account_id,
                                        volumeName=clone_name,
                                        volumeID=clone_id,
                                        volumeSize=size,
                                        enable512e=random.choice([True, False]),
                                        access=access)
            self.data[VOLUME_PATH][clone_id] = clone

            handle_id = self._GetNextIDUnlocked()
            handle = {
                "createTime" : TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone()),
                "lastUpdateTime" : TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone()),
                "result": {
                    "cloneID" : clone_id,
                    "message" : "Clone complete.",
                    "volumeID" : volume_id
                },
                "resultType" : "Clone",
                "status" : "complete"
            }
            self.data[ASYNC_HANDLES_PATH][handle_id] = handle

        return { "asyncHandle" : handle_id }

    def GetAsyncResult(self, methodParams, ip="", endpoint="", apiVersion=""):
        async_id = methodParams.get("asyncHandle", None)
        if not async_id:
            raise SolidFireApiError("GetAsyncResult", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[asyncHandle]")
        with self.dataLock:
            if async_id not in self.data[ASYNC_HANDLES_PATH]:
                raise SolidFireApiError("GetAsyncHandle", methodParams, ip, endpoint, "xDBNoSuchPath", 500, "DBClient operation requested on a non-existent path at [/asyncresults/{}]".format(async_id))
        handle = copy.deepcopy(self.data[ASYNC_HANDLES_PATH][async_id])
        return handle

    def CreateVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        account_id = methodParams.get("accountID", None)
        if not account_id:
            raise SolidFireApiError("CreateVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[accountID]")
        name = methodParams.get("name", None)
        if not name:
            raise SolidFireApiError("CreateVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[name]")
        size = methodParams.get("totalSize", None)
        if not size:
            raise SolidFireApiError("CreateVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[totalSize]")
        enable512e = methodParams.get("enable512e", False)
        qos = methodParams.get("qos", {})
        min_iops = qos.get("minIOPS", 50)
        max_iops = qos.get("maxIOPS", 15000)
        burst_iops = qos.get("burstIOPS", 15000)

        with self.dataLock:
            if account_id not in self.data[ACCOUNT_PATH]:
                raise SolidFireApiError("CreateVolume", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(account_id))
            account = self.data[ACCOUNT_PATH][account_id]

            vol_id = self._GetNextIDUnlocked()
            vol = self._NewVolumeJSON(clusterID=self.data[CLUSTER_ID_PATH],
                                      accountID=account_id,
                                      volumeName=name,
                                      volumeID=vol_id,
                                      volumeSize=size,
                                      enable512e=enable512e,
                                      minIOPS=min_iops,
                                      maxIOPS=max_iops,
                                      burstIOPS=burst_iops)
            account["volumes"].append(vol_id)
            self.data[VOLUME_PATH][vol_id] = vol
            self.data[ACCOUNT_PATH][account_id] = account

            return { "volumeID" : vol_id, "volume" : copy.deepcopy(vol) }

    def CreateMultipleVolumes(self, methodParams, ip="", endpoint="", apiVersion=""):
        account_id = methodParams.get("accountID", None)
        if not account_id:
            raise SolidFireApiError("CreateMultipleVolumes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[accountID]")
        vol_names = methodParams.get("names", None)
        if not vol_names:
            raise SolidFireApiError("CreateMultipleVolumes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[names]")
        size = methodParams.get("totalSize", None)
        if not size:
            raise SolidFireApiError("CreateMultipleVolumes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[totalSize]")
        enable512e = methodParams.get("enable512e", False)
        qos = methodParams.get("qos", {})
        min_iops = qos.get("minIOPS", 50)
        max_iops = qos.get("maxIOPS", 15000)
        burst_iops = qos.get("burstIOPS", 15000)

        with self.dataLock:
            if account_id not in self.data[ACCOUNT_PATH]:
                raise SolidFireApiError("CreateMultipleVolumes", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(account_id))
            account = self.data[ACCOUNT_PATH][account_id]

            new_vol_map = {}
            for name in vol_names:
                vol_id = self._GetNextIDUnlocked()
                vol = self._NewVolumeJSON(clusterID=self.data[CLUSTER_ID_PATH],
                                          accountID=account_id,
                                          volumeName=name,
                                          volumeID=vol_id,
                                          volumeSize=size,
                                          enable512e=enable512e,
                                          minIOPS=min_iops,
                                          maxIOPS=max_iops,
                                          burstIOPS=burst_iops)
                account["volumes"].append(vol_id)
                self.data[VOLUME_PATH][vol_id] = vol
                self.data[ACCOUNT_PATH][account_id] = account
                new_vol_map[vol_id] = vol

            return { "volumeIDs" : new_vol_map.keys(), "volumes" : copy.deepcopy(new_vol_map.values()) }

    def DeleteVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("DeleteVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")

        with self.dataLock:
            if volume_id not in self.data[VOLUME_PATH]:
                raise SolidFireApiError("DeleteVolume", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "Volume {} does not exist".format(volume_id))
            vol = self.data[VOLUME_PATH][volume_id]

            self.data[DELETED_VOLUMES_PATH][volume_id] = vol
            del self.data[VOLUME_PATH][volume_id]

            return { "volume" : copy.deepcopy(vol) }

    def DeleteVolumes(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_ids = methodParams.get("volumeIDs", None)
        if volume_ids is None:
            raise SolidFireApiError("DeleteVolumes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeIDs]")
        elif volume_ids == []:
            raise SolidFireApiError("DeleteVolumes", methodParams, ip, endpoint, "xInvalidParameter", 500, "No volume ID, account ID, or volume access group ID list specified")

        with self.dataLock:
            for volume_id in volume_ids:
                if volume_id not in self.data[VOLUME_PATH]:
                    raise SolidFireApiError("DeleteVolume", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "Volume {} does not exist".format(volume_id))
            volume_map = {}
            for volume_id in volume_ids:
                vol = self.data[VOLUME_PATH][volume_id]
                volume_map[volume_id] = vol

                self.data[DELETED_VOLUMES_PATH][volume_id] = vol
                del self.data[VOLUME_PATH][volume_id]

            return { "volumeIDs" : volume_map.keys(), "volumes" : copy.deepcopy(volume_map.values()) }

    def PurgeDeletedVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("PurgeDeletedVolume", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")

        with self.dataLock:
            vol = self.data[DELETED_VOLUMES_PATH][volume_id]

            # Remove the volume from the account
            account = self.data[ACCOUNT_PATH][vol["accountID"]]
            account["volumes"].remove(vol["volumeID"])
            self.data[ACCOUNT_PATH][vol["accountID"]] = account

            # Remove the volume from deleted volumes
            del self.data[DELETED_VOLUMES_PATH][volume_id]

        return {}

    def PurgeDeletedVolumes(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_ids = methodParams.get("volumeIDs", None)
        if not volume_ids:
            raise SolidFireApiError("PurgeDeletedVolumes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeIDs]")

        with self.dataLock:
            for volume_id in volume_ids:
                vol = self.data[DELETED_VOLUMES_PATH][volume_id]

                # Remove the volume from the account
                account = self.data[ACCOUNT_PATH][vol["accountID"]]
                account["volumes"].remove(vol["volumeID"])
                self.data[ACCOUNT_PATH][vol["accountID"]] = account

                # Remove the volume from deleted volumes
                del self.data[DELETED_VOLUMES_PATH][volume_id]

        return {}

    def ListVolumesForAccount(self, methodParams, ip="", endpoint="", apiVersion=""):
        account_id = methodParams.get("accountID", None)
        if not account_id:
            raise SolidFireApiError("RemoveAccount", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[accountID]")
        
        with self.dataLock:
            if account_id not in self.data[ACCOUNT_PATH]:
                raise SolidFireApiError("ListVolumesForAccount", methodParams, ip, endpoint, "xAccountIDDoesNotExist", 500, "Illegal AccountID {}".format(account_id))
            account = self.data[ACCOUNT_PATH][account_id]
            
            allvolumes = self.data[VOLUME_PATH]
            allvolumes.update(self.data[DELETED_VOLUMES_PATH])

            account_volumes = [ vol for vol_id, vol in allvolumes.iteritems() if vol_id in account["volumes"]]
            return { "volumes" : copy.deepcopy(account_volumes) }

    def ListDeletedVolumes(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            volumes = self.data[DELETED_VOLUMES_PATH]
            return { "volumes" : copy.deepcopy(volumes.values()) }

    def ModifyVolumePair(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("ModifyVolumePair", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")
        
        with self.dataLock:
            if volume_id not in self.data[VOLUME_PATH]:
                raise SolidFireApiError("DeleteVolume", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "Volume {} does not exist".format(volume_id))
            vol = self.data[VOLUME_PATH][volume_id]

            if "volumePairs" not in vol:
                raise SolidFireApiError("ModifyVolumePair", methodParams, ip, endpoint, "xVolumeNotPaired", 500, "Volume not paired.")

            if "pausedManual" in methodParams and methodParams["pausedManual"] is not None:
                if methodParams["pausedManual"]:
                    vol["volumePairs"][0]["remoteReplication"]["state"] = "PausedManual"
                    vol["volumePairs"][0]["remoteReplication"]["snapshotReplication"]["state"] = "PausedManual"
                else:
                    vol["volumePairs"][0]["remoteReplication"]["state"] = "Active"
                    vol["volumePairs"][0]["remoteReplication"]["snapshotReplication"]["state"] = "Idle"

            # Update the volume
            self.data[VOLUME_PATH][volume_id] = vol

        return {}

    def ListDrives(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            alldrives = self.data[DRIVES_PATH]
            return { "drives" : copy.deepcopy(alldrives.values()) }

    def ListActiveNodes(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            nodes = self.data[ACTIVE_NODES_PATH]
            return { "nodes" : copy.deepcopy(nodes.values()) }

    def ListPendingNodes(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            nodes = self.data[PENDING_NODES_PATH]
            return { "pendingNodes" : copy.deepcopy(nodes.values()) }

    def ListAllNodes(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            nodes = self.data[ACTIVE_NODES_PATH]
            pending_nodes = self.data[PENDING_NODES_PATH]
            return { "nodes" : copy.deepcopy(nodes.values()), "pendingNodes" : copy.deepcopy(pending_nodes.values()) }

    def AddDrives(self, methodParams, ip="", endpoint="", apiVersion=""):
        drives_to_add = methodParams.get("drives")
        if not drives_to_add:
            raise SolidFireApiError("AddDrives", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[drives]")

        with self.dataLock:
            alldrives = self.data[DRIVES_PATH]
            for drive in drives_to_add:
                if isinstance(drive, dict) and "driveID" in drive:
                    drive_id = drive["driveID"]
                else:
                    drive_id = int(drive)

                if drive_id not in alldrives.keys():
                    raise SolidFireApiError("AddDrives", methodParams, ip, endpoint, "xDriveIDDoesNotExist", 500, "Drive {} does not exist".format(drive_id))

                alldrives[drive_id]["status"] = "active"

            self.data[DRIVES_PATH] = alldrives

        return {}

    def RemoveDrives(self, methodParams, ip="", endpoint="", apiVersion=""):
        drives_to_rem = methodParams.get("drives")
        if not drives_to_rem:
            raise SolidFireApiError("RemoveDrives", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[drives]")

        with self.dataLock:
            alldrives = self.data[DRIVES_PATH]
            for drive in drives_to_rem:
                if isinstance(drive, dict) and "driveID" in drive:
                    drive_id = drive["driveID"]
                else:
                    drive_id = int(drive)

                if drive_id not in alldrives.keys():
                    raise SolidFireApiError("RemoveDrives", methodParams, ip, endpoint, "xDriveIDDoesNotExist", 500, "Drive {} does not exist".format(drive_id))

                alldrives[drive_id]["status"] = "available"
            self.data[DRIVES_PATH] = alldrives

        return {}

    def ListClusterFaults(self, methodParams, ip="", endpoint="", apiVersion=""):
        fault_types = methodParams.get("faultTypes", "all")
        faults = { "faults" : [] }

        if fault_types in ["all", "current"]:
            if random.choice([True, False]):
                # Create a set of "healthy" faults
                faults["faults"].extend(copy.deepcopy(random.sample(HEALTHY_FAULTS, random.randint(1, len(HEALTHY_FAULTS)-1))))
    
            else:
                # Create a set of "unhealthy" faults
                faults["faults"].extend(copy.deepcopy(random.sample(UNHEALTHY_FAULTS, random.randint(1, len(UNHEALTHY_FAULTS)-1))))
    
            for idx, fault in enumerate(faults["faults"], start=1):
                fault["clusterFaultID"] = idx
                fault["date"] = TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())
                fault["resolved"] = False

        if fault_types in ["all", "resolved"]:
            # Add random resolved faults
            resolved = random.sample(HEALTHY_FAULTS + UNHEALTHY_FAULTS, 1, len(HEALTHY_FAULTS) + len(UNHEALTHY_FAULTS))
            for idx, fault in enumerate(resolved, start=len(faults["faults"])):
                fault["clusterFaultID"] = idx
                fault["resolved"] = True
            faults["faults"].extend(resolved)

        return faults

    def AddNodes(self, methodParams, ip="", endpoint="", apiVersion=""):
        node_ids = methodParams.get("pendingNodes")
        if not node_ids:
            raise SolidFireApiError("AddNodes", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[pendingNodes]")
        
        with self.dataLock:
            nodes = self.data[ACTIVE_NODES_PATH]
            pending_nodes = self.data[PENDING_NODES_PATH]
            drives = self.data[DRIVES_PATH]
            retmap = []
            for pending_id in node_ids:
                if pending_id not in pending_nodes.keys():
                    raise SolidFireApiError("AddNodes", methodParams, ip, endpoint, "xDBNoSuchPath", 500, "DBClient operation requested on a non-existent path at [/pendingnodes/{}]".format(pending_id))
                node = pending_nodes.pop(pending_id)

                # Move each node from pending to active
                new_id = self._GetNextIDUnlocked()
                
                # FC specifics
                fs_id = 0
                fc_pg = None
                if node["platformInfo"]["nodeType"] == "FC0025":
                    fs_id = new_id+2
                    fc_pg = len([node for node in nodes if node["platformInfo"]["nodeType"] == "FC0025"])

                retmap.append({pending_id : new_id})
                node.pop("compatible", None)
                node.pop("assignedNodeID", None)
                node.pop("pendingNodeID", None)
                node.update({"associatedFServiceID" : fs_id,
                             "associatedMasterServiceID" : new_id+1,
                             "attributes" : {},
                             "fibreChannelTargetPortGroup" : fc_pg,
                             "nodeID" : new_id,
                             "virtualNetworks" : []})
                nodes[new_id] = node

                # Create drives for the node
                if node["platformInfo"]["nodeType"] != "FC0025":
                    for idx in xrange(self.data[DRIVES_PER_NODE_PATH]):
                        drive_id = self._GetNextID()
                        drives[drive_id] = {
                            "attributes": { },
                            "capacity": 300069052416,
                            "driveID": drive_id,
                            "nodeID": new_id,
                            "serial": "scsi-SATA_INTEL_SSD{}".format(random.randint(10000000,99999999)),
                            "slot": idx - 1,
                            "status": "available",
                            "type": "volume" if idx == 0 else "block"
                        }

            # Commit the changes
            self.data[PENDING_NODES_PATH] = pending_nodes
            self.data[ACTIVE_NODES_PATH] = nodes
            self.data[DRIVES_PATH] = drives

            return { "nodes" : copy.deepcopy(retmap) }

    def ListClusterAdmins(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            admins = self.data[ADMINS_PATH]
            return {"clusterAdmins" : copy.deepcopy(admins.values()) }

    def GetClusterMasterNodeID(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return {"nodeID" : self.data[CLUSTER_MASTER_PATH]}

    def GetClusterVersionInfo(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            api_ver = ".".join(self.data[CLUSTER_VERSION_PATH].split(".")[0:2])
            ver = {
                "clusterAPIVersion" : api_ver,
                "clusterVersion" : self.data[CLUSTER_VERSION_PATH],
                "clusterVersionInfo" : []
            }
            for node_id, node_version in self.data[NODE_VERSION_PATH].items():
                ver["clusterVersionInfo"].append({
                    "nodeID" : node_id,
                    "nodeVersion" : node_version,
                    "nodeInternalRevision" : "BuildType=Release Element=ELEMENT Release=ELEMENT ReleaseShort=ELEMENT Version=VERSION sfdev=9.14 Repository=ELEMENT Revision=ce906c4d4aae Options=timing,timing BuildDate={}".format(TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())).replace("ELEMENT", "unobtanium").replace("VERSION", self.data[CLUSTER_VERSION_PATH])
                })

            if SolidFireVersion(self.data[CLUSTER_VERSION_PATH]).major > 5:
                ver.update({"softwareVersionInfo" : copy.deepcopy(self.data[VERSION_INFO_PATH])})
            return ver

    def GetConstants(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return copy.deepcopy(self.data[CONSTANTS_PATH])

    def SetConstants(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            for key, value in methodParams.iteritems():
                self.data[CONSTANTS_PATH][key] = value

    def SetRepositories(self, methodParams, ip="", endpoint="", apiVersion=""):
        repos = copy.deepcopy(methodParams.get("repositories", None))
        if not repos:
            raise SolidFireApiError("SetRepositories", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[repositories]")

        for repo in repos:
            if "host" not in repo:
                raise SolidFireApiError("SetRepositories", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[host]")
            if "port" not in repo:
                repo["port"] = 0

        with self.dataLock:
            self.data[REPOS_PATH] = repos # note we already made a copy so direct assignment is safe
        return {}

    def ListRepositories(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            return {"hosts" : copy.deepcopy(self.data[REPOS_PATH])}

    def ListAptSourceLines(self, methodParams, ip="", endpoint="", apiVersion=""):
        return {"sourceLines" : []}

    def SetAptSourceLines(self, methodParams, ip="", endpoint="", apiVersion=""):
        if not methodParams.get("sourceLines", None):
            raise SolidFireApiError("SetAptSourceLines", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[sourceLines]")
        return {}

    def StartUpgrade(self, methodParams, ip="", endpoint="", apiVersion=""):
        package_name = methodParams.get("packageName", None)
        if not package_name:
            raise SolidFireApiError("StartUpgrade", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[packageName]")
        version = package_name.split("-")[-1]

        with self.dataLock:
            self.data[VERSION_INFO_PATH]["nodeID"] = 0
            self.data[VERSION_INFO_PATH]["packageName"] = package_name
            self.data[VERSION_INFO_PATH]["pendingVersion"] = version
            self.data[VERSION_INFO_PATH]["startTime"] = TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())
        return {}

    def SetUpgradeNodeId(self, methodParams, ip="", endpoint="", apiVersion=""):
        node_id = methodParams.get("nodeID", 0)

        with self.dataLock:
            self.data[VERSION_INFO_PATH]["nodeID"] = node_id

    def FinishUpgrade(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            self.data[CLUSTER_VERSION_PATH] = self.data[VERSION_INFO_PATH]["pendingVersion"]
            self.data[VERSION_INFO_PATH]["pendingVersion"] = ""
            self.data[VERSION_INFO_PATH]["startTime"] = ""

    def GetRawStats(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            result = {}
            node_count = len(self.data[ACTIVE_NODES_PATH].keys())
            for idx in xrange(node_count):
                key = "service-{}".format(idx)
                result[key] = {}
                result[key]["scacheBytesInUse"] = 0

            return result

    def GetServiceStatus(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            result = {}
            node_count = len(self.data[ACTIVE_NODES_PATH].keys())

            status = {idx : True for idx in xrange(node_count * self.data[DRIVES_PER_NODE_PATH])}
            return { "success" : status }

    def ListVolumeStatsByVolume(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            node_count = len(self.data[ACTIVE_NODES_PATH].keys())
            stats = []
            for volume in self.data[VOLUME_PATH].values():
                stats.append({
                    "accountID": volume["accountID"],
                    "actualIOPS":0,
                    "asyncDelay":None,
                    "averageIOPSize":0,
                    "burstIOPSCredit":0,
                    "clientQueueDepth":0,
                    "desiredMetadataHosts":None,
                    "latencyUSec":0,
                    "metadataHosts":{
                       "deadSecondaries":[],
                       "liveSecondaries": [random.choice(range(node_count * self.data[DRIVES_PER_NODE_PATH]))],
                       "primary": random.choice(range(node_count * self.data[DRIVES_PER_NODE_PATH]))
                    },
                    "nonZeroBlocks":0,
                    "readBytes":0,
                    "readBytesLastSample":0,
                    "readLatencyUSec":0,
                    "readOps":0,
                    "readOpsLastSample":0,
                    "samplePeriodMSec":0,
                    "throttle":0,
                    "timestamp": TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone()),
                    "unalignedReads":0,
                    "unalignedWrites":0,
                    "volumeAccessGroups": volume["volumeAccessGroups"],
                    "volumeID": volume["volumeID"],
                    "volumeSize": volume["totalSize"],
                    "volumeUtilization":0,
                    "writeBytes":0,
                    "writeBytesLastSample":0,
                    "writeLatencyUSec":0,
                    "writeOps":0,
                    "writeOpsLastSample":0,
                    "zeroBlocks": volume["totalSize"] / 4096
                })

            return { "volumeStats" : stats }

    def GetClusterHardwareInfo(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            node_info = {}
            for node in self.data[ACTIVE_NODES_PATH].values():
                node_info[node["nodeID"]] = {}
                node_info[node["nodeID"]]["platform"] = copy.deepcopy(node["platformInfo"])
            return { "clusterHardwareInfo" : { "nodes" : node_info } }

    def MovePrimariesAwayFromNode(self, methodParams, ip="", endpoint="", apiVersion=""):
        return {}

    def StartClusterBSCheck(self, methodParams, ip="", endpoint="", apiVersion=""):
        return {}

    def GetRepositoryPackages(self, methodParams, ip="", endpoint="", apiVersion=""):
        with self.dataLock:
            if SolidFireVersion(self.data[CLUSTER_VERSION_PATH]).major > 5 or float(apiVersion) >= 6.0:
                raise NotImplementedError("GetRepositoryPackages was removed after Boron")

            result = {}
            result["currentPackages"] = []
            result["availablePackages"] = []

            node_id = self.data[CLUSTER_MASTER_PATH]
            for pkg_name in self.data[INSTALLED_PACKAGES_PATH][node_id]:
                pkg_ver = pkg_name.split("-")[-1]
                result["currentPackages"].append({
                    "packageName" : pkg_name,
                    "versionNumber" : pkg_ver
                })
            for pkg_name in self.data[AVAILABLE_PACKAGES_PATH]:
                if pkg_name in self.data[INSTALLED_PACKAGES_PATH][node_id]:
                    continue
                pkg_ver = pkg_name.split("-")[-1]
                result["availablePackages"].append({
                    "packageName" : pkg_name,
                    "versionNumber" : pkg_ver
                })

            return result

    def GetVolumeStats(self, methodParams, ip="", endpoint="", apiVersion=""):
        volume_id = methodParams.get("volumeID", None)
        if not volume_id:
            raise SolidFireApiError("GetVolumeStats", methodParams, ip, endpoint, "xMissingParameter", 500, "Missing member=[volumeID]")

        with self.dataLock:
            if volume_id not in self.data[VOLUME_PATH]:
                raise SolidFireApiError("GetVolumeStats", methodParams, ip, endpoint, "xVolumeIDDoesNotExist", 500, "VolumeID {} does not exist.".format(volume_id))
            #volume = self.data[VOLUME_PATH][volume_id]

            stats = {}
            stats["volumeStats"] = {}
            stats["volumeStats"]["metadataHosts"] = {}
            stats["volumeStats"]["metadataHosts"]["primary"] = random.randint(20, 10000)
            stats["volumeStats"]["metadataHosts"]["liveSecondaries"] = []
            stats["volumeStats"]["metadataHosts"]["deadSecondaries"] = []
            if random.choice([True, False]):
                stats["volumeStats"]["metadataHosts"]["liveSecondaries"].append(random.randint(20, 10000))
            else:
                stats["volumeStats"]["metadataHosts"]["deadSecondaries"].append(random.randint(20, 10000))

            return stats

    def ForceWholeFileSync(self, methodParams, ip="", endpoint="", apiVersion=""):
        return {}



    #=============
    # Node methods
    #=============

    def GetDriveConfig(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return { "driveConfig" : {
                "numBlockActual" : self.data[DRIVES_PER_NODE_PATH] - 1,
                "numBlockExpected" : self.data[DRIVES_PER_NODE_PATH] - 1,
                "numSliceActual" : 1,
                "numSliceExpected" : 1,
                "numTotalActual" : self.data[DRIVES_PER_NODE_PATH],
                "numTotalExpected" : self.data[DRIVES_PER_NODE_PATH]
            }
        }

    def SetConfig(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def ListTests(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return { "tests": ["TestConnectEnsemble",
                           "TestConnectMvip",
                           "TestConnectSvip",
                           "TestDrives",
                           "TestHardwareConfig",
                           "TestLocateCluster",
                           "TestPing",
                           "TestLocalConnectivity",
                           "TestRemoteConnectivity",
                           "TestNetworkConfig"]}

    def AptClearCache(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def AptInstall(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            pending_version = self.data[VERSION_INFO_PATH]["pendingVersion"]
            if not pending_version:
                raise SolidFireApiError("AptInstall", methodParams, nodeIP, endpoint, "xSoftwareInstallNotInProgress", 500, "packageName=")

            for node in self.data[ACTIVE_NODES_PATH].values():
                if node["mip"] == nodeIP:
                    node_id = node["nodeID"]
                    break
            self.data[INSTALLED_PACKAGES_PATH][node_id].append(self.data[VERSION_INFO_PATH]["packageName"])

        return {}

    def AptUpgrade(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def AptUpdate(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {"output": "Get: 1 http://archive precise Release.gpg [490 B] Get: 2 http://archive precise-updates Release.gpg [490 B] Get: 3 http://archive precise-security Release.gpg [490 B] Get: 4 http://archive precise Release.gpg [490 B] Get: 5 http://archive Release.gpg [490 B] Get: 6 http://archive precise Release [8955 B] Get: 7 http://archive precise-updates Release [8965 B] Get: 8 http://archive precise-security Release [4875 B] Get: 9 http://archive precise Release [953 B] Get: 10 http://archive Release [909 B] Get: 11 http://archive precise/main amd64 Packages [133 kB] Get: 12 http://archive precise/restricted amd64 Packages [14 B] Get: 13 http://archive precise/universe amd64 Packages [12.6 kB] Get: 14 http://archive precise/multiverse amd64 Packages [961 B] Ign http://archive precise/main TranslationIndex Ign http://archive precise/multiverse TranslationIndex Ign http://archive precise/restricted TranslationIndex Ign http://archive precise/universe TranslationIndex Get: 15 http://archive precise-updates/main amd64 Packages [100 kB] Get: 16 http://archive precise-updates/restricted amd64 Packages [14 B] Get: 17 http://archive precise-updates/universe amd64 Packages [5676 B] Get: 18 http://archive precise-updates/multiverse amd64 Packages [597 B] Ign http://archive precise-updates/main TranslationIndex Ign http://archive precise-updates/multiverse TranslationIndex Ign http://archive precise-updates/restricted TranslationIndex Ign http://archive precise-updates/universe TranslationIndex Get: 19 http://archive precise-security/main amd64 Packages [68.1 kB] Get: 20 http://archive precise-security/restricted amd64 Packages [14 B] Get: 21 http://archive precise-security/universe amd64 Packages [5015 B] Get: 22 http://archive precise-security/multiverse amd64 Packages [573 B] Ign http://archive precise-security/main TranslationIndex Ign http://archive precise-security/multiverse TranslationIndex Ign http://archive precise-security/restricted TranslationIndex Ign http://archive precise-security/universe TranslationIndex Get: 23 http://archive precise/main amd64 Packages [1799 kB] Ign http://archive precise/main TranslationIndex Get: 24 http://archive Packages [10.7 kB] Ign http://archive Translation-en Ign http://archive precise/main Translation-en Ign http://archive precise/multiverse Translation-en Ign http://archive precise/restricted Translation-en Ign http://archive precise/universe Translation-en Ign http://archive precise-updates/main Translation-en Ign http://archive precise-updates/multiverse Translation-en Ign http://archive precise-updates/restricted Translation-en Ign http://archive precise-updates/universe Translation-en Ign http://archive precise-security/main Translation-en Ign http://archive precise-security/multiverse Translation-en Ign http://archive precise-security/restricted Translation-en Ign http://archive precise-security/universe Translation-en Ign http://archive precise/main Translation-en Fetched 2164 kB in 0s (4156 kB/s) Reading package lists... "}

    def DebPkgGetControlInfo(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        package_name = methodParams.get("packageName", None)
        if not package_name:
            raise SolidFireApiError("DebPkgGetControlInfo", methodParams, nodeIP, endpoint, "xMissingParameter", 500, "Missing member=[packageName]")

        pieces = package_name.split("-")
        element = pieces[2]
        version = pieces[3]
        majorver = version.split(".")[0]
        with self.dataLock:
            self.data[UPGRADE_ELEMENT_PATH] = element

        return {"control": {
            "Architecture": "amd64",
            "Depends": "boto-sfdev-precise-2.27.0-p1 (>=1), solidfire-binaries-ELEMENT-VERSION (>=VERSION), solidfire-otp-ELEMENT-MAJOR.0.0.2 (>=MAJOR.0.0.2), solidfire-python-framework-ELEMENT-VERSION (>=VERSION), solidfire-rtfi-files-ELEMENT-VERSION (>=VERSION), solidfire-sfapt-ELEMENT-VERSION (>=VERSION), solidfire-sftop-ELEMENT-VERSION (>=VERSION), solidfire-snmpd-config-ELEMENT-VERSION (>=VERSION)".replace("ELEMENT", element).replace("VERSION", version).replace("MAJOR", majorver),
            "Description": "SolidFire Elements MAJOR [ELEMENT] High Performance Storage Area Network".replace("ELEMENT", element).replace("MAJOR", majorver),
            "Filename": "pool/main/s/solidfire-san-ELEMENT-VERSION/solidfire-san-ELEMENT-VERSION_VERSION_amd64.deb".replace("ELEMENT", element).replace("VERSION", version),
            "Homepage": "solidfire.com",
            "MD5sum": "ce61880c03fa548dc922a78d0f1117a4",
            "Maintainer": "SolidFire Engineering (Release Team) <support@solidfire.com>",
            "Package": "solidfire-san-ELEMENT-VERSION".replace("ELEMENT", element).replace("VERSION", version),
            "Pre-Depends": "bashutils-sfdev-precise-1.1.14 (>=0), gconf2-common (>=3.2.5-0ubuntu2), openjdk-7-jre (>=7u79-2.5.6-0ubuntu1.12.04.1)",
            "Priority": "optional",
            "SHA1": "a2c30c9257eb7bd38bbd7d06028bcb0af16ea3d0",
            "SHA256": "1075271e35345c3f5f44f86750dff2f4f037a2d475abdbfa4eb0bb7f81146d8f",
            "Section": "main",
            "Size": "1846",
            "Version": version,
            "XB-MinSfVersion": "8.2.0.183",
            "XB-RepoConfigTable": "deb,amd64,,solidfire,precise main|deb,amd64,,omsa/repo,/|deb,amd64,ELEMENT-updates,/ubuntu,precise main restricted multiverse universe|deb,amd64,ELEMENT-updates,/ubuntu,precise-updates main restricted multiverse universe|deb,amd64,ELEMENT-updates,/security-ubuntu,precise-security main restricted multiverse universe|".replace("ELEMENT", element),
            "XB-SfServices": "sfconfig solidfire sfnetwd tty1",
            "XB-SfSupportedClusterApiVersions": ",".join(['"{}"'.format(api) for api in globalconfig.all_api_versions])
            }
        }

    def GetStartupFlags(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            nodes = self.data[ACTIVE_NODES_PATH]
            node_id = 0
            for node in nodes.itervalues():
                if node["mip"] == nodeIP:
                    node_id = node["nodeID"]
                    break
            if not node_id:
                SolidFireApiError("GetStartupFlags", methodParams, nodeIP, endpoint, "x", 500, "Could not find nodeID {}.".format(node_id))

            flags = self.data[STARTUP_FLAGS_PATH][node_id]
            return { "startupFlags" : copy.deepcopy(flags) }

    def ListMountedFileSystems(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {"mountEntries":[{"available":0,"capacity":0,"devPath":"sysfs","deviceId":14,"directory":"/sys","free":0,"path":"sysfs","pathLink":"sysfs","type":"sysfs"},{"available":0,"capacity":0,"devPath":"proc","deviceId":3,"directory":"/proc","free":0,"path":"proc","pathLink":"proc","type":"proc"},{"available":37906796544,"capacity":37906800640,"devPath":"udev","deviceId":5,"directory":"/dev","free":37906796544,"path":"udev","pathLink":"udev","type":"devtmpfs"},{"available":0,"capacity":0,"devPath":"devpts","deviceId":11,"directory":"/dev/pts","free":0,"path":"devpts","pathLink":"devpts","type":"devpts"},{"available":7583002624,"capacity":7583338496,"devPath":"tmpfs","deviceId":15,"directory":"/run","free":7583002624,"path":"tmpfs","pathLink":"tmpfs","type":"tmpfs"},{"available":67365261312,"capacity":78408368128,"devPath":"/dev/disk/by-uuid/a3866341-eca8-483c-bfd6-f00f181443b4","deviceId":2050,"directory":"/","free":71355039744,"path":"/dev/sda2","pathLink":"/dev/disk/by-uuid/a3866341-eca8-483c-bfd6-f00f181443b4","type":"ext4"},{"available":202967040,"capacity":245738496,"devPath":"/dev/sda1","deviceId":2049,"directory":"/boot","free":215654400,"path":"/dev/sda1","pathLink":"/dev/sda1","type":"ext2"},{"available":18164899840,"capacity":19600896000,"devPath":"/dev/sda3","deviceId":2051,"directory":"/var/log","free":19167387648,"path":"/dev/sda3","pathLink":"/dev/sda3","type":"ext4"},{"available":67365261312,"capacity":78408368128,"devPath":"/dev/disk/by-uuid/a3866341-eca8-483c-bfd6-f00f181443b4","deviceId":2050,"directory":"/proc/cmdline","free":71355039744,"path":"/dev/sda2","pathLink":"/dev/disk/by-uuid/a3866341-eca8-483c-bfd6-f00f181443b4","type":"ext4"},{"available":0,"capacity":0,"devPath":"none","deviceId":16,"directory":"/sys/fs/fuse/connections","free":0,"path":"none","pathLink":"none","type":"fusectl"},{"available":0,"capacity":0,"devPath":"none","deviceId":6,"directory":"/sys/kernel/debug","free":0,"path":"none","pathLink":"none","type":"debugfs"},{"available":0,"capacity":0,"devPath":"none","deviceId":10,"directory":"/sys/kernel/security","free":0,"path":"none","pathLink":"none","type":"securityfs"},{"available":5242880,"capacity":5242880,"devPath":"none","deviceId":17,"directory":"/run/lock","free":5242880,"path":"none","pathLink":"none","type":"tmpfs"},{"available":37916676096,"capacity":37916692480,"devPath":"none","deviceId":18,"directory":"/run/shm","free":37916676096,"path":"none","pathLink":"none","type":"tmpfs"},{"available":7864270848,"capacity":8312512512,"devPath":"/dev/nvme0n1","deviceId":64000,"directory":"/mnt/pendingDirtyBlocks","free":8293347328,"path":"/dev/nvme0n1","pathLink":"/dev/nvme0n1","type":"ext4"},{"available":0,"capacity":0,"devPath":"none","deviceId":19,"directory":"/sys/kernel/config","free":0,"path":"none","pathLink":"none","type":"configfs"}]}

    def AptPurgeOldKernels(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def GetAptSourceLinesFromSystem(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            element = self.data[UPGRADE_ELEMENT_PATH]
            return {"lines": [
                "deb [arch=amd64] http://archive/ELEMENT-updates/ubuntu/ precise main restricted universe multiverse".replace("ELEMENT", element),
                "deb [arch=amd64] http://archive/ELEMENT-updates/ubuntu/ precise-updates main restricted universe multiverse".replace("ELEMENT", element),
                "deb [arch=amd64] http://archive/ELEMENT-updates/security-ubuntu/ precise-security main restricted universe multiverse".replace("ELEMENT", element),
                "deb [arch=amd64] http://archive/solidfire/ precise main",
                "deb [arch=amd64] http://archive/omsa/repo/ /"
            ]}

    def GetVersionInfo(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            node_id = None
            for node in self.data[ACTIVE_NODES_PATH].values():
                if node["mip"] == nodeIP:
                    node_id = node["nodeID"]
                    break
            for node in self.data[PENDING_NODES_PATH].values():
                if node["mip"] == nodeIP:
                    node_id = node["pendingNodeID"]
                    break
            if not node_id:
                active = [node["mip"] for node in self.data[ACTIVE_NODES_PATH].values()]
                pending = [node["mip"] for node in self.data[PENDING_NODES_PATH].values()]
                raise Exception("Unknown node {}\nActive nodes {}\nPending nodes {}".format(nodeIP, active, pending))
            node_ver = self.data[NODE_VERSION_PATH][node_id]
            major_ver = SolidFireVersion(node_ver).major

            ver_info = {}
            for binary in ["sfapp", "sfbasiciocheck", "sfconfig", "sfnetwd", "sfsvcmgr"]:
                ver_info[binary] = {
                    "BuildDate" : TimestampToStr(time.time(), formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone()),
                    "BuildType" : "Release",
                    "Element" : "unobtanium",
                    "Release" : "unobtanium",
                    "ReleaseShort" : "unobtanium",
                    "Repository" : "unobtanium",
                    "Revision" : "5885b1b76ad6",
                    "md5" : "ae8b05976b0c028365fc456001959878",
                    "sfdev" : "{}.{}".format(major_ver, random.randint(30, 60)),
                    "Version" : node_ver
                }

            return {"versionInfo" : ver_info}

    def AptSearch(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        search_term = methodParams.get("searchTerm", None)

        if search_term == "solidfire-san-":
            with self.dataLock:
                for node in self.data[ACTIVE_NODES_PATH].values():
                    if node["mip"] == nodeIP:
                        node_id = node["nodeID"]
                        break

                package_list = []
                for pkg_name in self.data[INSTALLED_PACKAGES_PATH][node_id]:
                    pkg_ver = pkg_name.split("-")[-1]
                    package_list.append({
                        "action" : "",
                        "automatic" : "false",
                        "candidateVersion" : pkg_ver,
                        "currentState" : "i",
                        "currentVersion" : pkg_ver,
                        "packageName" : pkg_name,
                        "reverseDependsCount" : 0,
                        "trusted" : True
                    })
                for pkg_name in self.data[AVAILABLE_PACKAGES_PATH]:
                    if pkg_name in self.data[INSTALLED_PACKAGES_PATH][node_id]:
                        continue
                    pkg_ver = pkg_name.split("-")[-1]
                    package_list.append({
                        "action" : "",
                        "automatic" : "false",
                        "candidateVersion" : pkg_ver,
                        "currentState" : "p",
                        "currentVersion" : "<none>",
                        "packageName" : pkg_name,
                        "reverseDependsCount" : 0,
                        "trusted" : True
                    })

                return { "packages" : copy.deepcopy(package_list) }

        else:
            raise NotImplementedError("searchTerm={} has not been implemented".format(search_term))

    def InvokeSetVersion(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            for node in self.data[ACTIVE_NODES_PATH].values():
                if node["mip"] == nodeIP:
                    node_id = node["nodeID"]
                    break
            self.data[NODE_VERSION_PATH][node_id] = self.data[VERSION_INFO_PATH]["pendingVersion"]
        return {}

    def GetSystemStatus(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return { "rebootRequired" : random.choice([True, False]) }

    def RebootNode(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def RestartServices(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        return {}

    def InstallCleanup(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        with self.dataLock:
            for node in self.data[ACTIVE_NODES_PATH].values():
                if node["mip"] == nodeIP:
                    node_id = node["nodeID"]
                    break
            # Remove any package that is not the current version
            node_version = self.data[NODE_VERSION_PATH][node_id]
            removed = "\n".join([pkg for pkg in self.data[INSTALLED_PACKAGES_PATH][node_id] if node_version not in pkg])
            self.data[INSTALLED_PACKAGES_PATH][node_id] = [pkg for pkg in self.data[INSTALLED_PACKAGES_PATH][node_id] if node_version in pkg]

            return { "packages" : removed }

    def ReadLink(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        path = methodParams.get("path", None)
        if not path:
            raise SolidFireApiError("ReadLink", methodParams, nodeIP, endpoint, "xMissingParameter", 500, "Missing member=[path]")

        with self.dataLock:
            if path == "/sf/alternatives/@sf@bin@sfapp":
                return { "absolutePath": "/sf/packages/solidfire-binaries-unobtanium-{}/bin/sfapp".format(self.data[CLUSTER_VERSION_PATH]) }

            else:
                raise NotImplementedError("path=[{}] has not been implemented".format(path))

    def GetTime(self, methodParams, nodeIP, endpoint="", apiVersion=""):
        timestamp = time.time()
        times = {
            "hardware" : TimestampToStr(timestamp, formatString="%c 0.%f seconds", timeZone=UTCTimezone()),
            "local" : TimestampToStr(timestamp, formatString="%Y-%m-%dT%H:%M:%Sl", timeZone=UTCTimezone()),
            "utc" : TimestampToStr(timestamp, formatString="%Y-%m-%dT%H:%M:%SZ", timeZone=UTCTimezone())
        }

        with self.dataLock:
            if SolidFireVersion(self.data[CLUSTER_VERSION_PATH]).major >= 7:
                return { "time" : times}
            else:
                return times

