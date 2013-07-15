#!/usr/bin/env python

"""
Global storage for all actions to share
"""

# Using a multiprocessing dict allows any process/thread to update this and still be visible across all processes
# Unless someone does something strange, this module is implicitly imported long before any forks, so this will always be instantiated and accessable across processes.
import platform
if "windows" in platform.system().lower():
    __storage = dict()
    __lock = None
else:
    import multiprocessing
    __manager = multiprocessing.Manager()
    __storage = __manager.dict()
    __lock = multiprocessing.Lock()

def Dump():
    for key in sorted(__storage.keys()):
        print key + " => " + str(__storage[key])

def Lock():
    if "windows" in platform.system().lower():
        return
    __lock.acquire()

def Unlock():
    if "windows" in platform.system().lower():
        return
    __lock.release()

def Set(keyName, value):
    __storage[str(keyName)] = value

def Get(keyName):
    return __storage.get(str(keyName))

def Del(keyName):
    if keyName in __storage:
        del __storage[keyName]

class SharedValues:
    """
    List of defined shared values that actions use
    """
    accountName = "accountName"
    activeNodeList = "activeNodeList"
    blockServiceIDList = "blockServiceIDList"
    clientDhcpEnabled = "clientDHCPEnabled"
    clientHostname = "clientHostname"
    clientIQN = "clientIQN"
    clientOS = "clientOS"
    clusterMasterIP = "clusterMasterIP"
    clusterMasterID = "clusterMasterID"
    ensembleLeaderIP = "ensembleLeaderIP"
    ipmiIP = "ipmiIP"
    lastGCInfo = "lastGCInfo"
    nodeDriveCount = "nodeDriveCount"
    nodeIP = "nodeIP"
    pendingNodeList = "pendingNodeList"
    sliceServiceIDList = "sliceServiceIDList"
    volumeIQN = "volumeIQN"
    volumeList = "volumeList"
    volumeIDList = "volumeIDList"


