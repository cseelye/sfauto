#!/usr/bin/env python
"""
This module contains the operations for the StressRandom action
"""
import abc
import copy
import inspect
import multiprocessing
import random
import string
import time
from functools import wraps
import lib.libsf as libsf
from lib.libsf import mylog

import list_active_nodes
import reboot_node
import wait_for_cluster_healthy

# ---------------------------------------------------------------------------------------------------------------------
# Random number generator/helpers

rnd = None  # Module random generator - normally initialized by whoever imports this class

def init_rnd(func):
    ''' Decorator to initialize the random number generator if it hasn't been already'''
    @wraps(func)
    def inner(*args, **kwargs):
        global rnd
        if rnd == None:
            myseed = int(time.time() * 1000)
            rnd = random.Random(myseed)
            mylog.info("Initializing operations module random generator with seed " + str(myseed))
        return func(*args, **kwargs)
    return inner

@init_rnd
def randomName(length):
    return randomString(length, includeWhitespace=False, includePunctuation=False, extraChars='-')

@init_rnd
def randomString(length, includeWhitespace=True, includePunctuation=True, extraChars=None):
    possible_chars = string.digits + string.letters
    if includeWhitespace:
        possible_chars += ' ' # only include space - tab, newline, etc don't make sense in this context
    if includePunctuation:
        possible_chars += string.punctuation
    if extraChars:
        possible_chars += extraChars

    return ''.join(rnd.choice(possible_chars) for i in xrange(length))

@init_rnd
def randomIQN():
    # Starts with 'iqn.'
    iqn = "iqn."
    # Four numbers
    iqn += ''.join(rnd.choice(string.digits) for i in xrange(4))
    # A dash
    iqn += "-"
    # Two numbers
    iqn += ''.join(rnd.choice(string.digits) for i in xrange(2))
    # A dot
    iqn += "."
    # Lowercase letters, digits, dot, colon, dash
    iqn += ''.join(rnd.choice(string.digits + string.lowercase + ".:-") for i in xrange(rnd.randint(10,50)))

    return iqn

@init_rnd
def randomAttributes():
    attrs = {}
    attr_count = rnd.randint(0, 10)
    if attr_count > 0:
        for i in range(1, attr_count):
            attrs[randomName(rnd.randint(1,64))] = randomString(rnd.randint(1,64))
    return attrs

# ---------------------------------------------------------------------------------------------------------------------
# Decorators to mark operations as primary/secondary
def primary_action(klass):
    ''' Decorator to mark a class as a "primary" operation '''
    klass.operation = "primary"
    return klass

def secondary_operation(klass):
    ''' Decorator to mark a class as a "secondary" operation '''
    klass.operation = "secondary"
    return klass

class Operations(object):
    CREATE = 'CREATE'
    DELETE = 'DELETE'
    PURGE = 'PURGE'
    MODIFY = 'MODIFY'
    ADD = 'ADD'
    REMOVE = 'REMOVE'
    ADDCHILD = 'ADDCHILD'
    REMOVECHILD = 'REMOVECHILD'
    KILL = 'KILL'
    REBOOT = 'REBOOT'
    POWEROFF = 'POWEROFF'


class LoggingLock(object):
    def __init__(self, name):
        self.lock = multiprocessing.Lock()
        if name:
            self.name = name
        else:
            self.name = "unnamed"
        self.__enter__ = self.lock.__enter__
        self.__exit__ = self.lock.__exit__

    def acquire(self):
        mylog.debug("Locking " + self.name + " from " + inspect.stack()[1][3])
        self.lock.acquire()
    def release(self):
        mylog.debug("Unlocking " + self.name + " from " + inspect.stack()[1][3])
        self.lock.release()

class ClusterModel(object):
    def __init__(self, mvip, username, password):
        self.mvip = mvip
        self.username = username
        self.password = password
        self.svip = ""
        self.name = ""
        self.nextFakeObjectID = -1

        self.nodesToModify = []
        self.volumesToModify = []
        self.accountsToModify = []

        self.clusterLimits = {}
        self.activeNodeList = []
        self.pendingNodeList = []
        self.volumeList = []
        self.delVolumeList = []
        self.accountList = []

    def UpdateClusterConfig(self):
        '''
        Go out to the cluster and get the latest
        '''
        # Cluster limits
        tempLimits = libsf.CallApiMethod(self.mvip, self.username, self.password, "GetLimits", {})

        # Active nodes
        node_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListAllNodes", {})
        tempActiveNodeList = []
        for node in node_list["nodes"]:
            tempActiveNodeList.append(node)
        tempPendingNodeList = []
        for node in node_list["pendingNodes"]:
            tempPendingNodeList.append(node)

        # Volumes
        volume_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListActiveVolumes", {})
        tempVolumeList = volume_list["volumes"]

        # Accounts
        account_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListAccounts", {})
        tempAccountList = []
        for account in account_list["accounts"]:
            if account["status"] != "removed":
                tempAccountList.append(account)

        # Deleted volumes
        volume_list = libsf.CallApiMethod(self.mvip, self.username, self.password, "ListDeletedVolumes", {})
        tempDelVolumeList = volume_list["volumes"]

        # Add the deleted volumes to their respective accounts
        for vol in volume_list["volumes"]:
            account = [a for a in tempAccountList if a["accountID"] == vol["accountID"]][0]
            account["volumes"].append(vol["volumeID"])

        # Cluster capacity
        capacity = libsf.CallApiMethod(self.mvip, self.username, self.password, "GetClusterCapacity", {})
        tempCapacity = capacity["clusterCapacity"]

        self.clusterLimits = tempLimits
        self.activeNodeList = tempActiveNodeList
        self.pendingNodeList = tempPendingNodeList
        self.volumeList = tempVolumeList
        self.delVolumeList = tempDelVolumeList
        self.accountList = tempAccountList
        self.capacity = tempCapacity

    def GetFakeObjectID(self):
        vid = self.nextFakeObjectID
        self.nextFakeObjectID -= 1
        return vid



# ---------------------------------------------------------------------------------------------------------------------
# Operations

class StressOperation(object):
    ''' Base class for operations'''

    __metaclass__ = abc.ABCMeta

    #mpManager = multiprocessing.Manager()

    def __init__(self, debug=False):
        self.lock = multiprocessing.Lock()
        self.threaderror = multiprocessing.Value('i', 0)    # Simple return value for the worker - 0 for success, non-zero for failure
        self.started = multiprocessing.Value('i', 0)        # Flag to determine if the op has ever been started
        self.finished = multiprocessing.Value('i', 0)       # Flag to determine if the op has already run or not
        self.worker = multiprocessing.Process(target=self._OpThreadWrapper, name=self.Name() + "_worker")
        self.worker.daemon = True
        self.simulate = False
        self.showDebug = debug

    def Name(self):
        return self.__class__.__name__

    def debug(self, message):
        mylog.debug("  " + self.Name() + ": " + message)
    def info(self, message):
        mylog.info("  " + self.Name() + ": " + message)
    def warning(self, message):
        mylog.warning("  " + self.Name() + ": " + message)
    def error(self, message):
        mylog.error("  " + self.Name() + ": " + message)
    def passed(self, message):
        mylog.passed("  " + self.Name() + ": " + message)

    def Start(self):
        self.lock.acquire()
        try:
            if self.started.value != 0:
                raise Exception(self.Name() + " trying to start twice!")
            self.started.value = 1
            self.worker.start()
        finally:
            self.lock.release()

    def HasBeenStarted(self):
        if self.started.value == 0:
            return False
        else:
            return True

    def IsFinished(self):
        if self.finished.value == 0:
            return False
        else:
            return True

    def End(self):
        if self.worker == None:
            return
        self.worker.join()
        if self.threaderror.value != 0 or self.worker.exitcode != 0:
            self.error("Completed operation with errors")
            raise Exception("Error during " + self.Name() + " operation")
        self.passed("Successfully completed operation")

    def _OpThreadWrapper(self):
        if self.showDebug:
            mylog.showDebug()
        self.debug("My PID is " + str(multiprocessing.current_process().pid))
        try:
            self._OpThread()
        except KeyboardInterrupt:
            return
        except SystemExit:
            return
        except Exception as e:
            mylog.exception(self.Name() + ": Unhandled exception in op thread")
            self.threaderror.value = 1
        finally:
            self.finished.value = 1

    @abc.abstractmethod
    def Init(self, **kwargs): return
    @abc.abstractmethod
    def PrintOperation(self): return
    @abc.abstractmethod
    def _OpThread(self): return

@secondary_operation
class WaitAndDoNothingOp(StressOperation):
    '''
    Sleep for a random period of time
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.waitTime = rnd.randint(10,180)

    def PrintOperation(self):
        self.info("pending operation - Wait for " + str(self.waitTime) + " seconds")

    def _OpThread(self):
        self.info("Waiting for " + str(self.waitTime) + " seconds")
        time.sleep(self.waitTime)

@secondary_operation
class CreateVolumesOp(StressOperation):
    '''
    Create random volumes
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.volume_params = []
        self.volume_count = 0

        deleting_acount_ids = [a["accountID"] for a in self.cluster.accountsToModify if a["op"] == Operations.DELETE]
        possible_accounts = []
        for account in self.cluster.accountList:
            # Skip if this account is being deleted
            if account["accountID"] in deleting_acount_ids:
                continue
            possible_accounts.append(account)

        if self.cluster.capacity["provisionedSpace"] >= self.cluster.capacity["maxProvisionedSpace"] * 0.8:
            self.warning("Not creating any volumes because the cluster is already >= 80% provisioned")
            return

        if len(possible_accounts) <= 0:
            self.warning("Cannot create any volumes because there are no more accounts to use")
            return

        # Generate random volumes
        new_provisioned = 0
        target_volume_count = rnd.randint(50, 200)
        self.debug("Target volumes is " + str(target_volume_count))
        for i in xrange(target_volume_count):
            account = rnd.choice(possible_accounts)
            params = {}
            params["name"] = randomName(rnd.randint(1,64))
            params["accountID"] = account["accountID"]
            params["totalSize"] = rnd.randint(1, 500) * 1000 * 1000 * 1000
            params["enable512e"] = rnd.choice([True, False])
            qos = {}
            qos["maxIOPS"] = rnd.randint(1000, 100000)
            qos["burstIOPS"] = rnd.randint(qos["maxIOPS"], 100000)
            qos["minIOPS"] = rnd.randint(100, qos["maxIOPS"] if qos["maxIOPS"] <= 15000 else 15000)
            params["qos"] = qos
            params["attributes"] = randomAttributes()
            self.volume_params.append(params)
            self.volume_count += 1

            # Update the cluster model
            if len([a for a in self.cluster.accountsToModify if a["accountID"] == account["accountID"]]) <= 0:
                mod_account = copy.deepcopy(account)
                mod_account["op"] = Operations.ADDCHILD
                self.cluster.accountsToModify.append(mod_account)

            # Make sure we are not overfilling the cluster
            new_provisioned += params["totalSize"]
            if self.cluster.capacity["provisionedSpace"] + new_provisioned >= self.cluster.capacity["maxProvisionedSpace"] * 0.8:
                self.debug("Not creating any more volumes because the cluster will be >= 80% provisioned")
                break

    def End(self):
        super(self.__class__, self).End()
        # Update the cluster model
        for vol_params in self.volume_params:
            for account in reversed(self.cluster.accountsToModify):
                if account["accountID"] == vol_params["accountID"]:
                    self.cluster.accountsToModify.remove(account)
                    break

    def PrintOperation(self):
        self.info("pending operation - Create " + str(self.volume_count) + " volumes")

    def _OpThread(self):
        self.info("Creating " + str(self.volume_count) + " volumes")
        for vol_params in self.volume_params:
            self.debug("Creating volume " + vol_params["name"] + " in account " + str(vol_params["accountID"]))
            if self.simulate:
                time.sleep(0.1)
                continue
            try:
                libsf.CallApiMethod(self.cluster.mvip, self.cluster.username, self.cluster.password, "CreateVolume", vol_params)
            except libsf.SfError as e:
                # Ignore error from the cluster being too full to create another volume
                if e.name == 'xSliceServiceSelectionFailed':
                    self.debug("Not creating any more volumes because of xSliceServiceSelectionFailed")
                    break
                self.error("Failed to create volume " + vol_params["name"] + ": " + str(e))
                self.threaderror.value = 1
                break

@secondary_operation
class CreateAccountsOp(StressOperation):
    '''
    Create random accounts
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.account_params = []
        self.account_count = 0

        if len(self.cluster.accountList) == self.cluster.clusterLimits["accountCountMax"]:
            self.warning("Not creating accounts because there are already the limit on the cluster")
            return

        # Generate random accounts to create
        target_account_count = rnd.randint(5, 200)
        for i in xrange(target_account_count):
            params = {}

            # Generate a unique username
            username = randomName(rnd.randint(1,64))
            unique = False
            while not unique:
                unique = True
                for account in self.cluster.accountList:
                    if account["username"].lower() == username.lower():
                        self.debug("Duplicate username " + username)
                        unique = False
                for account in self.account_params:
                    if account["username"].lower() == username.lower():
                        self.debug("Duplicate username " + username)
                        unique = False
                if not unique:
                    username = randomName(rnd.randint(1,64))

            params["username"] = username
            params["initiatorSecret"] = randomString(rnd.randint(12,16))
            params["targetSecret"] = randomString(rnd.randint(12,16))
            params["attributes"] = randomAttributes()
            self.account_params.append(params)
            self.account_count += 1

            # Make sure we are not overfilling the cluster
            if len(self.cluster.accountList) >= self.cluster.clusterLimits["accountCountMax"]:
                break

    def PrintOperation(self):
        self.info("pending operation - Creating " + str(self.account_count) + " accounts")

    def _OpThread(self):
        self.info("Creating " + str(self.account_count) + " accounts")
        while True:
            try:
                params = self.account_params.pop()
            except IndexError:
                break
            self.debug("Creating account " + params["username"])
            if self.simulate:
                time.sleep(0.1)
                continue
            try:
                libsf.CallApiMethod(self.cluster.mvip, self.cluster.username, self.cluster.password, "AddAccount", params)
            except libsf.SfError as e:
                self.error("Failed to create account " + str(params["username"]) + ": " + str(e))
                self.threaderror.value = 1
                return

@secondary_operation
class DeleteAccountsOp(StressOperation):
    '''
    Delete a random number of empty accounts
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.account_params = []
        self.account_count = 0

        # Find all of the empty accounts that do not have some other operation pending on them
        pending_acount_ids = [a["accountID"] for a in self.cluster.accountsToModify]
        possible_accounts = []
        for account in self.cluster.accountList:
            if len(account["volumes"]) <= 0 and account["accountID"] not in pending_acount_ids:
                possible_accounts.append(account)
        if len(possible_accounts) <= 0:
            self.warning("Cannot delete any accounts because there are no more accounts to use")
            return

        # Delete between 1 account and 10% of the empty accounts
        if len(possible_accounts) <= 10:
            self.account_count = 1
        else:
            self.account_count = max(1, rnd.randint(1, int(len(possible_accounts) * 0.1)))

        # Select accounts to delete
        accounts_to_del = rnd.sample(possible_accounts, self.account_count)
        for del_account in accounts_to_del:
            params = {}
            params["accountID"] = del_account["accountID"]
            self.account_params.append(params)

            # Update cluster model
            account = copy.deepcopy(del_account)
            account["op"] = Operations.DELETE
            self.cluster.accountsToModify.append(account)

    def End(self):
        super(self.__class__, self).End()
        # Update the cluster model
        for params in self.account_params:
            for account in reversed(self.cluster.accountsToModify):
                if account["accountID"] == params["accountID"]:
                    self.cluster.accountsToModify.remove(account)
                    break

    def PrintOperation(self):
        self.info("pending operation - Deleting " + str(self.account_count) + " accounts")

    def _OpThread(self):
        self.info("Deleting " + str(self.account_count) + " accounts")
        for params in self.account_params:
            self.debug("Deleting account " + str(params["accountID"]))
            if self.simulate:
                time.sleep(0.1)
                continue
            try:
                libsf.CallApiMethod(self.cluster.mvip, self.cluster.username, self.cluster.password, "RemoveAccount", params)
            except libsf.SfError as e:
                self.error("Failed to delete account " + str(params["accountID"]) + " : " + str(e))
                self.threaderror.value = 1
                return

@secondary_operation
class DeleteAndPurgeVolumesOp(StressOperation):
    '''
    Delete and purge a random number of volumes
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.volume_params = []
        self.volume_count = 0
        self.purge_params = []
        self.purge_count = 0
        self.account_ids = []

        pending_volume_ids = [a["volumeID"] for a in self.cluster.volumesToModify]
        possible_volumes = []
        for volume in self.cluster.volumeList:
            # Skip volumes that are having something else done to them
            if volume["volumeID"] in pending_volume_ids:
                continue
            # Skip already deleted volumes
            if volume["status"] == "deleted":
                continue
            possible_volumes.append(volume)
        if len(possible_volumes) <= 0:
            self.warning("Cannot delete any volumes because there are no more volumes to use")
            return

        # Delete between 1 volume and 10% of the active volumes
        if len(possible_volumes) <= 10:
            self.volume_count = 1
        else:
            self.volume_count = max(1, rnd.randint(1, int(len(possible_volumes) * 0.1)))

        vols_to_del = rnd.sample(possible_volumes, self.volume_count)

        for del_volume in vols_to_del:
            params = {"volumeID" : del_volume["volumeID"]}
            self.volume_params.append(params)
            self.account_ids.append(del_volume["accountID"])

            # Update cluster model
            if len([a for a in self.cluster.accountsToModify if a["accountID"] == del_volume["accountID"]]) <= 0:
                account = [a for a in self.cluster.accountList if del_volume["volumeID"] in a["volumes"]][0]
                a = copy.deepcopy(a)
                a["op"] = Operations.REMOVECHILD
                self.cluster.accountsToModify.append(a)

            # Randomly choose to purge or not
            if rnd.choice([True, False]):
                self.purge_params.append(params)
                self.purge_count += 1
                # Update cluster model
                v = copy.deepcopy(params)
                v["op"] = Operations.PURGE
                self.cluster.volumesToModify.append(v)
            else:
                # Update cluster model
                v = copy.deepcopy(params)
                v["op"] = Operations.DELETE
                self.cluster.volumesToModify.append(v)

    def End(self):
        super(self.__class__, self).End()
        # Update the cluster model
        for account in reversed(self.cluster.accountsToModify):
            if account["accountID"] in self.account_ids:
                self.cluster.accountsToModify.remove(account)

    def PrintOperation(self):
        self.info("pending operation - Deleting " + str(self.volume_count) + " volumes and purging " +  str(self.purge_count) + " of them")

    def _OpThread(self):
        self.info("Deleting " + str(self.volume_count) + " volumes")
        for params in self.volume_params:
            #mylog.info("Delete list is length " + str(len(self.volume_params)))
            self.debug("Deleting volume " + str(params["volumeID"]))
            if self.simulate:
                time.sleep(0.1)
                continue
            try:
                libsf.CallApiMethod(self.cluster.mvip, self.cluster.username, self.cluster.password, "DeleteVolume", params)
            except libsf.SfError as e:
                self.error("Failed to delete volume " + str(params["volumeID"]) + ": " + str(e))
                self.threaderror.value = 1
                return

        self.info("Purging " + str(self.purge_count) + " volumes")
        for params in self.purge_params:
            #mylog.info("Purge list is length " + str(len(self.purge_params)))
            self.debug("Purging volume " + str(params["volumeID"]))
            if self.simulate:
                time.sleep(0.1)
                continue
            try:
                libsf.CallApiMethod(self.cluster.mvip, self.cluster.username, self.cluster.password, "PurgeDeletedVolume", params)
            except libsf.SfError as e:
                self.error("Failed to purge volume " + str(params["volumeID"]) + ": " + str(e))
                self.threaderror.value = 1
                return

@primary_action
class SoftNodeRebootOp(StressOperation):
    '''
    Gracefully reboot a random node in the cluster
    '''
    @init_rnd
    def Init(self, **kwargs):
        self.cluster = kwargs["cluster"]
        self.node = None

        pending_op_nodes = [n["nodeID"] for n in self.cluster.nodesToModify if n["op"] in [Operations.KILL, Operations.POWEROFF, Operations.REBOOT]]
        if len(pending_op_nodes) > 0:
            self.warning("Not rebooting a node because another node is being perturbed")
            return

        pending_op_nodes = [n["nodeID"] for n in self.cluster.nodesToModify if n["op"] in [Operations.ADD, Operations.REMOVE]]

        possible_nodes = []
        for node in self.cluster.activeNodeList:
            # Skip nodes being added/removed from the cluster
            if node["nodeID"] in pending_op_nodes:
                continue
            possible_nodes.append(node)

        self.node = rnd.choice(possible_nodes)
        self.debug("Selected " + self.node["mip"])

        # Update the cluster model
        n = copy.deepcopy(self.node)
        n["op"] = Operations.REBOOT
        self.cluster.nodesToModify.append(n)

    def PrintOperation(self):
        if self.node:
            self.info("pending operation - Reboot " + self.node["mip"])

    def End(self):
        super(self.__class__, self).End()
        if not self.node:
            return
        # Update the cluster model
        for node in reversed(self.cluster.nodesToModify):
            if node["mip"] == self.node["mip"]:
                self.cluster.nodesToModify.remove(node)

    def _OpThread(self):
        if not self.node:
            return
        if self.simulate:
            self.info("Rebooting " + self.node["mip"])
            time.sleep(180)
            self.info("Finished rebooting " + self.node["mip"])
            return
        if not reboot_node.Execute(node_ip=self.node["mip"], waitForUp=True):
            self.threaderror.value = 1
            return
        if not wait_for_cluster_healthy.Execute(mvip=self.cluster.mvip, username=self.cluster.username, password=self.cluster.password):
            self.threaderror.value = 1
            return


