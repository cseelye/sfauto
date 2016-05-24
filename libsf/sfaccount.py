#!/usr/bin/env python2.7
"""
SolidFire account object and related data structures
"""
from . import SolidFireClusterAPI, GetHighestAPIVersion, InvalidArgumentError, UnknownObjectError
from .logutil import GetLogger

def _refresh(fn):
    """Decorator to refresh the attributes of this object from the cluster"""
    def wrapper(self, *args, **kwargs):
        self.Refresh()
        fn(self, *args, **kwargs)
        self.Refresh()
    return wrapper

class SFAccount(object):
    """Common interactions with a SolidFire account"""

    @staticmethod
    def Create(mvip, username, password, accountName, initiatorSecret=None, targetSecret=None):
        """
        Create an account

        Args:
            mvip:               the management VIP of the cluster (string)
            username:           the admin user of the cluster (string)
            password:           the admin password of the cluster (string)
            account_name:       the name for the new account
            initiator_secret:   the initiator CHAP secret
            target secret:      the target CHAP secret

        Returns:
            An SFAccount object
        """
        api = SolidFireClusterAPI(mvip,
                                  username,
                                  password,
                                  maxRetryCount=5,
                                  retrySleep=20,
                                  errorLogThreshold=1,
                                  errorLogRepeat=1)

        params = {}
        params["username"] = accountName
        if initiatorSecret:
            params["initiatorSecret"] = initiatorSecret
        if targetSecret:
            params["targetSecret"] = targetSecret
        result = api.CallWithRetry("AddAccount", params)
        return SFAccount.Find(mvip, username, password, accountID=result["accountID"])

    @staticmethod
    def Find(mvip, username, password, accountName=None, accountID=None):
        """
        Find an account with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            mvip:           the management VIP of the cluster (string)
            username:       the admin user of the cluster (string)
            password:       the admin password of the cluster (string)
            accountName: the name of the account to find (string)
            accountID: the ID of the account to find (int)

        Returns:
            An SFAccount object
        """
        if not accountName and not accountID:
            raise InvalidArgumentError("Please specify either acountName or accountID")

        api = SolidFireClusterAPI(mvip,
                                  username,
                                  password,
                                  maxRetryCount=5,
                                  retrySleep=20,
                                  errorLogThreshold=1,
                                  errorLogRepeat=1)

        account_list = api.CallWithRetry("ListAccounts", {})
        if accountName:
            for account in account_list["accounts"]:
                if (account["username"].lower() == str(accountName).lower()):
                    return SFAccount(account, mvip, username, password)
            raise UnknownObjectError("Could not find account with name {}".format(accountName))

        else:
            try:
                accountID = int(accountID)
            except ValueError:
                raise InvalidArgumentError("Please specify an integer for accountID")
            if accountID <= 0:
                raise InvalidArgumentError("Please specify a positive non-zero integer for accountID")
            for account in account_list["accounts"]:
                if account["accountID"] == accountID:
                    return SFAccount(account, mvip, username, password)
            raise UnknownObjectError("Could not find account with ID {}".format(accountID))

    @staticmethod
    def CreateCHAPSecret():
        import random
        import string
        return "".join(random.choice(string.ascii_letters + string.digits) for x in range(14))

    def __init__(self, account, mvip, username, password):
        """
        Create a new SFAccount object.  This object is typically meant to be constructed by the appropriate factory function in SFCluster

        Args:
            account:    the account dictionary returned from the cluster
            mvip:       the managment VIP of the cluster
            username:   the admin username of the cluster
            password:   the admin password of the cluster
        """
        for key, value in account.items():
            setattr(self, key, value)
        self.ID = account["accountID"]
        self.mvip = mvip
        self.clusterUsername = username
        self.clusterPassword = password
        self.log = GetLogger()
        self.api = SolidFireClusterAPI(self.mvip,
                                       self.clusterUsername,
                                       self.clusterPassword,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)
        self._unpicklable = ["log", "api"]

    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.iteritems():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()
        self.api = SolidFireClusterAPI(self.mvip,
                                       self.clusterUsername,
                                       self.clusterPassword,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)
        for key in self._unpicklable:
            assert hasattr(self, key)

    def __str__(self):
        return "[{}] {}".format(self.ID, getattr(self, "name", "unknown"))

    def __getitem__(self, key):
        return getattr(self, key, None)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __contains__(self, key):
        if getattr(self, key, False):
            return True
        return False

    def Refresh(self):
        """Refresh the state of this object with information from the cluster"""
        group = SFAccount.Find(self.mvip, self.clusterUsername, self.clusterPassword, accountID=self.ID)
        #pylint: disable=attribute-defined-outside-init
        self.__dict__ = group.__dict__.copy()
        #pylint: enable=attribute-defined-outside-init

    def PurgeDeletedVolumes(self):
        """
        Purge the deleted volumes in this account
        """
        params = {}
        params["accountID"] = self.ID
        result = self.api.CallWithRetry("ListVolumesForAccount", params)
        deleted_volumes = [vol["volumeID"] for vol in result["volumes"] if vol["status"] == "deleted"]

        if len(deleted_volumes) <= 0:
            self.log.debug("No deleted volumes to purge from account {}".format(self.username))
            return

        self.log.debug("Purging {} deleted volumes from account {}".format(len(deleted_volumes), self.username))

        ver = GetHighestAPIVersion(self.mvip, self.clusterUsername, self.clusterPassword)
        if ver >= 9.0:
            params = {}
            params["volumeIDs"] = deleted_volumes
            self.api.CallWithRetry("PurgeDeletedVolumes", params, apiVersion=ver)
        else:
            for vol_id in deleted_volumes:
                params = {}
                params["volumeID"] = vol_id
                self.api.CallWithRetry("PurgeDeletedVolume", params)

    def Delete(self, purgeDeletedVolumes=True):
        """
        Delete this account from the cluster. After deletion this object becomes invalid
        
        Args:
            purgeDeletedVolumes:    purge this account's deleted volumes before deleting
        """
        if purgeDeletedVolumes:
            self.PurgeDeletedVolumes()

        params = {}
        params["accountID"] = self.ID
        self.api.CallWithRetry("RemoveAccount", params)
