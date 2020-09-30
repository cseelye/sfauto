#!/usr/bin/env python
"""
SolidFire cluster pair object and related data structures
"""
from .logutil import GetLogger
from . import SolidFireClusterAPI, InvalidArgumentError, UnknownObjectError

def _refresh(fn):
    """Decorator to refresh the attributes of this object from the cluster"""
    def wrapper(self, *args, **kwargs):
        self.Refresh()
        fn(self, *args, **kwargs)
        self.Refresh()
    return wrapper

class SFClusterPair(object):
    """Common interactions with a SolidFire cluster pair"""

    @staticmethod
    def Find(mvip, username, password, clusterPairID=0, clusterPairUUID=None, remoteClusterMVIP=None, remoteClusterName=None):
        """
        Find the cluster pair with the given ID, UUID or cluster name

        Args:
            mvip:               the management VIP of the cluster (string)
            username:           the admin user of the cluster (string)
            password:           the admin password of the cluster (string)
            clusterPairID:      the ID of the cluster pair to find
            clusterPairUUID:    the UUID of the cluster pair to find
            remoteClusterName:  the name of paired cluster to find

        Returns:
            An SFClusterPair object
        """
        if clusterPairID <=0 and not clusterPairUUID and not remoteClusterMVIP and not remoteClusterName:
            raise InvalidArgumentError("Please specify either clusterPairID, clusterPairUUID, remoteClusterMVIP or remoteClusterName")

        api = SolidFireClusterAPI(mvip,
                                  username,
                                  password,
                                  maxRetryCount=5,
                                  retrySleep=20,
                                  errorLogThreshold=1,
                                  errorLogRepeat=1)

        result = api.CallWithRetry("ListClusterPairs", {}, apiVersion=6.0)

        if clusterPairID > 0:
            for pair in result["clusterPairs"]:
                if pair["clusterPairID"] == clusterPairID:
                    return SFClusterPair(pair, mvip, username, password)
                raise UnknownObjectError("Could not find pair on {} with ID {}".format(mvip, clusterPairID))
        elif clusterPairUUID:
            for pair in result["clusterPairs"]:
                if pair["clusterPairUUID"] == clusterPairUUID:
                    return SFClusterPair(pair, mvip, username, password)
                raise UnknownObjectError("Could not find pair on {} with UUID {}".format(mvip, clusterPairUUID))
        elif remoteClusterMVIP:
            for pair in result["clusterPairs"]:
                if pair["mvip"] == remoteClusterMVIP:
                    return SFClusterPair(pair, mvip, username, password)
            raise UnknownObjectError("Could not find pair on {} with remote cluster MVIP {}".format(mvip, remoteClusterMVIP))
        elif remoteClusterName:
            for pair in result["clusterPairs"]:
                if pair["clusterName"] == remoteClusterName:
                    return SFClusterPair(pair, mvip, username, password)
            raise UnknownObjectError("Could not find pair on {} with cluster name {}".format(mvip, remoteClusterName))

    def __init__(self, pair, mvip, username, password):
        """
        Create a new SFClusterPair object.  This object is typically meant to be constructed by the appropriate factory function in SFCluster

        Args:
            pair: the pair dictionary returned from the cluster
            mvip: the managment VIP of the cluster
            username: the admin usernameof the cluster
            password: the admin password of the cluster
        """
        for key, value in pair.items():
            setattr(self, key, value)
        self.ID = pair["clusterPairID"]
        self.UUID = pair["clusterPairUUID"]
        self.localMVIP = mvip
        self.username = username
        self.password = password
        self.log = GetLogger()
        self.api = SolidFireClusterAPI(self.localMVIP,
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
        self.api = SolidFireClusterAPI(self.localMVIP,
                                       self.username,
                                       self.password,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)
        for key in self._unpicklable:
            assert hasattr(self, key)

    def __str__(self):
        return "[{}] {} {}".format(getattr(self, "ID", 0), getattr(self, "UUID", "0-0-0-0"), getattr(self, "clusterName", "unknown"))

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
        group = SFClusterPair.Find(self.localMVIP, self.username, self.password, clusterPairUUID=self.UUID)
        #pylint: disable=attribute-defined-outside-init
        self.__dict__ = group.__dict__.copy()
        #pylint: enable=attribute-defined-outside-init

