#!/usr/bin/env python
"""
SolidFire cluster pair object and related data structures
"""
import libsf
from libsf import mylog
import libsfcluster

class SFClusterPair(object):
    def __init__(self, pair, mvip, username, password):
        """
        Create a new SFClusterPair object.  This object is only meant to be constructed by the appropriate factory function in SFCluster

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

    def __str__(self):
        return "[" + str(self.ID) + "] " + self.UUID + " " + self.clusterName

    def __refresh(fn):
        """
        Decorator to refresh the attributes of this object from the cluster
        """
        def wrapper(self, *args, **kwargs):
            self = libsfcluster.SFCluster(self.localMVIP, self.username, self.password).FindClusterPair
            fn(self, *args, **kwargs)
        return wrapper
