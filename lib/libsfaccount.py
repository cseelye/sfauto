#!/usr/bin/env python
"""
SolidFire acount object and related data structures
"""
import libsf
from libsf import mylog
import libsfcluster

class SFAccount(object):
    def __init__(self, account, mvip, username, password):
        """
        Create a new SFAccount object.  This object is only meant to be constructed by the appropriate factory function in SFCluster

        Args:
            account: the account dictionary returned from the cluster
            mvip: the managment VIP of the cluster
            username: the admin usernameof the cluster
            password: the admin password of the cluster
        """
        for key, value in account.items():
            setattr(self, key, value)
        self.ID = account["accountID"]
        self.mvip = mvip
        self.username = username
        self.password = password

    def __str__(self):
        return "[" + str(self.ID) + "] " + self.name

    def __refresh(fn):
        """
        Decorator to refresh the attributes of this object from the cluster
        """
        def wrapper(self, *args, **kwargs):
            #mylog.debug("Refreshing account object")
            self = libsfcluster.SFCluster(self.mvip, self.username, self.password).FindAccount(accountID=self.ID)
            fn(self, *args, **kwargs)
        return wrapper
