#!/usr/bin/env python
"""
SolidFire volume access group object and related data structures
"""
import libsf
from libsf import mylog
import libsfcluster

class SFVolGroup(object):
    def __init__(self, volgroup, mvip, username, password):
        """
        Create a new SFVolGroup object.  This object is only meant to be constructed by the appropriate factory function in SFCluster
        
        Args:
            volgroup: the volumeAccessGroup dictionary returned from the cluster
            mvip: the managment VIP of the cluster
            username: the admin usernameof the cluster
            password: the admin password of the cluster
        """
        for key, value in volgroup.items():
            setattr(self, key, value)
        self.ID = volgroup["volumeAccessGroupID"]
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
            #mylog.debug("Refreshing volgroup object")
            self = libsfcluster.SFCluster(self.mvip, self.username, self.password).FindVolumeAccessGroup(volgroupID=self.ID)
            fn(self, *args, **kwargs)
        return wrapper

    def Delete(self):
        """
        Delete this group
        """

        params = {}
        params["volumeAccessGroupID"] = self.ID
        libsf.CallApiMethod(self.mvip, self.username, self.password, "DeleteVolumeAccessGroup", params, ApiVersion=5.0)

    @__refresh
    def AddInitiators(self, initiatorList):
        """
        Add a list of initiators to the group
        
        Args:
            initiatorList: a list of initiator IQNs (strings) to add
        """
        # Append the IQNs to the existing list
        full_iqn_list = self.initiators
        for iqn in initiatorList:
            if iqn.lower() in full_iqn_list:
                mylog.debug(iqn + " is already in group " + self.name)
            else:
                full_iqn_list.append(iqn)

        # Modify the VAG on the cluster
        params = {}
        params["volumeAccessGroupID"] = self.ID
        params["initiators"] = full_iqn_list
        libsf.CallApiMethod(self.mvip, self.username, self.password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    @__refresh
    def RemoveInitiators(self, initiatorList):
        """
        Remove a list of initiators from the group
        
        Args:
            initiatorList: a list of initiator IQNs (strings) to remove
        """
        # Append the IQNs to the existing list
        full_iqn_list = self.initiators
        for iqn in initiatorList:
            if iqn.lower() in full_iqn_list:
                full_iqn_list.remove(iqn)
            else:
                mylog.debug(iqn + " is already not in group " + self.name)

        # Modify the VAG on the cluster
        params = {}
        params["volumeAccessGroupID"] = self.ID
        params["initiators"] = full_iqn_list
        libsf.CallApiMethod(self.mvip, self.username, self.password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    @__refresh
    def AddVolumes(self, volumeIDList):
        """
        Add a list of volumes to the group
        
        Args:
            volumeIDList: a list of volume IDs (ints) to add
        """
        volume_ids = self.volumes
        for vol_id in volumeIDList:
            if vol_id not in volume_ids:
                volume_ids.append(vol_id)
            else:
                mylog.debug("volumeID " + str(vol_id) + " is already in group")

        # Add the requested volumes
        params = {}
        params["volumes"] = volume_ids
        params["volumeAccessGroupID"] = self.ID
        libsf.CallApiMethod(self.mvip, self.username, self.password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)

    @__refresh
    def RemoveVolumes(self, volumeIDList):
        """
        Remove a list of volumes from the group
        
        Args:
            volumeIDList: a list of volume IDs (ints) to remove
        """
        volume_ids = self.volumes
        for vol_id in volumeIDList:
            if vol_id in volume_ids:
                volume_ids.remove(vol_id)
            else:
                mylog.debug("volumeID " + str(vol_id) + " is already not in group")

        # Remove the requested volumes
        params = {}
        params["volumes"] = volume_ids
        params["volumeAccessGroupID"] = self.ID
        libsf.CallApiMethod(self.mvip, self.username, self.password, "ModifyVolumeAccessGroup", params, ApiVersion=5.0)


