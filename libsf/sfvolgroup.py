#!/usr/bin/env python2.7
"""
SolidFire volume access group object and related data structures
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

class SFVolGroup(object):
    """Common interactions with a SolidFire volume group"""

    @staticmethod
    def Create(mvip, username, password, volgroupName, iqns=None, volumeIDs=None):
        """
        Create a volume access group

        Args:
            mvip:           the management VIP of the cluster (string)
            username:       the admin user of the cluster (string)
            password:       the admin password of the cluster (string)
            volgroupName: the name of the group (string)
            iqns:         the list of initiator IQNs to add to the group (list of strings)
            volumeIDs:    the list of volume IDs to add to the group (list of ints)

        Returns:
            An SFVolGroup object
        """
        api = SolidFireClusterAPI(mvip,
                                  username,
                                  password,
                                  maxRetryCount=5,
                                  retrySleep=20,
                                  errorLogThreshold=1,
                                  errorLogRepeat=1)
        version = GetHighestAPIVersion(mvip, username, password)

        params = {}
        params["name"] = volgroupName
        if iqns:
            params["initiators"] = iqns
        if volumeIDs:
            params["volumes"] = volumeIDs
        result = api.CallWithRetry("CreateVolumeAccessGroup", params, apiVersion=version)
        return SFVolGroup.Find(mvip=mvip,
                               username=username,
                               password=password,
                               volgroupID=result["volumeAccessGroupID"])

    @staticmethod
    def Find(mvip, username, password, volgroupName=None, volgroupID=None):
        """
        Find a volume access group with the given name or ID.  If there are duplicate names, the first match is returned.

        Args:
            mvip:           the management VIP of the cluster (string)
            username:       the admin user of the cluster (string)
            password:       the admin password of the cluster (string)
            volgroupName:   the name of the group to find (string)
            volgroupID:     the ID of the group to find (int)

        Returns:
            An SFVolGroup object
        """
        if not volgroupName and not volgroupID:
            raise InvalidArgumentError("Please specify either volgroupName or volgroupID")

        api = SolidFireClusterAPI(mvip,
                                  username,
                                  password,
                                  maxRetryCount=5,
                                  retrySleep=20,
                                  errorLogThreshold=1,
                                  errorLogRepeat=1)
        version = GetHighestAPIVersion(mvip, username, password)

        vag_list = api.CallWithRetry("ListVolumeAccessGroups", {}, apiVersion=version)
        if volgroupName:
            for vag in vag_list["volumeAccessGroups"]:
                if vag["name"].lower() == volgroupName.lower():
                    return SFVolGroup(vag, mvip, username, password)
            raise UnknownObjectError("Could not find group with name {}".format(volgroupName))

        else:
            try:
                volgroupID = int(volgroupID)
            except ValueError:
                raise InvalidArgumentError("Please specify an integer for volgroupID")
            if volgroupID <= 0:
                raise InvalidArgumentError("Please specify a positive non-zero integer for volgroupID")
            for vag in vag_list["volumeAccessGroups"]:
                if vag["volumeAccessGroupID"] == volgroupID:
                    return SFVolGroup(vag, mvip, username, password, version)
            raise UnknownObjectError("Could not find group with ID {}".format(volgroupID))

    def __init__(self, volgroup, mvip, username, password, apiVersion=9.0):
        """
        Create a new SFVolGroup object.  This object is typically  meant to be constructed by the appropriate factory function in SFCluster
        
        Args:
            volgroup:   the volumeAccessGroup dictionary returned from the cluster
            mvip:       the managment VIP of the cluster
            username:   the admin usernameof the cluster
            password:   the admin password of the cluster
            apiVersion: the API version on the cluster to use
        """
        for key, value in volgroup.items():
            setattr(self, key, value)

        self.ID = volgroup["volumeAccessGroupID"]
        self.mvip = mvip
        self.username = username
        self.password = password
        self.apiVersion = apiVersion
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

    def __str__(self):
        return "[{}] {}".format(self.ID, getattr(self, "name", "unknown"))

    def __repr__(self):
        return "SFVolGroup({})".format(self)

    def __getitem__(self, key):
        return getattr(self, key, None)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __contains__(self, key):
        if getattr(self, key, False):
            return True
        return False

    def Refresh(self):
        """Refresh the state of this object with information for the cluster"""
        group = SFVolGroup.Find(self.mvip, self.username, self.password, volgroupID=self.ID)
        #pylint: disable=attribute-defined-outside-init
        self.__dict__ = group.__dict__.copy()
        #pylint: enable=attribute-defined-outside-init

    def Delete(self):
        """
        Delete this group
        """
        params = {}
        params["volumeAccessGroupID"] = self.ID
        self.api.CallWithRetry("DeleteVolumeAccessGroup", params, apiVersion=self.apiVersion)

    @_refresh
    def AddInitiators(self, initiatorList):
        """
        Add a list of initiators to the group
        
        Args:
            initiatorList: a list of initiator IQNs (strings) to add
        """
        # Append the IQNs to the existing list
        full_iqn_list = self.initiators
        for iqn in initiatorList:
            if any([iqn.lower() == init.lower() for init in full_iqn_list]):
                self.log.debug("{} is already in group {}".format(iqn, self.name))
            else:
                full_iqn_list.append(iqn)

        # Modify the VAG on the cluster
        params = {}
        params["volumeAccessGroupID"] = self.ID
        params["initiators"] = full_iqn_list
        self.api.CallWithRetry("ModifyVolumeAccessGroup", params, apiVersion=self.apiVersion)

    @_refresh
    def RemoveInitiators(self, initiatorList):
        """
        Remove a list of initiators from the group
        
        Args:
            initiatorList: a list of initiator IQNs (strings) to remove
        """
        # Append the IQNs to the existing list
        full_iqn_list = self.initiators
        for iqn in initiatorList:
            if any([iqn.lower() == init.lower() for init in full_iqn_list]):
                full_iqn_list.remove(iqn)
            else:
                self.log.debug("{} is already not in group {}".format(iqn, self.name))

        # Modify the VAG on the cluster
        params = {}
        params["volumeAccessGroupID"] = self.ID
        params["initiators"] = full_iqn_list
        self.api.CallWithRetry("ModifyVolumeAccessGroup", params, apiVersion=self.apiVersion)

    @_refresh
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
                self.log.debug("volumeID {} is already in group {}".format(vol_id, self.name))

        # Add the requested volumes
        params = {}
        params["volumes"] = volume_ids
        params["volumeAccessGroupID"] = self.ID
        self.api.CallWithRetry("ModifyVolumeAccessGroup", params, apiVersion=self.apiVersion)

    @_refresh
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
                self.log.debug("volumeID {} is already not in group {}".format(vol_id, self.name))

        # Remove the requested volumes
        params = {}
        params["volumes"] = volume_ids
        params["volumeAccessGroupID"] = self.ID
        self.api.CallWithRetry("ModifyVolumeAccessGroup", params, apiVersion=self.apiVersion)

    def ModifyLUNAssignments(self, newLUNAssignments):
        """
        Change the LUN assignments of the volumes in this group
        
        Args:
            newLUNAssignments:  the list of LUN assignments (list of dict)
                                {"volumeID": 1, "lun": 13}
        """
        params = {}
        params["volumeAccessGroupID"] = self.ID
        params["lunAssignments"] = newLUNAssignments
        self.api.CallWithRetry("ModifyVolumeAccessGroupLunAssignments", params, apiVersion=self.apiVersion)
