#!/usr/bin/env python2.7
#pylint: skip-file

from __future__ import print_function
import pytest
import random
from libsf import SolidFireAPIError
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .testutil import RandomString, RandomIP

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeCreate(object):

    def test_negative_VolumeCreateNoAccount(self):
        print()
        from volume_create import VolumeCreate
        assert not VolumeCreate(volume_size=random.randint(1, 8000),
                             volume_name=RandomString(random.randint(1, 64)),
                             volume_count=1,
                             account_id=9999)

    def test_negative_VolumeCreateAccountSearchFailure(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        with APIFailure("ListAccounts"):
            assert not VolumeCreate(volume_size=random.randint(1, 8000),
                                 volume_name=RandomString(random.randint(1, 64)),
                                 volume_count=1,
                                 account_id=existing_id)

    def test_negative_VolumeCreateSingleFailure(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        with APIFailure("CreateVolume"):
            assert not VolumeCreate(volume_size=random.randint(1, 8000),
                                 volume_name=RandomString(random.randint(1, 64)),
                                 volume_count=1,
                                 account_id=existing_id)

    def test_negative_VolumeCreateFailure(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        with APIFailure("CreateMultipleVolumes"):
            assert not VolumeCreate(volume_size=random.randint(1, 8000),
                                 volume_prefix=RandomString(random.randint(1, 50)),
                                 volume_count=random.randint(2, 20),
                                 account_id=existing_id)

    def test_VolumeCreateSingle(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        assert VolumeCreate(volume_size=random.randint(1, 8000),
                             volume_name=RandomString(random.randint(1, 64)),
                             volume_count=1,
                             account_id=existing_id)

    def test_VolumeCreateSingleExplicit(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        assert VolumeCreate(volume_size=random.randint(1, 8000),
                             volume_prefix=RandomString(random.randint(1, 64)),
                             volume_count=1,
                             create_single=True,
                             account_id=existing_id)

    def test_VolumeCreateGiBExplicit(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        assert VolumeCreate(volume_size=random.randint(1, 7400),
                             volume_prefix=RandomString(random.randint(1, 64)),
                             volume_count=random.randint(2, 10),
                             gib=True,
                             account_id=existing_id)

    def test_VolumeCreateWaitExplicit(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from volume_create import VolumeCreate
        assert VolumeCreate(volume_size=random.randint(1, 7400),
                             volume_prefix=RandomString(random.randint(1, 64)),
                             volume_count=2,
                             create_single=True,
                             wait=1,
                             account_id=existing_id)

    def test_VolumeCreateWithAllOptions(self):
        print()
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        max_iops = random.randint(5000, 90000)
        burst_iops = max_iops + random.randint(1, 10000)
        from volume_create import VolumeCreate
        assert VolumeCreate(volume_size=random.randint(1, 7400),
                             volume_prefix=RandomString(random.randint(1, 50)) + "-",
                             volume_count=random.randint(1, 10),
                             volume_start=random.randint(1, 100),
                             min_iops=random.randint(100, 1000),
                             max_iops=max_iops,
                             burst_iops=burst_iops,
                             enable512e=random.choice([True, False]),
                             gib=random.choice([True, False]),
                             create_single=random.choice([True, False]),
                             wait=random.randint(0, 1),
                             account_id=existing_id)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeDelete(object):

    def test_VolumeDeleteNoMatches(self):
        print()
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_prefix="nomatchingvolumes")

    def test_negative_VolumeDeleteNoArgs(self):
        print()
        from volume_delete import VolumeDelete
        assert not VolumeDelete()

    def test_VolumeDeleteTestMode(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_ids=random.sample(volume_ids, random.randint(2, 15)),
                             test=True)

    def test_negative_VolumeDeleteFailure(self):
        print()
        volume_names = [vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        with APIFailure("DeleteVolumes"):
            assert not VolumeDelete(volume_names=random.sample(volume_names, random.randint(2, 15)))

    def test_negative_VolumeDeleteFailurePreFluorine(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        with APIVersion(8.0):
            with APIFailure("DeleteVolume"):
                assert not VolumeDelete(volume_ids=random.sample(volume_ids, random.randint(2, 15)))

    def test_negative_VolumeDeleteSearchFailure(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        with APIFailure("ListActiveVolumes"):
            assert not VolumeDelete(volume_ids=random.sample(volume_ids, random.randint(2, 15)))

    def test_DeleteSingleVolume(self):
        print()
        volume_names = [vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_names=volume_names[random.randint(0, len(volume_names)-1)],
                             purge=True)

    def test_VolumeDeleteNoPurge(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_ids=random.sample(volume_ids, random.randint(2, 15)),
                             purge=False)

    def test_VolumeDelete(self):
        print()
        volume_names = [vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_names=random.sample(volume_names, random.randint(2, 15)),
                             purge=random.choice([True, False]))

    def test_VolumeDeletePreFluorine(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        from volume_delete import VolumeDelete
        with APIVersion(8.0):
            assert VolumeDelete(volume_ids=random.sample(volume_ids, random.randint(2, 15)),
                             purge=random.choice([True, False]))

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestPurgeVolumes(object):

    def test_negative_VolumePurgeSearchFailure(self):
        print()
        from volume_purge import VolumePurge
        with APIFailure("ListDeletedVolumes"):
            assert not VolumePurge()

    def test_negative_VolumePurgeFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_ids=volume_ids,
                             purge=False)
        from volume_purge import VolumePurge
        with APIFailure("PurgeDeletedVolumes"):
            assert not VolumePurge()

    def test_negative_VolumePurgeFailurePreFluorine(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_ids=volume_ids,
                             purge=False)
        from volume_purge import VolumePurge
        with APIVersion(8.0):
            with APIFailure("PurgeDeletedVolume"):
                assert not VolumePurge()

    def test_VolumePurge(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_delete import VolumeDelete
        assert VolumeDelete(volume_ids=volume_ids,
                             purge=False)
        from volume_purge import VolumePurge
        assert VolumePurge()

    def test_VolumePurgeNoVolumes(self):
        print()
        from volume_purge import VolumePurge
        assert VolumePurge()
        assert VolumePurge()

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeExtend(object):

    def test_VolumeExtendNoMatch(self):
        print()
        from volume_extend import VolumeExtend
        assert not VolumeExtend(new_size=random.randint(2, 8000),
                                 volume_names=["nomatch", "doesntexist","invalid"])

    def test_VolumeExtendTestMode(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_extend import VolumeExtend
        assert VolumeExtend(new_size=random.randint(2, 8000),
                             volume_ids=volume_ids,
                             test=True)

    def test_VolumeExtend(self):
        print()
        volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] < 200 * 1000 * 1000 * 1000]
        volume_ids = random.sample(volumes, random.randint(2, min(15, len(volumes))))
        from volume_extend import VolumeExtend
        assert VolumeExtend(new_size=random.randint(200, 8000),
                             volume_ids=volume_ids)

    def test_negative_VolumeExtendSmaller(self):
        print()
        volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] > 1 * 1000 * 1000 * 1000]
        volume_ids = random.sample(volumes, random.randint(2, min(15, len(volumes))))
        from volume_extend import VolumeExtend
        assert not VolumeExtend(new_size=1,
                             volume_ids=volume_ids)

    def test_VolumeExtendGiB(self):
        print()
        volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] < 150 * 1024 * 1024 * 1024]
        volume_ids = random.sample(volumes, random.randint(2, min(15, len(volumes))))
        from volume_extend import VolumeExtend
        assert VolumeExtend(new_size=random.randint(200, 7400),
                             gib=True,
                             volume_ids=volume_ids)

    def test_negative_VolumeExtendFailure(self):
        print()
        volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] < 150 * 1000 * 1000 * 1000]
        if len(volumes) < 3:
            print("small volumes = {}".format(volumes))
            print("all volumes = {}".format(globalconfig.cluster.ListActiveVolumes({})["volumes"]))
        volume_ids = random.sample(volumes, random.randint(2, min(15, len(volumes))))
        from volume_extend import VolumeExtend
        with APIFailure("ModifyVolume"):
            assert not VolumeExtend(new_size=random.randint(200, 8000),
                                     volume_ids=volume_ids)

    def test_negative_VolumeExtendSearchFailure(self):
        print()
        volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] < 150 * 1000 * 1000 * 1000]
        volume_ids = random.sample(volumes, random.randint(2, min(15, len(volumes))))
        from volume_extend import VolumeExtend
        with APIFailure("ListActiveVolumes"):
            assert not VolumeExtend(new_size=random.randint(200, 8000),
                                     volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeSetQos(object):

    def test_VolumeSetQos(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_qos import VolumeSetQos
        assert VolumeSetQos(volume_ids=volume_ids,
                            min_iops=random.randint(50, 1000),
                            max_iops=random.randint(1500, 90000),
                            burst_iops=random.randint(91000,100000))

    def test_VolumeSetQosTestMode(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_qos import VolumeSetQos
        assert VolumeSetQos(volume_ids=volume_ids,
                            min_iops=random.randint(50, 1000),
                            max_iops=random.randint(1500, 90000),
                            burst_iops=random.randint(91000,100000))

    def test_VolumeSetQosNoMatch(self):
        print()
        from volume_set_qos import VolumeSetQos
        assert not VolumeSetQos(volume_names=["nomatch", "doesntexist","invalid"],
                                min_iops=random.randint(50, 1000),
                                max_iops=random.randint(1500, 90000),
                                burst_iops=random.randint(91000,100000))

    def test_negative_SetVolumeQoSFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_qos import VolumeSetQos
        with APIFailure("ModifyVolume"):
            assert not VolumeSetQos(volume_ids=volume_ids,
                                    min_iops=random.randint(50, 1000),
                                    max_iops=random.randint(1500, 90000),
                                    burst_iops=random.randint(91000,100000))

    def test_negative_SetVolumeQoSSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_qos import VolumeSetQos
        with APIFailure("ListActiveVolumes"):
            assert not VolumeSetQos(volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeLock(object):

    def test_VolumeLock(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_lock import VolumeLock
        assert VolumeLock(volume_ids=volume_ids)

    def test_VolumeLockTestMode(self):
        print()
        volume_names = random.sample([vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_lock import VolumeLock
        assert VolumeLock(volume_names=volume_names,
                             test=True)

    def test_VolumeLockNoMatch(self):
        print()
        from volume_lock import VolumeLock
        assert not VolumeLock(volume_names=["nomatch", "doesntexist","invalid"])

    def test_negative_VolumeLockFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_lock import VolumeLock
        with APIFailure("ModifyVolume"):
            assert not VolumeLock(volume_ids=volume_ids)

    def test_negative_VolumeLockSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_lock import VolumeLock
        with APIFailure("ListActiveVolumes"):
            assert not VolumeLock(volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeUnlock(object):

    def test_VolumeUnlock(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_unlock import VolumeUnlock
        assert VolumeUnlock(volume_ids=volume_ids)

    def test_VolumeUnlockTestMode(self):
        print()
        volume_names = random.sample([vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_unlock import VolumeUnlock
        assert VolumeUnlock(volume_names=volume_names,
                             test=True)

    def test_VolumeUnlockNoMatch(self):
        print()
        from volume_unlock import VolumeUnlock
        assert not VolumeUnlock(volume_names=["nomatch", "doesntexist","invalid"])

    def test_negative_VolumeUnlockFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_unlock import VolumeUnlock
        with APIFailure("ModifyVolume"):
            assert not VolumeUnlock(volume_ids=volume_ids)

    def test_negative_VolumeUnlockSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_unlock import VolumeUnlock
        with APIFailure("ListActiveVolumes"):
            assert not VolumeUnlock(volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeSetAttributes(object):

    def test_VolumeSetAttribute(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_attribute import VolumeSetAttribute
        assert VolumeSetAttribute(volume_ids=volume_ids,
                                   attribute_name=RandomString(32),
                                   attribute_value=RandomString(64))

    def test_VolumeSetAttributeTestMode(self):
        print()
        volume_names = random.sample([vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_attribute import VolumeSetAttribute
        assert VolumeSetAttribute(volume_names=volume_names,
                                   attribute_name=RandomString(32),
                                   attribute_value=RandomString(64),
                                   test=True)

    def test_VolumeSetAttributeNoMatch(self):
        print()
        from volume_set_attribute import VolumeSetAttribute
        assert VolumeSetAttribute(volume_names=["nomatch", "doesntexist","invalid"],
                                   attribute_name=RandomString(32),
                                   attribute_value=RandomString(64))

    def test_VolumeSetAttributeFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_attribute import VolumeSetAttribute
        with APIFailure("ModifyVolume"):
            assert not VolumeSetAttribute(volume_ids=volume_ids,
                                       attribute_name=RandomString(32),
                                       attribute_value=RandomString(64))

    def test_VolumeSetAttributeSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volume_set_attribute import VolumeSetAttribute
        with APIFailure("ListActiveVolumes"):
            assert not VolumeSetAttribute(volume_ids=volume_ids,
                                       attribute_name=RandomString(32),
                                       attribute_value=RandomString(64))

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestGetVolumeIQN(object):

    def test_negative_GetVolumeIQNNoVolumeName(self):
        print()
        from volume_get_iqn import GetVolumeIQN
        assert not GetVolumeIQN(volume_name=RandomString(random.randint(8, 64)))

    def test_negative_GetVolumeIQNNoVolumeID(self):
        print()
        volume_ids = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]]
        while True:
            non_id = random.randint(1, 20000)
            if non_id not in volume_ids:
                break

        from volume_get_iqn import GetVolumeIQN
        assert not GetVolumeIQN(volume_id=non_id)

    def test_negative_GetVolumeIQNSearchFailure(self):
        print()
        from volume_get_iqn import GetVolumeIQN
        with APIFailure("ListActiveVolumes"):
            assert not GetVolumeIQN(volume_name=RandomString(random.randint(1, 64)))

    def test_GetVolumeIQN(self):
        print()
        volume_name = random.choice([vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]])
        from volume_get_iqn import GetVolumeIQN
        assert GetVolumeIQN(volume_name=volume_name)

    def test_GetVolumeIQNBash(self, capsys):
        print()
        volume = random.choice(globalconfig.cluster.ListActiveVolumes({})["volumes"])
        from volume_get_iqn import GetVolumeIQN
        assert GetVolumeIQN(volume_id=volume["volumeID"], output_format="bash")
        out, _ = capsys.readouterr()
        out = out.strip()
        print(out)
        assert len(out.split("\n")) == 1
        assert out.startswith("iqn")
        assert out.endswith("{}.{}".format(volume["name"], volume["volumeID"]))

    def test_GetVolumeIQNJson(self, capsys):
        print()
        volume = random.choice(globalconfig.cluster.ListActiveVolumes({})["volumes"])
        from volume_get_iqn import GetVolumeIQN
        assert GetVolumeIQN(volume_name=volume["name"], output_format="json")
        out, _ = capsys.readouterr()
        out = out.strip()
        print(out)
        assert len(out.split("\n")) == 1
        import json
        iqn = json.loads(out)["iqn"]
        assert iqn.startswith("iqn")
        assert iqn.endswith("{}.{}".format(volume["name"], volume["volumeID"]))

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestVolumeClone(object):

    def test_negative_VolumeCloneLimitsFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        with APIFailure("GetLimits"):
            assert not VolumeClone(clone_count=random.randint(2, 5),
                                    volume_ids=volume_ids)

    def test_negative_VolumeCloneNewAccountNoAccount(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        assert not VolumeClone(clone_count=random.randint(2, 5),
                                volume_ids=volume_ids,
                                dest_account_id=9999)

    def test_negative_VolumeCloneNewAccountSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        with APIFailure("ListAccounts"):
            assert not VolumeClone(clone_count=random.randint(2, 5),
                                    volume_ids=volume_ids,
                                    dest_account_id=2)

    def test_negative_VolumeCloneVolumeSearchFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        with APIFailure("ListActiveVolumes"):
            assert not VolumeClone(clone_count=random.randint(2, 5),
                                    volume_ids=volume_ids)

    def test_VolumeCloneTest(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        assert VolumeClone(clone_count=random.randint(2, 5),
                            volume_ids=volume_ids,
                            test=True)

    def test_negative_VolumeCloneFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        with APIFailure("CloneVolume"):
            assert not VolumeClone(clone_count=random.randint(2, 5),
                                    volume_ids=volume_ids)

    def test_VolumeClone(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        from volume_clone import VolumeClone
        assert VolumeClone(clone_count=random.randint(2, 5),
                            volume_ids=volume_ids)

    def test_VolumeCloneNewAccount(self):
        print()
        print("all volumes = {}".format(globalconfig.cluster.ListActiveVolumes({})["volumes"]))
        print("all accounts = {}".format(globalconfig.cluster.ListAccounts({})["accounts"]))
        new_account = random.choice([account["accountID"] for account in globalconfig.cluster.ListAccounts({})["accounts"]])
        source_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["accountID"] != new_account]
        print("new_account = {}".format(new_account))
        print("source_volumes = {}".format(source_volumes))
        volume_ids = random.sample(source_volumes, random.randint(1, min(5, len(source_volumes))))
        from volume_clone import VolumeClone
        assert VolumeClone(clone_count=random.randint(2, 5),
                            volume_ids=volume_ids,
                            dest_account_id=new_account)

    def test_VolumeCloneNewSize(self):
        print()
        clone_size = random.randint(800, 2000)
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["totalSize"] < clone_size*1000*1000*1000], random.randint(2, 5))
        from volume_clone import VolumeClone
        assert VolumeClone(clone_count=random.randint(2, 5),
                            volume_ids=volume_ids,
                            clone_size=clone_size)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestRemoteRepPauseVolume(object):

    def test_negative_RemoteRepPauseVolumeSearchFailure(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_pause_volume import RemoteRepPauseVolume
        with APIFailure("ListActiveVolumes"):
            assert not RemoteRepPauseVolume(volume_ids=volume_ids)

    def test_RemoteRepPauseVolumeNoVolumes(self):
        print()
        from remoterep_pause_volume import RemoteRepPauseVolume
        assert RemoteRepPauseVolume(volume_prefix="nomatch")

    def test_RemoteRepPauseVolumeTestMode(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_pause_volume import RemoteRepPauseVolume
        assert RemoteRepPauseVolume(volume_ids=volume_ids,
                                      test=True)

    def test_negative_RemoteRepPauseVolumeFailure(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_pause_volume import RemoteRepPauseVolume
        with APIFailure("ModifyVolumePair"):
            assert not RemoteRepPauseVolume(volume_ids=volume_ids)

    def test_RemoteRepPauseVolume(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_pause_volume import RemoteRepPauseVolume
        assert RemoteRepPauseVolume(volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestRemoteRepResumeVolume(object):

    def test_negative_RemoteRepResumeVolumeSearchFailure(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_resume_volume import RemoteRepResumeVolume
        with APIFailure("ListActiveVolumes"):
            assert not RemoteRepResumeVolume(volume_ids=volume_ids)

    def test_RemoteRepResumeVolumeNoVolumes(self):
        print()
        from remoterep_resume_volume import RemoteRepResumeVolume
        assert RemoteRepResumeVolume(volume_prefix="nomatch")

    def test_RemoteRepResumeVolumeTestMode(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_resume_volume import RemoteRepResumeVolume
        assert RemoteRepResumeVolume(volume_ids=volume_ids,
                                       test=True)

    def test_negative_RemoteRepResumeVolumeFailure(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_resume_volume import RemoteRepResumeVolume
        with APIFailure("ModifyVolumePair"):
            assert not RemoteRepResumeVolume(volume_ids=volume_ids)

    def test_RemoteRepResumeVolume(self):
        print()
        paired_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if "volumePairs" in vol and vol["volumePairs"]]
        volume_ids = random.sample(paired_volumes, random.randint(2, min(15, len(paired_volumes)-1)))
        from remoterep_resume_volume import RemoteRepResumeVolume
        assert RemoteRepResumeVolume(volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestForceWoleSync(object):

    def test_VolumeForceWholeSyncWait(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        assert VolumeForceWholeSync(volume_ids=volume_ids,
                              wait=True)

    def test_VolumeForceWholeSyncNoWait(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        assert VolumeForceWholeSync(volume_ids=volume_ids,
                              wait=False)

    def test_negative_NoVolumes(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        assert not VolumeForceWholeSync(volume_ids=999999)

    def test_negative_NoMatch(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        assert VolumeForceWholeSync(volume_regex="nomatch")

    def test_TestMode(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        assert VolumeForceWholeSync(volume_ids=volume_ids,
                              test=True)

    def test_negative_APIFailure(self):
        print()
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 5))
        
        from volume_force_whole_sync import VolumeForceWholeSync
        with APIFailure("ForceWholeFileSync"):
            assert not VolumeForceWholeSync(volume_ids=volume_ids)

