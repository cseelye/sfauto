#!/usr/bin/env python2.7
#pylint: skip-file

import pytest
import random
from libsf import SolidFireAPIError, InvalidArgumentError
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .testutil import RandomString, RandomIP

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestDriveVerifyCount(object):

    def test_negative_DriveVerifyCountFailure(self):
        state=random.choice(["active", "available", "removing", "failed"])
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == state])
        print
        from drive_verify_count import DriveVerifyCount
        with APIFailure("ListDrives"):
            assert not DriveVerifyCount(expected=expected,
                                   compare="eq",
                                   state=state)

    def test_negative_DriveVerifyCountBadState(self):
        print
        from drive_verify_count import DriveVerifyCount
        with pytest.raises(InvalidArgumentError):
            DriveVerifyCount(expected=1,
                        compare="eq",
                        state="asdf")

    def test_negative_DriveVerifyCountNe(self):
        print
        from drive_verify_count import DriveVerifyCount
        assert not DriveVerifyCount(expected=1,
                               compare="eq",
                               state=random.choice(["active", "available", "removing", "failed"]))

    def test_DriveVerifyCountEq(self):
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "available"])
        print
        from drive_verify_count import DriveVerifyCount
        assert DriveVerifyCount(expected=expected,
                           compare="eq",
                           state="available")

    def test_DriveVerifyCountLt(self):
        state=random.choice(["active", "available", "removing", "failed"])
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == state])
        print
        from drive_verify_count import DriveVerifyCount
        assert DriveVerifyCount(expected=expected + 1,
                           compare="lt",
                           state=state)

    def test_DriveVerifyCountGt(self):
        print
        from drive_verify_count import DriveVerifyCount
        assert DriveVerifyCount(expected=1,
                           compare="gt",
                           state=random.choice(["active", "available"]))

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestDriveAdd(object):

    def test_negative_DriveAddNodeSearchFailure(self):
        print
        from drive_add import DriveAdd
        with APIFailure("ListActiveNodes"):
            assert not DriveAdd()

    def test_negative_DriveAddDriveSearchFailure(self):
        print
        from drive_add import DriveAdd
        with APIFailure("ListDrives"):
            assert not DriveAdd()

    def test_negative_DriveAddNoNode(self):
        print
        from drive_add import DriveAdd
        assert not DriveAdd(node_ips=[RandomIP()])

    def test_negative_DriveAddFailure(self):
        print
        drive_slots = None if random.choice([True, False]) else random.sample(range(-1, 10), random.randint(1, 10))
        from drive_add import DriveAdd
        with APIFailure("AddDrives"):
            assert not DriveAdd(by_node=random.choice([True, False]),
                                 wait_for_sync=random.choice([True, False]),
                                 drive_slots=drive_slots)

    def test_DriveAddNoneAvailable(self):
        print
        from drive_add import DriveAdd
        assert DriveAdd()
        assert DriveAdd()

    def test_DriveAddByNode(self):
        print
        from drive_add import DriveAdd
        assert DriveAdd(by_node=True,
                         wait_for_sync=random.choice([True, False]))

    def test_DriveAddByNodeAndSlot(self):
        print
        from drive_add import DriveAdd
        assert DriveAdd(by_node=True,
                         drive_slots=random.sample(range(-1,10), random.randint(1, 6)),
                         wait_for_sync=random.choice([True, False]))

    def test_DriveAddAllNodes(self):
        print
        from drive_add import DriveAdd
        assert DriveAdd(wait_for_sync=random.choice([True, False]))

    def test_DriveAddAllNodesAndSlot(self):
        print
        from drive_add import DriveAdd
        assert DriveAdd(drive_slots=random.sample(range(-1,10), random.randint(1, 10)),
                         wait_for_sync=random.choice([True, False]))

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestDriveRemove(object):

    def test_negative_DriveRemoveNodeSearchFailure(self):
        print
        from drive_remove import DriveRemove
        with APIFailure("ListActiveNodes"):
            assert not DriveRemove(node_ips=[RandomIP()])

    def test_negative_DriveRemoveDriveSearchFailure(self):
        print
        active_ip = globalconfig.cluster.ListActiveNodes({})["nodes"][0]["mip"]
        from drive_remove import DriveRemove
        with APIFailure("ListDrives"):
            assert not DriveRemove(node_ips=[active_ip])

    def test_negative_DriveRemoveNoNode(self):
        print
        from drive_remove import DriveRemove
        assert not DriveRemove(node_ips=[RandomIP()])

    def test_negative_DriveRemoveFailure(self):
        print
        nodes_with_drives = set([drive["nodeID"] for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "active"])
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"] if node["nodeID"] in nodes_with_drives]
        node_ips = random.sample(active_ips, random.randint(1, len(active_ips)))
        drive_slots = None if random.choice([True, False]) else random.sample(range(-1, 10), random.randint(1, 10))
        from drive_remove import DriveRemove
        with APIFailure("RemoveDrives"):
            assert not DriveRemove(by_node=random.choice([True, False]),
                                    wait_for_sync=random.choice([True, False]),
                                    node_ips=node_ips,
                                    drive_slots=drive_slots)

    def test_DriveRemoveNoneActive(self):
        print
        nodes_without_drives = set([drive["nodeID"] for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "available"])
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"] if node["nodeID"] in nodes_without_drives]
        node_ips = random.sample(active_ips, 1)
        from drive_remove import DriveRemove
        assert DriveRemove(node_ips=node_ips)

    def test_DriveRemoveByNode(self):
        print
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from drive_remove import DriveRemove
        assert DriveRemove(node_ips=active_ips,
                            by_node=True,
                            wait_for_sync=random.choice([True, False]))

    def test_DriveRemoveByNodeAndSlot(self):
        print
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from drive_remove import DriveRemove
        assert DriveRemove(node_ips=active_ips,
                            by_node=True,
                            drive_slots=random.sample(range(-1,10), random.randint(1, 6)),
                            wait_for_sync=random.choice([True, False]))

    def test_DriveRemoveAllNodes(self):
        print
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from drive_remove import DriveRemove
        assert DriveRemove(node_ips=active_ips,
                            wait_for_sync=random.choice([True, False]))

    def test_DriveRemoveAllNodesAndSlot(self):
        print
        active_ips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from drive_remove import DriveRemove
        assert DriveRemove(node_ips=active_ips,
                            drive_slots=random.sample(range(-1,10), random.randint(1, 10)),
                            wait_for_sync=random.choice([True, False]))

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestDriveWaitfor(object):

    def test_negative_DriveWaitforListDrivesFailure(self):
        print
        from drive_waitfor import DriveWaitfor
        with APIFailure("ListDrives"):
            assert not DriveWaitfor(states=["available"],
                                     expected=random.randint(1, 11))

    def test_negative_DriveWaitforTimeout(self):
        print
        from drive_waitfor import DriveWaitfor
        assert not DriveWaitfor(states=["active"],
                                 compare="gt",
                                 expected=1000,
                                 timeout=1)

    def test_DriveWaitforAvailable(self):
        print
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "available"])
        from drive_waitfor import DriveWaitfor
        assert DriveWaitfor(states=["available"],
                             compare="ge",
                             expected=random.randint(1, expected))

    def test_DriveWaitforActive(self):
        print
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "active"])
        from drive_waitfor import DriveWaitfor
        assert DriveWaitfor(states=["active"],
                             compare="eq",
                             expected=expected)

    def test_DriveWaitforAvailableAndActive(self):
        print
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["status"] == "active" or \
                                                                                                            drive["status"] == "available"])
        from drive_waitfor import DriveWaitfor
        assert DriveWaitfor(states=["active", "available"],
                             compare="gt",
                             expected=random.randint(1, expected-1))
