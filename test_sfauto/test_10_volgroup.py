#!/usr/bin/env python2.7
#pylint: skip-file

import pytest
import random
from libsf import InvalidArgumentError
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .testutil import RandomString, RandomIP, RandomIQN

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestCreateVolgroup(object):

    def test_CreateVolgroup(self):
        print
        from volgroup_create import CreateVolumeGroup
        assert CreateVolumeGroup(RandomString(random.randint(1, 64)))

    def test_CreateVolgroupWithVolumesAndInitiators(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_create import CreateVolumeGroup
        assert CreateVolumeGroup(RandomString(random.randint(1, 64)),
                                 iqns=[RandomIQN() for _ in xrange(random.randint(1, 5))],
                                 volume_ids=volume_ids)

    def test_negative_CreateVolgroupVolumeSearchFailure(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_create import CreateVolumeGroup
        with APIFailure("ListActiveVolumes"):
            assert not CreateVolumeGroup(volgroup_name=RandomString(random.randint(1, 64)),
                                         volume_ids=volume_ids)

    def test_negative_CreateVolgroupExistsStrict(self):
        print
        from volgroup_create import CreateVolumeGroup
        name = RandomString(random.randint(1, 64))
        assert CreateVolumeGroup(volgroup_name=name)
        assert not CreateVolumeGroup(volgroup_name=name,
                                     strict=True)

    def test_CreateVolgroupExists(self):
        print
        from volgroup_create import CreateVolumeGroup
        name = RandomString(random.randint(1, 64))
        assert CreateVolumeGroup(volgroup_name=name)
        assert CreateVolumeGroup(volgroup_name=name)

    def test_negative_CreateVolgroupSearchFailure(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_create import CreateVolumeGroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not CreateVolumeGroup(volgroup_name=RandomString(random.randint(1, 64)),
                                         volume_ids=volume_ids)

    def test_CreateVolgroupTest(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_create import CreateVolumeGroup
        assert CreateVolumeGroup(volgroup_name=RandomString(random.randint(1, 64)),
                                 volume_ids=volume_ids,
                                 test=True)

    def test_negative_CreateVolgroupFailure(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_create import CreateVolumeGroup
        with APIFailure("CreateVolumeAccessGroup"):
            assert not CreateVolumeGroup(volgroup_name=RandomString(random.randint(1, 64)),
                                         volume_ids=volume_ids)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestAddInitiatorsToVolgroup(object):

    def test_negative_AddInitiatorsToVolgroupNoGroup(self):
        print
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        assert not AddInitiatorsToVolgroup(initiators=[RandomIQN() for _ in xrange(random.randint(1, 5))],
                                           volgroup_id=9999)

    def test_negative_AddInitiatorsToVolgroupSearchFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not AddInitiatorsToVolgroup(initiators=[RandomIQN() for _ in xrange(random.randint(1, 5))],
                                               volgroup_id=1)

    def test_AddInitiatorsToVolgroupAlreadyIn(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) > 0])
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        assert AddInitiatorsToVolgroup(initiators=volgroup["initiators"],
                                       volgroup_id=volgroup["volumeAccessGroupID"])

    def test_negative_AddInitiatorsToVolgroupAlreadyInStrict(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) > 0])
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        assert not AddInitiatorsToVolgroup(initiators=volgroup["initiators"],
                                           volgroup_name=volgroup["name"],
                                           strict=True)

    def test_negative_AddInitiatorsToVolgroupFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not AddInitiatorsToVolgroup(initiators=[RandomIQN() for _ in xrange(random.randint(1, 5))],
                                               volgroup_id=volgroup_id)

    def test_AddInitiatorsToVolgroup(self):
        print
        volgroup_name = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["name"]
        from volgroup_add_initiators import AddInitiatorsToVolgroup
        assert AddInitiatorsToVolgroup(initiators=[RandomIQN() for _ in xrange(random.randint(1, 5))],
                                       volgroup_name=volgroup_name)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestAddVolumesToVolgroup(object):

    def test_negative_AddVolumesToVolgroupNoGroup(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_add_volumes import AddVolumesToVolgroup
        assert not AddVolumesToVolgroup(volume_ids=volume_ids,
                                        volgroup_id=9999)

    def test_negative_AddVolumesToVolgroupSearchFailure(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        from volgroup_add_volumes import AddVolumesToVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not AddVolumesToVolgroup(volume_ids=volume_ids,
                                            volgroup_id=volgroup["volumeAccessGroupID"])

    def test_negative_AddVolumesToVolgroupVolumeSearchFailure(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        from volgroup_add_volumes import AddVolumesToVolgroup
        with APIFailure("ListActiveVolumes"):
            assert not AddVolumesToVolgroup(volume_ids=volume_ids,
                                            volgroup_id=volgroup["volumeAccessGroupID"])

    def test_AddVolumesToVolgroupTest(self):
        print
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in volgroup["volumes"]], random.randint(2, 15))
        from volgroup_add_volumes import AddVolumesToVolgroup
        assert AddVolumesToVolgroup(volume_ids=volume_ids,
                                    volgroup_id=volgroup["volumeAccessGroupID"],
                                    test=True)

    def test_AddVolumesToVolgroupAlreadyIn(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 1])
        volume_ids = random.sample(volgroup["volumes"], random.randint(2, min(15, len(volgroup["volumes"]))))
        from volgroup_add_volumes import AddVolumesToVolgroup
        assert AddVolumesToVolgroup(volume_ids=random.sample(range(1, 27), random.randint(1, 10)),
                                    volgroup_id=volgroup["volumeAccessGroupID"])

    def test_negative_AddVolumesToVolgroupAlreadyInStrict(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 1])
        volume_ids = random.sample(volgroup["volumes"], random.randint(2, min(15, len(volgroup["volumes"]))))
        from volgroup_add_volumes import AddVolumesToVolgroup
        assert not AddVolumesToVolgroup(volume_ids=volume_ids,
                                        volgroup_id=volgroup["volumeAccessGroupID"],
                                        strict=True)

    def test_negative_AddVolumesToVolgroupFailure(self):
        print
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in volgroup["volumes"]], random.randint(2, 15))
        from volgroup_add_volumes import AddVolumesToVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not AddVolumesToVolgroup(volume_ids=volume_ids,
                                            volgroup_name=volgroup["name"])

    def test_AddVolumesToVolgroup(self):
        print
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in volgroup["volumes"]], random.randint(2, 15))
        from volgroup_add_volumes import AddVolumesToVolgroup
        assert AddVolumesToVolgroup(volume_ids=volume_ids,
                                    volgroup_name=volgroup["name"])

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestDeleteAllVolgroups(object):

    def test_negative_DeleteAllVolgroupsSearchFailure(self):
        print
        from volgroup_delete_all import DeleteAllVolgroups
        with APIFailure("ListVolumeAccessGroups"):
            assert not DeleteAllVolgroups()

    def test_negative_DeleteAllVolgroupsFailure(self):
        print
        from volgroup_delete_all import DeleteAllVolgroups
        with APIFailure("DeleteVolumeAccessGroup"):
            assert not DeleteAllVolgroups()

    def test_DeleteAllVolgroups(self):
        print
        from volgroup_delete_all import DeleteAllVolgroups
        assert DeleteAllVolgroups()

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestDeleteVolgroup(object):

    def test_negative_DeleteVolgroupNoGroupStrict(self):
        print
        from volgroup_delete import DeleteVolgroup
        assert not DeleteVolgroup(volgroup_id=9999,
                                  strict=True)

    def test_DeleteVolgroupNoGroup(self):
        print
        from volgroup_delete import DeleteVolgroup
        assert DeleteVolgroup(volgroup_id=9999)

    def test_negative_DeleteVolgroupsSearchFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_delete import DeleteVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not DeleteVolgroup(volgroup_id=volgroup_id)

    def test_negative_DeleteVolgroupsFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_delete import DeleteVolgroup
        with APIFailure("DeleteVolumeAccessGroup"):
            assert not DeleteVolgroup(volgroup_id=volgroup_id)

    def test_DeleteVolgroup(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_delete import DeleteVolgroup
        assert DeleteVolgroup(volgroup_id=volgroup_id)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestListVolgroups(object):

    def test_ListVolgroups(self):
        print
        from volgroup_list import ListVolgroups
        assert ListVolgroups()

    def test_negative_ListVolumesForVolgroupSearchFailure(self):
        print
        from volgroup_list import ListVolgroups
        with APIFailure("ListVolumeAccessGroups"):
            assert not ListVolgroups()

    def test_ListVolgroupsBash(self, capsys):
        print
        group_names = [group["name"] for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"]]
        from volgroup_list import ListVolgroups
        assert ListVolgroups(output_format="bash")
        out, _ = capsys.readouterr()
        print "captured = [{}]".format(out)
        assert out.strip() == " ".join(group_names)

    def test_ListVolgroupsJSON(self, capsys):
        print
        group_names = [group["name"] for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"]]
        from volgroup_list import ListVolgroups
        assert ListVolgroups(output_format="json")
        out, _ = capsys.readouterr()
        print "captured = [{}]".format(out)
        import json
        groups = json.loads(out)
        assert "volumeAccessGroups" in list(groups.keys())
        assert len(groups["volumeAccessGroups"]) == len(group_names)
        assert all([name in groups["volumeAccessGroups"] for name in group_names])

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestModifyVolgroupLunAssignments(object):

    def test_negative_ModifyVolgroupLunAssignmentsNoGroup(self):
        print
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert not ModifyVolgroupLunAssignments(method="seq",
                                            lun_min=random.randint(0, 8193),
                                            lun_max=random.randint(8193, 16383),
                                            volgroup_id=9999)

    def test_negative_ModifyVolgroupLunAssignmentsSearchFailure(self):
        print
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        with APIFailure("ListVolumeAccessGroups"):
            assert not ModifyVolgroupLunAssignments(method="seq",
                                                lun_min=random.randint(0, 8193),
                                                lun_max=random.randint(8193, 16383),
                                                volgroup_id=9999)

    def test_negative_ModifyVolgroupLunAssignmentsMinGtMax(self):
        print
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        with pytest.raises(InvalidArgumentError):
            ModifyVolgroupLunAssignments(method="seq",
                                         lun_min=random.randint(8193, 16383),
                                         lun_max=random.randint(0, 8193),
                                         volgroup_id=9999)

    def test_ModifyVolgroupLunAssignmentsSeq(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert ModifyVolgroupLunAssignments(method="seq",
                                            lun_min=random.randint(4096, 8193),
                                            lun_max=random.randint(8193, 16383),
                                            volgroup_id=volgroup_id)

    def test_negative_ModifyVolgroupLunAssignmentsSeqFailure(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        with APIFailure("ModifyVolumeAccessGroupLunAssignments"):
            assert not ModifyVolgroupLunAssignments(method="seq",
                                                    lun_min=random.randint(4096, 8193),
                                                    lun_max=random.randint(8193, 16383),
                                                    volgroup_id=volgroup_id)

    def test_negative_ModifyVolgroupLunAssignmentsSeqMinTooHigh(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 2])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert not ModifyVolgroupLunAssignments(method="seq",
                                                lun_min=16381,
                                                lun_max=16383,
                                                volgroup_id=volgroup_id)

    def test_ModifyVolgroupLunAssignmentsRev(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert ModifyVolgroupLunAssignments(method="rev",
                                            lun_min=random.randint(4096, 8193),
                                            lun_max=random.randint(8193, 16383),
                                            volgroup_id=volgroup_id)

    def test_negative_ModifyVolgroupLunAssignmentsRevMaxTooLow(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 3])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert not ModifyVolgroupLunAssignments(method="rev",
                                                lun_min=0,
                                                lun_max=2,
                                                volgroup_id=volgroup_id)

    def test_ModifyVolgroupLunAssignmentsRand(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert ModifyVolgroupLunAssignments(method="rand",
                                            lun_min=random.randint(4096, 8193),
                                            lun_max=random.randint(8193, 16383),
                                            volgroup_id=volgroup_id)

    def test_negative_ModifyVolgroupLunAssignmentsRandRangeTooSmall(self):
        print
        volgroup_id = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 3])["volumeAccessGroupID"]
        lun_min = random.randint(0, 16380)
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert not ModifyVolgroupLunAssignments(method="rand",
                                                lun_min=lun_min,
                                                lun_max=lun_min+2,
                                                volgroup_id=volgroup_id)

    def test_ModifyVolgroupLunAssignmentsVol(self):
        print
        groups = [group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0 and max(group["volumes"]) < 16382 - len(group["volumes"])]
        volgroup_id = random.choice(groups)["volumeAccessGroupID"]
        from volgroup_modify_lun_assignments import ModifyVolgroupLunAssignments
        assert ModifyVolgroupLunAssignments(method="vol",
                                            volgroup_id=volgroup_id)

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestRemoveInitiatorsFromVolgroup(object):

    def test_negative_RemoveInitiatorsFromVolgroupNoGroup(self):
        print
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        assert not RemoveInitiatorsFromVolgroup(initiators=[RandomIQN()],
                                                volgroup_id=9999)

    def test_negative_RemoveInitiatorsFromVolgroupSearchFailure(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) > 0])
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not RemoveInitiatorsFromVolgroup(initiators=[RandomIQN()],
                                                    volgroup_id=volgroup["volumeAccessGroupID"])

    def test_RemoveInitiatorsFromVolgroupAlreadyOut(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) <= 0])
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        assert RemoveInitiatorsFromVolgroup(initiators=[RandomIQN()],
                                            volgroup_id=volgroup["volumeAccessGroupID"])

    def test_negative_RemoveInitiatorsFromVolgroupAlreadyOutStrict(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) <= 0])
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        assert not RemoveInitiatorsFromVolgroup(initiators=[RandomIQN()],
                                                volgroup_id=volgroup["volumeAccessGroupID"],
                                                strict=True)

    def test_negative_RemoveInitiatorsFromVolgroupFailure(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) > 0])
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not RemoveInitiatorsFromVolgroup(initiators=random.sample(volgroup["initiators"], random.randint(1, min(5, len(volgroup["initiators"])))),
                                                    volgroup_id=volgroup["volumeAccessGroupID"])

    def test_RemoveInitiatorsFromVolgroup(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["initiators"]) > 0])
        from volgroup_remove_initiators import RemoveInitiatorsFromVolgroup
        assert RemoveInitiatorsFromVolgroup(initiators=random.sample(volgroup["initiators"], random.randint(1, min(5, len(volgroup["initiators"])))),
                                            volgroup_id=volgroup["volumeAccessGroupID"])

@pytest.mark.usefixtures("fake_cluster_perclass")
class TestRemoveVolumesFromVolgroup(object):

    def test_negative_RemoveVolumesFromVolgroupNoGroup(self):
        print
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"]], random.randint(2, 15))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert not RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                             volgroup_id=9999)

    def test_negative_RemoveVolumesFromVolgroupNoVolumes(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) <= 0])
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert not RemoveVolumesFromVolgroup(volgroup_id=volgroup["volumeAccessGroupID"])

    def test_negative_RemoveVolumesFromVolgroupSearchFailure(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])
        volume_ids=random.sample(volgroup["volumes"], random.randint(1, min(5, len(volgroup["volumes"]))))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                                 volgroup_name=volgroup["name"])

    def test_negative_RemoveVolumesFromVolgroupVolumeSearchFailure(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])
        volume_ids=random.sample(volgroup["volumes"], random.randint(1, min(5, len(volgroup["volumes"]))))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        with APIFailure("ListActiveVolumes"):
            assert not RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                                 volgroup_name=volgroup["name"])

    def test_RemoveVolumesFromVolgroupTest(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])
        volume_ids=random.sample(volgroup["volumes"], random.randint(1, min(5, len(volgroup["volumes"]))))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                         volgroup_id=volgroup["volumeAccessGroupID"],
                                         test=True)

    def test_RemoveVolumesFromVolgroupAlreadyOut(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) <= 0])
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in volgroup["volumes"]], random.randint(2, 15))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                         volgroup_name=volgroup["name"])

    def test_negative_RemoveVolumesFromVolgroupAlreadyOutStrict(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) <= 0])
        volume_ids = random.sample([vol["volumeID"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in volgroup["volumes"]], random.randint(2, 15))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert not RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                             volgroup_id=volgroup["volumeAccessGroupID"],
                                             strict=True)

    def test_negative_RemoveVolumesFromVolgroupFailure(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])
        volume_ids=random.sample(volgroup["volumes"], random.randint(1, min(5, len(volgroup["volumes"]))))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                                 volgroup_name=volgroup["name"])

    def test_RemoveVolumesFromVolgroup(self):
        print
        volgroup = random.choice([group for group in globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"] if len(group["volumes"]) > 0])
        volume_ids=random.sample(volgroup["volumes"], random.randint(1, min(5, len(volgroup["volumes"]))))
        from volgroup_remove_volumes import RemoveVolumesFromVolgroup
        assert RemoveVolumesFromVolgroup(volume_ids=volume_ids,
                                         volgroup_name=volgroup["name"])

