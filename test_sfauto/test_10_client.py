#!/usr/bin/env python2.7
#pylint: skip-file

import json
import pytest
import random
from libsf import SolidFireAPIError
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .fake_client import ClientConnectFailure, ClientCommandFailure
from .testutil import RandomString, RandomIQN, RandomIP

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.volgroup_add_clients
class TestAddClientsToVolgroup(object):

    def test_negative_AddClientsToVolgroupClientFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_clients import AddClientsToVolgroup
        with ClientCommandFailure("( [[ -e /etc/open-iscsi/initiatorname.iscsi ]] && cat /etc/open-iscsi/initiatorname.iscsi || cat /etc/iscsi/initiatorname.iscsi ) | grep -v '#' | cut -d'=' -f2",
                                  (1, "", "No such file or directory")):
            assert not AddClientsToVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                            volgroup_id=volgroup_id,
                                            connection_type="iscsi")

    def test_negative_AddClientsToVolgroupNoGroup(self):
        print
        from volgroup_add_clients import AddClientsToVolgroup
        assert not AddClientsToVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                        volgroup_id=9999,
                                        connection_type="iscsi")

    def test_negative_AddClientsToVolgroupDuplicateID(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_clients import AddClientsToVolgroup
        assert not AddClientsToVolgroup(client_ips=["1.1.1.1", "1.1.1.1"],
                                        volgroup_id=volgroup_id,
                                        connection_type="iscsi")

    def test_negative_AddClientsToVolgroupFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_clients import AddClientsToVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not AddClientsToVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                            volgroup_id=volgroup_id,
                                            connection_type="iscsi")

    def test_AddClientsToVolgroup(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from volgroup_add_clients import AddClientsToVolgroup
        assert AddClientsToVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                    volgroup_id=volgroup_id,
                                    connection_type="iscsi")

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_create_account
class TestClientCreateAccount(object):

    def test_negative_ClientCreateAccountClusterInfoFailure(self):
        print
        from client_create_account import ClientCreateAccount
        with APIFailure("GetClusterInfo"):
            assert not ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateAccountListAccountsFailure(self):
        print
        from client_create_account import ClientCreateAccount
        with APIFailure("ListAccounts"):
            assert not ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateAccountAccountExistsStrict(self):
        print
        existing_name = random.choice(globalconfig.cluster.ListAccounts({})["accounts"])["username"]
        from client_create_account import ClientCreateAccount
        assert not ClientCreateAccount(client_ips=["1.1.1.1"],
                                           account_name=existing_name,
                                           strict=True)

    def test_ClientCreateAccountAccountExists(self):
        print
        existing_name = random.choice(globalconfig.cluster.ListAccounts({})["accounts"])["username"]
        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=["1.1.1.1"],
                                       account_name=existing_name)

    def test_negative_ClientCreateAccountConnectFailure(self):
        print
        from client_create_account import ClientCreateAccount
        with ClientConnectFailure():
            assert not ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_ClientCreateAccountNoCHAP(self):
        print
        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                       chap=False)

    def test_negative_ClientCreateAccountFailure(self):
        print
        from client_create_account import ClientCreateAccount
        with APIFailure("AddAccount"):
            assert not ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_ClientCreateAccount(self):
        print
        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_ClientCreateAccountSharedAccount(self):
        print
        from client_create_account import ClientCreateAccount
        with APIFailure("AddAccount", SolidFireAPIError("AddAccount", {}, "0.0.0.0", "https://0.0.0.0:443/json-rpc/0.0", "xDuplicateUsername", 500, "Fake unit test failure")):
            assert ClientCreateAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                           account_name=RandomString(random.randint(1, 64)),
                                           chap=False)

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.create_volgroup_for_client
class TestClientCreateVolgroup(object):

    def test_negative_ClientCreateVolgroupSearchFailure(self):
        print
        from client_create_volgroup import ClientCreateVolgroup
        with APIFailure("ListVolumeAccessGroups"):
            assert not ClientCreateVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateVolgroupConnectFailure(self):
        print
        from client_create_volgroup import ClientCreateVolgroup
        with ClientConnectFailure():
            assert not ClientCreateVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateVolgroupGroupExistsStrict(self):
        print
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        client = globalconfig.clients.CreateClient(hostname=volgroup["name"])
        from client_create_volgroup import ClientCreateVolgroup
        assert not ClientCreateVolgroup(client_ips=[client.ip],
                                            strict=True)

    def test_ClientCreateVolgroupGroupExists(self):
        print
        volgroup = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])
        client = globalconfig.clients.CreateClient(hostname=volgroup["name"])
        from client_create_volgroup import ClientCreateVolgroup
        assert ClientCreateVolgroup(client_ips=[client.ip])

    def test_negative_ClientCreateVolgroupFailure(self):
        print
        from client_create_volgroup import ClientCreateVolgroup
        with APIFailure("CreateVolumeAccessGroup"):
            assert not ClientCreateVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_ClientCreateVolgroup(self):
        print
        from client_create_volgroup import ClientCreateVolgroup
        assert ClientCreateVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

@pytest.mark.incremental
@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_create_volumes
class TestClientCreateVolumes(object):

    def test_negative_ClientCreateVolumesAccountSearchFailure(self):
        print
        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        with APIFailure("ListAccounts"):
            assert not ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                                volume_count=random.randint(1, 10),
                                                min_iops=random.randint(50, 1000),
                                                max_iops=max_iops,
                                                burst_iops=burst_iops,
                                                enable512e=random.choice([True, False]),
                                                gib=random.choice([True, False]),
                                                create_single=random.choice([True, False]),
                                                wait=random.choice([0, 1]),
                                                client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateVolumesVolumeSearchFailure(self):
        print
        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        with APIFailure("ListActiveVolumes"):
            assert not ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                                volume_count=random.randint(1, 10),
                                                min_iops=random.randint(50, 1000),
                                                max_iops=max_iops,
                                                burst_iops=burst_iops,
                                                enable512e=random.choice([True, False]),
                                                gib=random.choice([True, False]),
                                                create_single=random.choice([True, False]),
                                                wait=random.choice([0, 1]),
                                                client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateVolumesNoAccount(self):
        print
        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        assert not ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                            volume_count=random.randint(1, 10),
                                            min_iops=random.randint(50, 1000),
                                            max_iops=max_iops,
                                            burst_iops=burst_iops,
                                            enable512e=random.choice([True, False]),
                                            gib=random.choice([True, False]),
                                            create_single=random.choice([True, False]),
                                            wait=random.choice([0, 1]),
                                            client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientCreateVolumesVolumeCreateFailure(self):
        print
        client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=client_ips)

        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        with APIFailure("ListActiveVolumes"):
            assert not ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                                volume_count=random.randint(1, 10),
                                                min_iops=random.randint(50, 1000),
                                                max_iops=max_iops,
                                                burst_iops=burst_iops,
                                                enable512e=random.choice([True, False]),
                                                gib=random.choice([True, False]),
                                                create_single=random.choice([True, False]),
                                                wait=random.choice([0, 1]),
                                                client_ips=client_ips)

    def test_ClientCreateVolumes(self, state):
        print
        state.client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=state.client_ips)

        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        assert ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                        volume_count=random.randint(1, 10),
                                        min_iops=random.randint(50, 1000),
                                        max_iops=max_iops,
                                        burst_iops=burst_iops,
                                        enable512e=random.choice([True, False]),
                                        gib=random.choice([True, False]),
                                        create_single=random.choice([True, False]),
                                        wait=random.choice([0, 1]),
                                        client_ips=state.client_ips)

    def test_ClientCreateVolumesExistingVolumes(self, state):
        print
        from client_create_volumes import ClientCreateVolumes
        max_iops = random.randint(3000, 50000)
        burst_iops = int(max_iops * 1.1)
        assert ClientCreateVolumes(volume_size=random.randint(1, 7400),
                                        volume_count=random.randint(1, 10),
                                        min_iops=random.randint(50, 1000),
                                        max_iops=max_iops,
                                        burst_iops=burst_iops,
                                        enable512e=random.choice([True, False]),
                                        gib=random.choice([True, False]),
                                        create_single=random.choice([True, False]),
                                        wait=random.choice([0, 1]),
                                        client_ips=state.client_ips)

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_delete_account
class TestClientDeleteAccount(object):

    def test_negative_ClientDeleteAccountConnectFailure(self):
        print
        from client_delete_account import ClientDeleteAccount
        with ClientConnectFailure():
            assert not ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientDeleteAccountNoAccountStrict(self):
        print
        from client_delete_account import ClientDeleteAccount
        assert not ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                           strict=True)

    def test_ClientDeleteAccountNoAccount(self):
        print
        from client_delete_account import ClientDeleteAccount
        assert ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

    def test_negative_ClientDeleteAccountPurgeFailure(self):
        print
        deleted_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListDeletedVolumes({})["volumes"]]
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0 and any([vid in deleted_volumes for vid in account["volumes"]])])
        from client_delete_account import ClientDeleteAccount
        with APIFailure("PurgeDeletedVolumes"):
            assert not ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                               account_name=account["username"])

    def test_negative_ClientDeleteAccountFailure(self):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) <= 0])
        from client_delete_account import ClientDeleteAccount
        with APIFailure("RemoveAccount"):
            assert not ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                               account_name=account["username"])

    def test_ClientDeleteAccountSharedAccount(self):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) <= 0])
        from client_delete_account import ClientDeleteAccount
        with APIFailure("RemoveAccount", SolidFireAPIError("RemoveAccount", {}, "0.0.0.0", "https://0.0.0.0:443/json-rpc/0.0", "xAccountIDDoesNotExist", 500, "Fake unit test failure")):
            assert ClientDeleteAccount(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                           account_name=account["username"])

    def test_ClientDeleteAccount(self):
        print
        client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=client_ips)

        from client_delete_account import ClientDeleteAccount
        assert ClientDeleteAccount(client_ips=client_ips)

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_delete_volumes
class TestClientDeleteVolumes(object):

    def test_negative_DeleteVolumesforClientsConnectionFailure(self):
        print
        from client_delete_volumes import ClientDeleteVolumes
        with ClientConnectFailure():
            assert not ClientDeleteVolumes(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                               purge=random.choice([True, False]))

    def test_negative_DeleteVolumesforClientsNoAccount(self):
        print
        from client_delete_volumes import ClientDeleteVolumes
        assert ClientDeleteVolumes(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                           purge=random.choice([True, False]))

    def test_negative_DeleteVolumesforClientsVolumeSearchFailure(self):
        print
        from client_delete_volumes import ClientDeleteVolumes
        with APIFailure("ListActiveVolumes"):
            assert not ClientDeleteVolumes(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                               purge=random.choice([True, False]))

    def test_negative_DeleteVolumesforClientsAccountNoVolumes(self):
        print
        client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=client_ips)

        from client_delete_volumes import ClientDeleteVolumes
        assert ClientDeleteVolumes(client_ips=client_ips,
                                       purge=random.choice([True, False]))

    def test_negative_DeleteVolumesforClientsAccountDeleteFailure(self):
        print
        client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=client_ips)

        from client_create_volumes import ClientCreateVolumes
        assert ClientCreateVolumes(volume_count=random.randint(1, 10),
                                       volume_size=random.randint(1, 7400),
                                       client_ips=client_ips)

        from client_delete_volumes import ClientDeleteVolumes
        with APIFailure("DeleteVolumes"):
            assert not ClientDeleteVolumes(client_ips=client_ips,
                                               purge=random.choice([True, False]))

    def test_DeleteVolumesforClients(self):
        print
        client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))]

        from client_create_account import ClientCreateAccount
        assert ClientCreateAccount(client_ips=client_ips)

        from client_create_volumes import ClientCreateVolumes
        assert ClientCreateVolumes(volume_count=random.randint(1, 10),
                                       volume_size=random.randint(1, 7400),
                                       client_ips=client_ips)

        from client_delete_volumes import ClientDeleteVolumes
        assert ClientDeleteVolumes(client_ips=client_ips,
                                       purge=random.choice([True, False]))

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_remove_from_volgroup
class TestClientRemoveFromVolgroup(object):

    def test_negative_ClientRemoveFromVolgroupConnectFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        with ClientConnectFailure():
            assert not ClientRemoveFromVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                                 volgroup_id=volgroup_id,
                                                 connection_type="iscsi")

    def test_negative_ClientRemoveFromVolgroupClientFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        with ClientCommandFailure("cat /etc/iscsi/initiatorname.iscsi | grep -v '#' | cut -d'=' -f2", (1, "", "No such file or directory")):
            assert not ClientRemoveFromVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                                 volgroup_id=volgroup_id,
                                                 connection_type="iscsi")

    def test_negative_ClientRemoveFromVolgroupDuplicateID(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        assert not ClientRemoveFromVolgroup(client_ips=["1.1.1.1", "1.1.1.1"],
                                             volgroup_id=volgroup_id,
                                             connection_type="iscsi")

    def test_negative_ClientRemoveFromVolgroupNotInGroup(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        assert not ClientRemoveFromVolgroup(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))],
                                             volgroup_id=volgroup_id,
                                             connection_type="iscsi")

    def test_negative_ClientRemoveFromVolgroupFailure(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        client_ips = [RandomIP() for _ in xrange(random.randint(2, 6))]
        from volgroup_add_clients import AddClientsToVolgroup
        assert AddClientsToVolgroup(client_ips=client_ips,
                                    volgroup_id=volgroup_id,
                                    connection_type="iscsi")

        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        with APIFailure("ModifyVolumeAccessGroup"):
            assert not ClientRemoveFromVolgroup(client_ips=client_ips,
                                                 volgroup_id=volgroup_id,
                                                 connection_type="iscsi")

    def test_ClientRemoveFromVolgroup(self):
        print
        volgroup_id = random.choice(globalconfig.cluster.ListVolumeAccessGroups({})["volumeAccessGroups"])["volumeAccessGroupID"]
        client_ips = [RandomIP() for _ in xrange(random.randint(2, 6))]
        from volgroup_add_clients import AddClientsToVolgroup
        assert AddClientsToVolgroup(client_ips=client_ips,
                                    volgroup_id=volgroup_id,
                                    connection_type="iscsi")

        from client_remove_from_volgroup import ClientRemoveFromVolgroup
        assert ClientRemoveFromVolgroup(client_ips=client_ips,
                                         volgroup_id=volgroup_id,
                                         connection_type="iscsi")

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.mount_volumes_on_clients
class TestClientMountVolumes(object):

    def test_negative_ClientMountVolumesFailure(self):
        print
        from client_mount_volumes import ClientMountVolumes
        assert not ClientMountVolumes(client_ips=[RandomIP() for _ in xrange(random.randint(2, 6))])

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_verify_volume_count
class TestClientVerifyVolumeCount(object):

    def test_NoVolumes(self, capfd):
        print
        client_ips = [RandomIP() for _ in xrange(random.randint(2, 5))]

        from client_verify_volume_count import ClientVerifyVolumeCount
        assert ClientVerifyVolumeCount(expected=0,
                                       client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Found 0 volumes") == len(client_ips)
        assert " but expected" not in stdout

    def test_RandomVolumes(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_verify_volume_count import ClientVerifyVolumeCount
        assert ClientVerifyVolumeCount(expected=volume_count,
                                       client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Found {} volumes".format(volume_count)) == len(client_ips)
        assert " but expected" not in stdout

    def test_negative_FewerVolumes(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_verify_volume_count import ClientVerifyVolumeCount
        assert not ClientVerifyVolumeCount(expected=volume_count+1,
                                           client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Found {} volumes but expected {} volumes".format(volume_count, volume_count+1)) == len(client_ips)

    def test_negative_MoreVolumes(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_verify_volume_count import ClientVerifyVolumeCount
        assert not ClientVerifyVolumeCount(expected=volume_count-1,
                                           client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Found {} volumes but expected {} volumes".format(volume_count, volume_count-1)) == len(client_ips)

    def test_negative_ClientConnectFailure(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_verify_volume_count import ClientVerifyVolumeCount
        with ClientConnectFailure():
            assert not ClientVerifyVolumeCount(expected=volume_count,
                                               client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("SSH error:") == len(client_ips)

    def test_negative_ClientError(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_verify_volume_count import ClientVerifyVolumeCount
        with ClientCommandFailure("iscsiadm"):
            assert not ClientVerifyVolumeCount(expected=volume_count,
                                               client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Client command failed") == len(client_ips)

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_list_volumes
class TestGetClientVolumes(object):

    def test_NoVolumes(self, capfd):
        print
        client_ip = RandomIP()

        from client_list_volumes import GetClientVolumes
        assert GetClientVolumes(client_ip=client_ip)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "Found 0 iSCSI volumes" in stdout

    def test_RandomVolumes(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client = globalconfig.clients.CreateClient()
        client.SetClientConnectedVolumes(volume_count)

        from client_list_volumes import GetClientVolumes
        assert GetClientVolumes(client_ip=client.ip)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "Found {} iSCSI volumes".format(volume_count) in stdout

    def test_RandomVolumesBash(self, capsys):
        print
        volume_count = random.randint(10, 100)
        client = globalconfig.clients.CreateClient()
        client.SetClientConnectedVolumes(volume_count)

        from client_list_volumes import GetClientVolumes
        assert GetClientVolumes(client_ip=client.ip,
                                output_format="bash")

        stdout, _ = capsys.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert len(stdout.split()) == volume_count

    def test_RandomVolumesJson(self, capsys):
        print
        volume_count = random.randint(10, 100)
        client = globalconfig.clients.CreateClient()
        client.SetClientConnectedVolumes(volume_count)

        from client_list_volumes import GetClientVolumes
        assert GetClientVolumes(client_ip=client.ip,
                                output_format="json")

        stdout, _ = capsys.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        vols = json.loads(stdout)
        assert len(vols["volumes"]) == volume_count

    def test_negative_ClientConnectFailure(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client = globalconfig.clients.CreateClient()
        client.SetClientConnectedVolumes(volume_count)

        from client_list_volumes import GetClientVolumes
        with ClientConnectFailure():
            assert not GetClientVolumes(client_ip=client.ip)
    
        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "SSH error:" in stdout

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_clean_iscsi
class TestClientCleanIscsi(object):

    def test_NoVolumes(self, capfd):
        print
        client_ip = RandomIP()

        from client_clean_iscsi import ClientCleanIscsi
        assert ClientCleanIscsi(client_ips=[client_ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "Cleaned iSCSI" in stdout
        assert "Successfully cleaned iSCSI on all clients" in stdout

    def test_RandomVolumes(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_clean_iscsi import ClientCleanIscsi
        assert ClientCleanIscsi(client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Cleaned iSCSI") == len(client_ips)
        assert "Successfully cleaned iSCSI on all clients" in stdout

    def test_negative_ClientConnectFailure(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(volume_count)
            client_ips.append(client.ip)

        from client_clean_iscsi import ClientCleanIscsi
        with ClientConnectFailure():
            assert not ClientCleanIscsi(client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("SSH error:") == len(client_ips)

    @pytest.mark.skipif(True, reason="Need failing command sample output")
    def test_negative_LogoutFailure(self):
        print

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_login_volumes
class TestClientLoginVolumes(object):

    def test_NoVolumes(self, capfd):
        print
        client = globalconfig.clients.CreateClient()
        globalconfig.cluster.AddAccount({"username":client.hostname})

        from client_login_volumes import ClientLoginVolumes
        assert ClientLoginVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "There were no iSCSI targets discovered" in stdout
        assert "Logging in to 0 iSCSI volumes" in stdout

    def test_SerialLogin(self, capfd):
        print
        volume_count = random.randint(10, 50)
        client_count = random.randint(2, 5)
        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            globalconfig.cluster.AddAccount({"username":client.hostname})
            globalconfig.cluster.CreateRandomVolumes(volume_count, client.hostname)
            client_ips.append(client.ip)

        from client_login_volumes import ClientLoginVolumes
        assert ClientLoginVolumes(client_ips=client_ips,
                            login_order="serial")

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Logging in to {} iSCSI volumes".format(volume_count)) == client_count
        assert stdout.count("Logging in to iqn") == volume_count * client_count
        assert stdout.count("->") == volume_count * client_count

    def test_ParallelLogin(self, capfd):
        print
        volume_count = random.randint(10, 50)
        client_count = random.randint(2, 5)
        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            globalconfig.cluster.AddAccount({"username":client.hostname})
            globalconfig.cluster.CreateRandomVolumes(volume_count, client.hostname)
            client_ips.append(client.ip)

        from client_login_volumes import ClientLoginVolumes
        assert ClientLoginVolumes(client_ips=client_ips,
                            login_order="parallel")

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Logging in to {} iSCSI volumes".format(volume_count)) == client_count
        assert stdout.count("Logging in to all targets in parallel") == client_count
        assert stdout.count("->") == volume_count * client_count

    def test_SharedAccount(self, capfd):
        print
        volume_count = random.randint(10, 50)
        client_count = random.randint(2, 5)

        account_name = RandomString(random.randint(6, 16))
        globalconfig.cluster.AddAccount({"username":account_name})
        globalconfig.cluster.CreateRandomVolumes(volume_count, account_name)

        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            client_ips.append(client.ip)

        from client_login_volumes import ClientLoginVolumes
        assert ClientLoginVolumes(client_ips=client_ips,
                            login_order="parallel",
                            account_name=account_name)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Using account {}".format(account_name)) == client_count
        assert stdout.count("Logging in to {} iSCSI volumes".format(volume_count)) == client_count
        assert stdout.count("Logging in to all targets in parallel") == client_count
        assert stdout.count("->") == volume_count * client_count

    def test_Volgroup(self, capfd):
        print
        volume_count = random.randint(10, 50)
        client_count = random.randint(2, 5)
        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            globalconfig.cluster.AddAccount({"username":client.hostname})
            globalconfig.cluster.CreateVolumeAccessGroup({"name":client.hostname, "initiators":[client.iqn]})
            globalconfig.cluster.CreateRandomVolumes(volume_count, client.hostname, client.hostname)
            client_ips.append(client.ip)

        from client_login_volumes import ClientLoginVolumes
        assert ClientLoginVolumes(client_ips=client_ips,
                            login_order="serial",
                            auth_type="iqn")

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("Logging in to {} iSCSI volumes".format(volume_count)) == client_count
        assert stdout.count("Logging in to iqn") == volume_count * client_count
        assert stdout.count("->") == volume_count * client_count

    def test_negative_GetClusterInfoError(self, capfd):
        print
        client = globalconfig.clients.CreateClient()
        globalconfig.cluster.AddAccount({"username":client.hostname})

        from client_login_volumes import ClientLoginVolumes
        with APIFailure("GetClusterInfo"):
            assert not ClientLoginVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "method=[GetClusterInfo]" in stdout

    def test_negative_UnknownAccount(self, capfd):
        print
        client = globalconfig.clients.CreateClient()

        from client_login_volumes import ClientLoginVolumes
        assert not ClientLoginVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "Could not find account" in stdout

    def test_negative_ListAccountsError(self, capfd):
        print
        client = globalconfig.clients.CreateClient()
        globalconfig.cluster.AddAccount({"username":client.hostname})

        from client_login_volumes import ClientLoginVolumes
        with APIFailure("ListAccounts"):
            assert not ClientLoginVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "method=[ListAccounts]" in stdout

    def test_negative_DiscoveryError(self, capfd):
        print
        volume_count = random.randint(10, 50)
        client = globalconfig.clients.CreateClient()
        globalconfig.cluster.AddAccount({"username":client.hostname})
        globalconfig.cluster.CreateRandomVolumes(volume_count, client.hostname)

        from client_login_volumes import ClientLoginVolumes
        with ClientCommandFailure("iscsiadm -m discovery -t sendtargets -p"):
            assert not ClientLoginVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert "Client command failed" in stdout

    def test_negative_ClientConnectFailure(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            globalconfig.cluster.AddAccount({"username":client.hostname})
            globalconfig.cluster.CreateRandomVolumes(volume_count, client.hostname)
            client_ips.append(client.ip)

        from client_login_volumes import ClientLoginVolumes
        with ClientConnectFailure(random.choice(client_ips)):
            assert not ClientLoginVolumes(client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("SSH error:") == 1
        assert "Could not log in to volumes on all clients" in stdout


@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.client_logout_volumes
class TestClientLogoutVolumes(object):

    def test_NoVolumes(self, capfd):
        print
        client = globalconfig.clients.CreateClient()
        globalconfig.cluster.AddAccount({"username":client.hostname})

        from client_logout_volumes import ClientLogoutVolumes
        assert ClientLogoutVolumes(client_ips=[client.ip])

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert ": Logged out of all volumes" in stdout
        assert ": Cleaned iSCSI" in stdout
        assert "Successfully logged out of volumes on all clients" in stdout

    def test_RandomVolumes(self, capfd):
        print
        client_count = random.randint(2, 5)
        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(random.randint(10, 50))
            client_ips.append(client.ip)

        from client_logout_volumes import ClientLogoutVolumes
        assert ClientLogoutVolumes(client_ips=client_ips)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count(": Logged out of all volumes") == client_count
        assert stdout.count(": Cleaned iSCSI") == client_count
        assert "Successfully logged out of volumes on all clients" in stdout

    def test_NoClean(self, capfd):
        print
        client_count = random.randint(2, 5)
        client_ips = []
        for _ in xrange(client_count):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(random.randint(10, 50))
            client_ips.append(client.ip)

        from client_logout_volumes import ClientLogoutVolumes
        assert ClientLogoutVolumes(client_ips=client_ips,
                             clean=False)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count(": Logged out of all volumes") == client_count
        assert "Cleaned iSCSI" not in stdout
        assert "Successfully logged out of volumes on all clients" in stdout

    def test_negative_ClientConnectFailure(self, capfd):
        print
        volume_count = random.randint(10, 100)
        client_ips = []
        for idx in xrange(random.randint(2, 5)):
            client = globalconfig.clients.CreateClient()
            client.SetClientConnectedVolumes(random.randint(10, 50))
            client_ips.append(client.ip)

        from client_logout_volumes import ClientLogoutVolumes
        with ClientConnectFailure(random.choice(client_ips)):
            assert not ClientLogoutVolumes(client_ips=client_ips,
                                     clean=False)

        stdout, _ = capfd.readouterr()
        print "\ncaptured stdout = [{}]".format(stdout)
        assert stdout.count("SSH error:") == 1
        assert "Could not log out of volumes on all clients" in stdout


