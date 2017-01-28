#!/usr/bin/env python2.7
#pylint: skip-file

import pytest
import random
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .testutil import RandomString, RandomIQN, RandomIP

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.account_create
class TestAccountCreate(object):

    def test_negative_AccountCreateAccountSearchFailure(self):
        print
        from account_create import AccountCreate
        with APIFailure("ListAccounts"):
            assert not AccountCreate(account_name=RandomString(random.randint(1, 64)))

    def test_AccountCreate(self):
        print
        from account_create import AccountCreate
        assert AccountCreate(account_name=RandomString(random.randint(1, 64)))

    def test_AccountCreateStrict(self):
        print
        from account_create import AccountCreate
        assert AccountCreate(account_name=RandomString(random.randint(1, 64)),
                             strict=True)

    def test_AccountCreateExistingAccount(self):
        print
        existing_name = random.choice(globalconfig.cluster.ListAccounts({})["accounts"])["username"]
        from account_create import AccountCreate
        assert AccountCreate(account_name=existing_name)

    def test_negative_AccountCreateExistingAccountStrict(self):
        print
        existing_name = globalconfig.cluster.ListAccounts({})["accounts"][0]["username"]
        from account_create import AccountCreate
        assert not AccountCreate(account_name=existing_name,
                                 strict=True)

    def test_negative_AccountCreateFailure(self):
        print
        from account_create import AccountCreate
        with APIFailure("AddAccount"):
            assert not AccountCreate(account_name=RandomString(random.randint(1, 64)))

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.account_list_volumes
class TestAccountListVolumes(object):

    def test_AccountListVolumes(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        print "accounts = {}".format(accounts)
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from account_list_volumes import AccountListVolumes
        assert AccountListVolumes(account_id=existing_id)

    def test_negative_AccountListVolumesNoAccount(self):
        print
        from account_list_volumes import AccountListVolumes
        assert not AccountListVolumes(account_id=9999)

    def test_negative_AccountListVolumesSearchFailure(self):
        print
        from account_list_volumes import AccountListVolumes
        with APIFailure("ListAccounts"):
            assert not AccountListVolumes(account_id=9999)

    def test_negative_AccountListVolumesVolumeSearchFailure(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from account_list_volumes import AccountListVolumes
        with APIFailure("ListActiveVolumes"):
            assert not AccountListVolumes(account_id=existing_id)

    def test_AccountListVolumesNoVolumes(self):
        print
        account_id = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) <= 0])["accountID"]
        from account_list_volumes import AccountListVolumes
        assert AccountListVolumes(account_id=account_id)

    def test_AccountListVolumesIDBash(self, capsys):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0])
        from account_list_volumes import AccountListVolumes
        assert AccountListVolumes(account_id=account["accountID"],
                                     output_format="bash",
                                     by_id=True)
        out, _ = capsys.readouterr()
        print out
        assert len(out.split()) == len(account["volumes"])
        [int(volid) for volid in out.split()]

    def test_AccountListVolumesBash(self, capsys):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0])
        from account_list_volumes import AccountListVolumes
        assert AccountListVolumes(account_id=account["accountID"],
                                     output_format="bash")
        out, _ = capsys.readouterr()
        print out
        assert len(out.split()) == len(account["volumes"])

    def test_AccountListVolumesJSONID(self, capsys):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0])
        from account_list_volumes import AccountListVolumes    
        assert AccountListVolumes(account_id=account["accountID"],
                                     output_format="json",
                                     by_id=True)
        out, _ = capsys.readouterr()
        print out
        import json
        js = json.loads(out)
        print js
        assert len(js["volumes"]) == len(account["volumes"])

    def test_AccountListVolumesJSON(self, capsys):
        print
        account = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0])
        from account_list_volumes import AccountListVolumes
        assert AccountListVolumes(account_id=account["accountID"],
                                     output_format="json")
        out, _ = capsys.readouterr()
        print out
        import json
        js = json.loads(out)
        print js
        assert len(js["volumes"]) == len(account["volumes"])

@pytest.mark.usefixtures("fake_cluster_permethod")
@pytest.mark.account_delete
class TestAccountDelete(object):

    def test_AccountDeleteNoAccount(self):
        print
        from account_delete import AccountDelete
        assert AccountDelete(account_name="nomatch")

    def test_negative_AccountDeleteNoAccountStrict(self):
        print
        from account_delete import AccountDelete
        assert not AccountDelete(account_name="nomatch",
                                 strict=True)

    def test_negative_AccountDeleteSearchFailure(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from account_delete import AccountDelete
        with APIFailure("ListAccounts"):
            assert not AccountDelete(account_id=existing_id)

    def test_negative_AccountDeletePurgeFailure(self):
        print
        import json
        deleted_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListDeletedVolumes({})["volumes"]]
        print "deleted_volumes = {}".format(json.dumps(deleted_volumes))
        print "all accounts = {}".format(json.dumps(globalconfig.cluster.ListAccounts({})["accounts"]))
        account_id = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0 and any([vid in deleted_volumes for vid in account["volumes"]])])["accountID"]

        # accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        # deleted_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListDeletedVolumes({})["volumes"]]
        # while True:
        #     account = accounts[random.randint(0, len(accounts)-1)]
        #     if len(account["volumes"]) > 0 and any([vid in deleted_volumes for vid in account["volumes"]]):
        #         account_id = account["accountID"]
        #         break
        from account_delete import AccountDelete
        with APIFailure("PurgeDeletedVolumes"):
            assert not AccountDelete(account_id=account_id)

    def test_negative_AccountDeletePurgeFailurePreFluorine(self):
        print
        deleted_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListDeletedVolumes({})["volumes"]]
        account_id = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) > 0 and any([vid in deleted_volumes for vid in account["volumes"]])])["accountID"]
        # accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        # deleted_volumes = [vol["volumeID"] for vol in globalconfig.cluster.ListDeletedVolumes({})["volumes"]]
        # while True:
        #     account = accounts[random.randint(0, len(accounts)-1)]
        #     if len(account["volumes"]) > 0 and any([vid in deleted_volumes for vid in account["volumes"]]):
        #         account_id = account["accountID"]
        #         break
        from account_delete import AccountDelete
        with APIVersion(8.0), APIFailure("PurgeDeletedVolume"):
            assert not AccountDelete(account_id=account_id)

    def test_negative_AccountDeleteFailure(self):
        print
        account_id = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) <= 0])["accountID"]
        # accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        # for account in accounts:
        #     if len(account["volumes"]) > 0:
        #         empty_account = account["accountID"]
        from account_delete import AccountDelete
        with APIFailure("RemoveAccount"):
            assert not AccountDelete(account_id=account_id)

    def test_AccountDelete(self):
        print
        account_id = random.choice([account for account in globalconfig.cluster.ListAccounts({})["accounts"] if len(account["volumes"]) <= 0])["accountID"]
        # accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        # for account in accounts:
        #     if len(account["volumes"]) > 0:
        #         empty_account = account["accountID"]
        from account_delete import AccountDelete
        assert AccountDelete(account_id=account_id,
                             strict=True)

@pytest.mark.usefixtures("fake_cluster_perclass")
@pytest.mark.account_move_volumes
class TestAccountMoveVolumes(object):

    def test_negative_AccountMoveVolumesNoAccount(self):
        print
        from account_move_volumes import AccountMoveVolumes
        assert not AccountMoveVolumes(account_id=9999)

    def test_negative_AccountMoveVolumesSearchFailure(self):
        print
        from account_move_volumes import AccountMoveVolumes
        with APIFailure("ListAccounts"):
            assert not AccountMoveVolumes(account_id=9999)

    def test_negative_AccountMoveVolumesVolumeSearchFailure(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        existing_id = accounts[random.randint(0, len(accounts)-1)]["accountID"]
        from account_move_volumes import AccountMoveVolumes
        with APIFailure("ListActiveVolumes"):
            assert not AccountMoveVolumes(account_id=existing_id,
                                          volume_names="asdf")

    def test_AccountMoveVolumesTest(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        for account in accounts:
            if len(account["volumes"]) > 0:
                empty_account = account["accountID"]
        volume_count = random.randint(1, 10)
        from account_move_volumes import AccountMoveVolumes
        assert AccountMoveVolumes(account_id=empty_account,
                                  volume_regex=".+",
                                  volume_count=volume_count,
                                  test=True)

    def test_AccountMoveVolumesNoVolumes(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) == 0:
                empty_id = account["accountID"]
                break
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) > 0:
                account_id = account["accountID"]
                break
        from account_move_volumes import AccountMoveVolumes
        assert AccountMoveVolumes(account_id=account_id,
                                  source_account_id=empty_id)

    def test_negative_AccountMoveVolumesNoVolumesStrict(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) == 0:
                empty_id = account["accountID"]
                break
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) > 0:
                account_id = account["accountID"]
                break
        from account_move_volumes import AccountMoveVolumes
        assert not AccountMoveVolumes(account_id=account_id,
                                      source_account_id=empty_id,
                                      strict=True)

    def test_negative_AccountMoveVolumesAlreadyThereStrict(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) > 0:
                account_id = account["accountID"]
                break
        from account_move_volumes import AccountMoveVolumes
        assert not AccountMoveVolumes(account_id=account_id,
                                      source_account_id=account_id,
                                      strict=True)

    def test_negative_AccountMoveVolumesFailure(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) == 0:
                empty_id = account["accountID"]
                break
        volume_count = random.randint(1, 10)
        from account_move_volumes import AccountMoveVolumes
        with APIFailure("ModifyVolume"):
            assert not AccountMoveVolumes(account_id=empty_id,
                                        volume_regex=".+",
                                        volume_count=volume_count)

    def test_AccountMoveVolumes(self):
        print
        accounts = globalconfig.cluster.ListAccounts({})["accounts"]
        while True:
            account = accounts[random.randint(0, len(accounts)-1)]
            if len(account["volumes"]) == 0:
                empty_id = account["accountID"]
                break
        volumes_to_move = random.sample([vol["name"] for vol in globalconfig.cluster.ListActiveVolumes({})["volumes"] if vol["volumeID"] not in account["volumes"]], random.randint(2, 10))
        from account_move_volumes import AccountMoveVolumes
        assert AccountMoveVolumes(account_id=empty_id,
                                    volume_names=volumes_to_move)
