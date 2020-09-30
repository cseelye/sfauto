#!/usr/bin/env python
#pylint: skip-file

from __future__ import print_function
import pytest
import random
from libsf import SolidFireAPIError, InvalidArgumentError
from libsf import netutil
from . import globalconfig
from .fake_cluster import APIFailure, APIVersion
from .fake_client import ClientCommandFailure
from .testutil import RandomString, RandomIP

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodeAdd10gRoute(object):

    def test_NodeAdd10gRoute(self):
        print()
        node_ip = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])["mip"]
        gateway = RandomIP()
        netmask = "255.255.255.0"
        network = netutil.CalculateNetwork(gateway, netmask)
        from node_add_10g_route import NodeAdd10gRoute
        assert NodeAdd10gRoute(node_ip=node_ip,
                                 network=network,
                                 netmask=netmask,
                                 gateway=gateway)

    def test_negative_NodeAdd10gRouteFailure(self):
        print()
        node_ip = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])["mip"]
        gateway = RandomIP()
        netmask = "255.255.255.0"
        network = netutil.CalculateNetwork(gateway, netmask)
        from node_add_10g_route import NodeAdd10gRoute
        with APIFailure("SetConfig"):
            assert not NodeAdd10gRoute(node_ip=node_ip,
                                         network=network,
                                         netmask=netmask,
                                         gateway=gateway)

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestClusterAddNodes(object):

    def test_negative_ClusterAddNodesListNodesFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("ListAllNodes"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_negative_ClusterAddNodesFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("AddNodes"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_negative_ClusterAddNodesListActiveNodesFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("ListActiveNodes"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_negative_ClusterAddNodesGetDriveConfigFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("GetDriveConfig"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_negative_ClusterAddNodesListDrivesFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("ListDrives"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_negative_ClusterAddNodesAddDrivesFailure(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        with APIFailure("AddDrives"):
            assert not ClusterAddNodes(node_ips=pending_ips,
                                rtfi=random.choice([True, False]),
                                by_node=random.choice([True, False]),
                                wait_for_sync=random.choice([True, False]))

    def test_ClusterAddNodes(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        assert ClusterAddNodes(node_ips=pending_ips,
                        rtfi=random.choice([True, False]),
                        by_node=False,
                        wait_for_sync=random.choice([True, False]))

    def test_ClusterAddNodesByNode(self):
        print()
        pending_ips = [node["mip"] for node in globalconfig.cluster.ListPendingNodes({})["pendingNodes"]]
        from cluster_add_nodes import ClusterAddNodes
        assert ClusterAddNodes(node_ips=pending_ips,
                        rtfi=random.choice([True, False]),
                        by_node=True,
                        wait_for_sync=random.choice([True, False]))

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodeGetDriveCount(object):

    def test_NodeGetDriveCount(self):
        print()
        node_ip = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])["mip"]
        from node_get_drive_count import NodeGetDriveCount
        assert NodeGetDriveCount(node_ip=node_ip)

    def test_negative_NodeGetDriveCountFailure(self):
        print()
        node_ip = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])["mip"]
        from node_get_drive_count import NodeGetDriveCount
        with APIFailure("GetDriveConfig"):
            assert not NodeGetDriveCount(node_ip=node_ip)

    def test_NodeGetDriveCountBash(self, capsys):
        print()
        node = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["nodeID"] == node["nodeID"]])
        from node_get_drive_count import NodeGetDriveCount
        assert NodeGetDriveCount(node_ip=node["mip"],
                                 output_format="bash")
        out, _ = capsys.readouterr()
        print("captured = [{}]".format(out))
        assert int(out.strip()) == expected

    def test_NodeGetDriveCountJSON(self, capsys):
        print()
        node = random.choice(globalconfig.cluster.ListActiveNodes({})["nodes"])
        expected = len([drive for drive in globalconfig.cluster.ListDrives({})["drives"] if drive["nodeID"] == node["nodeID"]])
        from node_get_drive_count import NodeGetDriveCount
        assert NodeGetDriveCount(node_ip=node["mip"],
                                 output_format="json")
        out, _ = capsys.readouterr()
        print("captured = [{}]".format(out))
        import json
        drives = json.loads(out)
        assert "driveCount" in drives
        assert int(drives["driveCount"]) == expected

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestClusterListNodes(object):

    def test_ActiveNodes(self, capfd):
        print()
        node_mips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(node_state="active")
        
        out, _ = capfd.readouterr()
        print("captured = [{}]".format(out))
        assert "{} active nodes in cluster".format(len(node_mips)) in out

    def test_PendingNodes(self, capfd):
        print()
        node_mips = [node["mip"] for node in globalconfig.cluster.ListAllNodes({})["pendingNodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(node_state="pending")
        
        out, _ = capfd.readouterr()
        print("captured = [{}]".format(out))
        assert "{} pending nodes in cluster".format(len(node_mips)) in out

    def test_AllNodes(self, capfd):
        print()
        nodes = globalconfig.cluster.ListAllNodes({})
        nodes = nodes["nodes"] + nodes["pendingNodes"]

        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(node_state="all")

        out, _ = capfd.readouterr()
        print("captured = [{}]".format(out))
        assert "{} nodes in cluster".format(len(nodes)) in out

    def test_AllNodesDefault(self, capfd):
        print()
        nodes = globalconfig.cluster.ListAllNodes({})
        nodes = nodes["nodes"] + nodes["pendingNodes"]

        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes()

        out, _ = capfd.readouterr()
        print("captured = [{}]".format(out))
        assert "{} nodes in cluster".format(len(nodes)) in out

    def test_negative_ListAllNodesFailure(self, capsys):
        print()
        node_mips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        with APIFailure("ListAllNodes"):
            assert not ClusterListNodes(node_state="active")

    def test_ActiveNodesBash(self, capsys):
        print()
        node_mips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(node_state="active",
                        output_format="bash")

        out, _ = capsys.readouterr()
        print("captured = [{}]".format(out))
        assert len(out.split()) == len(node_mips)
        assert all(ip in node_mips for ip in out.split())

    def test_ActiveNodesJSON(self, capsys):
        print()
        node_mips = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(node_state="active",
                        output_format="json")

        out, _ = capsys.readouterr()
        print("captured = [{}]".format(out))
        import json
        js = json.loads(out)
        assert len(js["nodes"]) == len(node_mips)
        assert all([ip in node_mips for ip in js["nodes"]])

    def test_ActiveNodesBashID(self, capsys):
        print()
        node_ids = [str(node["nodeID"]) for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(by_id=True,
                        node_state="active",
                        output_format="bash")
        out, _ = capsys.readouterr()
        out = out.strip()
        print("captured = [{}]".format(out))
        print("node_ids = {}".format(node_ids))
        assert len(out.split()) == len(node_ids)
        assert all(nid in node_ids for nid in out.split())

    def test_ActiveNodesJSONID(self, capsys):
        print()
        node_ids = [node["nodeID"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from cluster_list_nodes import ClusterListNodes
        assert ClusterListNodes(by_id=True,
                        node_state="active",
                        output_format="json")
        out, _ = capsys.readouterr()
        print("captured = [{}]".format(out))
        import json
        js = json.loads(out)
        assert len(js["nodes"]) == len(node_ids)
        assert all([nid in node_ids for nid in js["nodes"]])

@pytest.mark.skipif(True, reason="Need to fake AT2 GetResource calls for looking up IPMI addresses")
@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodePowerOff(object):

    def test_NodePowerOff(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_off import NodePowerOff
        assert NodePowerOff(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))))

    def test_NodePowerOffBoulder192Subnet(self):
        print()
        from node_power_off import NodePowerOff
        assert NodePowerOff(node_ips=["192.168.133.{}".format(random.randint(1, 100)) for _ in range(random.randint(1, 5))])

    def test_negative_NodePowerOffFailure(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_off import NodePowerOff
        with ClientCommandFailure("ipmitool -Ilanplus -Uroot -Pcalvin -H"):
            assert not NodePowerOff(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))))

    def test_negative_NodePowerOffBadIPMIaddressList(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        node_ips = random.sample(active_nodes, random.randint(1, len(active_nodes)))
        ipmi_ips = [ip for ip in node_ips]
        ipmi_ips.pop()
        from node_power_off import NodePowerOff
        with pytest.raises(InvalidArgumentError):
            assert not NodePowerOff(node_ips=node_ips,
                                     ipmi_ips=ipmi_ips)

@pytest.mark.skipif(True, reason="Need to fake AT2 GetResource calls for looking up IPMI addresses")
@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodePowerOn(object):

    def test_NodePowerOn(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_on import NodePowerOn
        assert NodePowerOn(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))))

    def test_NodePowerOnNoWait(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_on import NodePowerOn
        assert NodePowerOn(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))),
                            wait_for_up=False)

    def test_NodePowerOnBoulder192Subnet(self):
        print()
        from node_power_on import NodePowerOn
        assert NodePowerOn(node_ips=["192.168.133.{}".format(random.randint(1, 100)) for _ in range(random.randint(1, 5))])

    def test_negative_NodePowerOnFailure(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_on import NodePowerOn
        with ClientCommandFailure("ipmitool -Ilanplus -Uroot -Pcalvin -H"):
            assert not NodePowerOn(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))))

    def test_negative_NodePowerOnBadIPMIaddressList(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        node_ips = random.sample(active_nodes, random.randint(1, len(active_nodes)))
        ipmi_ips = [ip for ip in node_ips]
        ipmi_ips.pop()
        from node_power_on import NodePowerOn
        with pytest.raises(InvalidArgumentError):
            assert not NodePowerOn(node_ips=node_ips,
                                     ipmi_ips=ipmi_ips)

@pytest.mark.skipif(True, reason="Need to fake AT2 GetResource calls for looking up IPMI addresses")
@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodePowerCycle(object):

    def test_NodePowerCycle(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_cycle import NodePowerCycle
        assert NodePowerCycle(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))),
                               down_time=random.randint(1, 20))

    def test_NodePowerCycleNoWait(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_cycle import NodePowerCycle
        assert NodePowerCycle(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))),
                               wait_for_up=False)

    def test_NodePowerCycleNoDownTime(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_cycle import NodePowerCycle
        assert NodePowerCycle(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))),
                               down_time=0)

    def test_NodePowerCycleBoulder192Subnet(self):
        print()
        from node_power_cycle import NodePowerCycle
        assert NodePowerCycle(node_ips=["192.168.133.{}".format(random.randint(1, 100)) for _ in range(random.randint(1, 5))],
                               down_time=random.randint(1, 20))

    def test_negative_NodePowerCycleFailure(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        from node_power_cycle import NodePowerCycle
        with ClientCommandFailure("ipmitool -Ilanplus -Uroot -Pcalvin -H"):
            assert not NodePowerCycle(node_ips=random.sample(active_nodes, random.randint(1, len(active_nodes))),
                                       down_time=random.randint(1, 20))

    def test_negative_NodePowerCycleBadIPMIaddressList(self):
        print()
        active_nodes = [node["mip"] for node in globalconfig.cluster.ListActiveNodes({})["nodes"]]
        node_ips = random.sample(active_nodes, random.randint(1, len(active_nodes)))
        ipmi_ips = [ip for ip in node_ips]
        ipmi_ips.pop()
        from node_power_cycle import NodePowerCycle
        with pytest.raises(InvalidArgumentError):
            assert not NodePowerCycle(node_ips=node_ips,
                                       ipmi_ips=ipmi_ips)

@pytest.mark.usefixtures("fake_cluster_permethod")
class TestNodeGetBinaryVersion(object):

    def test_NodeGetBinaryVersion(self):
        print()
        from node_get_binary_version import NodeGetBinaryVersion
        assert NodeGetBinaryVersion()

    def test_negative_ListActiveNodesFailure(self):
        print()
        from node_get_binary_version import NodeGetBinaryVersion
        with APIFailure("ListAllNodes"):
            assert not NodeGetBinaryVersion()

    def test_negative_GetVersionInfoFailure(self):
        print()
        from node_get_binary_version import NodeGetBinaryVersion
        with APIFailure("ListAllNodes"):
            assert not NodeGetBinaryVersion()

