#!/usr/bin/python

"""
This script will display the used space on the cluster, nodes and drives
    --mvip              The managementVIP of the cluster
    SFMVIP env var

    --user              The cluster admin username
    SFUSER env var

    --pass              The cluster admin password
    SFPASS env var
"""

import sys
from optparse import OptionParser
import time
import curses
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults


class BSInfo:
    def __init__(self):
        self.ServiceID = 0
        self.DiskDevice = ""
        self.DriveID = ""
        self.TotalSize = 0
        self.UsedCapacity = 0
        self.NodeID = 0

class NodeInfo:
    def __init__(self):
        self.Name = ""
        self.NodeID = 0
        self.TotalCapacity = 0
        self.UsedCapacity = 0
        self.BSList = dict()

class ClusterInfo:
    def __init__(self):
        self.Name = ""
        self.Version = ""
        self.TotalCapacity = 0
        self.UsedCapacity = 0
        self.Fullness = ""
        self.NodeList = dict()


def main(stdscr):

    win = None
    while True:
        # Get cluster capacity info
        cluster = ClusterInfo()
        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterInfo', {})
        cluster.Name = result["clusterInfo"]["name"]
        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterCapacity', {})
        cluster.TotalCapacity = result["clusterCapacity"]["maxUsedSpace"]
        cluster.UsedCapacity = result["clusterCapacity"]["usedSpace"]
        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterVersionInfo', {})
        cluster.Version = result["clusterVersion"]
        if float(cluster.Version) > 4.0:
            result = libsf.CallApiMethod(mvip, username, password, 'GetClusterFullThreshold', {})
            cluster.Fullness = result["fullness"]

        # Get node info
        node_list = dict()
        result = libsf.CallApiMethod(mvip, username, password, 'ListActiveNodes', {})
        for node in result["nodes"]:
            n = NodeInfo()
            n.Name = node["name"]
            n.NodeID = node["nodeID"]
            node_list[n.NodeID] = n

        # Get BS info
        bs_list = dict()
        result = libsf.CallApiMethod(mvip, username, password, 'GetCompleteStats', {})
        for clustername in result.keys():
            if "unaligned" in clustername: continue
            for nodeindex,node in result[clustername]["nodes"].iteritems():
                for serviceindex,service in node.iteritems():
                    if "block" in serviceindex:
                        bs = BSInfo()
                        bs.ServiceID = service["serviceID"][0]
                        bs.UsedCapacity = service["activeDiskBytes"][0]
                        bs_list[bs.ServiceID] = bs
            break

        result = libsf.CallApiMethod(mvip, username, password, 'ListServices', {})
        for service in result["services"]:
            if service["service"]["serviceType"] != "block": continue
            serviceid = service["service"]["serviceID"]
            if serviceid in bs_list.keys():
                bs_list[serviceid].NodeID = service["service"]["nodeID"]
                bs_list[serviceid].DriveID = service["drive"]["driveID"]

        result = libsf.CallApiMethod(mvip, username, password, 'GetClusterHardwareInfo', {})
        for driveid,drive in result["clusterHardwareInfo"]["drives"].iteritems():
            if drive == None: continue
            for serviceid,bs in bs_list.iteritems():
                if driveid == str(bs.DriveID):
                    bs.DiskDevice = drive["logicalname"]
                    bs.TotalSize = int(drive["size"])

        for bs in bs_list.values():
            if bs.NodeID <= 0: continue
            node_list[bs.NodeID].BSList[bs.ServiceID] = bs
        for node in node_list.values():
            node.TotalCapacity = 0
            node.UsedCapacity = 0
            for bs in node.BSList.values():
                node.TotalCapacity += bs.TotalSize
                node.UsedCapacity += bs.UsedCapacity
            cluster.NodeList[node.NodeID] = node

        curses.curs_set(0)
        stdscr.clear()
        win_height = len(bs_list.keys()) + len(node_list.keys()) + 5
        win_width = 110
        win = libsf.CreateCenteredWindow(stdscr, win_height, win_width)
        win.border(0)
        win.idlok(True)
        win.scrollok(True)

        current_line = 2
        win.addstr(current_line, 4, cluster.Name + "  (" + libsf.HumanizeDecimal(cluster.UsedCapacity) + "/" + libsf.HumanizeDecimal(cluster.TotalCapacity) + ")  " + ProgressBar(cluster.UsedCapacity, cluster.TotalCapacity))
        current_line += 1
        for node in cluster.NodeList.values():
            win.addstr(current_line, 4, "    " + node.Name + "  (" + libsf.HumanizeDecimal(node.UsedCapacity) + "/" + libsf.HumanizeDecimal(node.TotalCapacity) + ")  " + ProgressBar(node.UsedCapacity, node.TotalCapacity))
            current_line += 1
            for bs in node.BSList.values():
                win.addstr(current_line, 4, "        block%-3d"%bs.ServiceID + "  (" + libsf.HumanizeDecimal(bs.UsedCapacity) + "/" + libsf.HumanizeDecimal(bs.TotalSize) + ")  " + ProgressBar(bs.UsedCapacity, bs.TotalSize))
                current_line += 1
        win.refresh()
        time.sleep(10)


def ProgressBar(full, total):
    barlength = 40
    if (total > 0):
       percent = int(round((float(full) * 100) / float(total)))
    else:
        percent = 0
    pad = int(percent/(100.0/float(barlength)))
    bar = "[" + "#"*pad + "-"*(barlength-pad) + "]"
    bar = bar + " " + str(percent) + "%"
    return bar

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    try:
        # Parse command line arguments
        parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
        parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the management IP of the cluster")
        parser.add_option("-u", "--user", type="string", dest="username", default=sfdefaults.username, help="the admin account for the cluster")
        parser.add_option("-p", "--pass", type="string", dest="password", default=sfdefaults.password, help="the admin password for the cluster")
        (options, extra_args) = parser.parse_args()

        mvip = options.mvip
        username = options.username
        password = options.password
        if not libsf.IsValidIpv4Address(mvip):
            mylog.error("'" + str(mvip) + "' does not appear to be a valid MVIP")
            sys.exit(1)

        curses.wrapper(main)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

