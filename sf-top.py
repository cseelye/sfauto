#!/usr/bin/env python2.7
"""
Monitor a SolidFire cluster, including cluster stats, process resource utilization, etc.

This script will display a table of information from each node in the cluster, as well as cluster wide information.
Node information includes CPU, memory and disk usage, process uptime, network traffic, etc.
Cluster information includes the number of objects (accounts, volumes, sessions, etc), capacity stats, GC info, syncing, ssload, scache usage, cluster faults, etc.

All of the information can be logged to CSV files as well using the --export flag
Collection intervals can be changed with --interval and --cluster_interval
Display size can be changed with --columns and --compact
"""

# cover a couple different ways of doing this
__version__ = '2.7'
VERSION = __version__
version = __version__

from optparse import OptionParser
import socket
import re
import time
import datetime
import calendar
import platform
import math
import os
import urllib2
import random
import httplib
import json
import sys
import tarfile
import traceback
import multiprocessing
import signal
import copy
import inspect
import BaseHTTPServer
import ssl
import lib.sfdefaults as sfdefaults
import lib.libsf as libsf

missing_modules = []
try:
    import paramiko
except ImportError:
    missing_modules.append("paramiko")
try:
    import colorconsole
    from colorconsole import terminal
except ImportError:
    missing_modules.append("colorconsole")

if len(missing_modules) > 0:
    print "You are missing the following required python modules:"
    for mod in missing_modules:
        print "  " + mod
    print "Here are some suggestions --"
    print "Ubuntu: sudo apt-get install python-dev python-setuptools; sudo easy_install <module_name>"
    print "Windows: easy_install <module name> or pypm install <module name>"
    print "    You may also need to install pycrypto from here: http://www.voidspace.org.uk/python/modules.shtml#pycrypto"
    print "MacOS: sudo easy_install <module_name>"
    exit(1)

import logging
logger = paramiko.util.logging.getLogger()
logger.setLevel(logging.FATAL)

class LocalTimezone(datetime.tzinfo):
    STDOFFSET = datetime.timedelta(seconds = -time.timezone)

    if time.daylight:
        DSTOFFSET = datetime.timedelta(seconds = -time.altzone)
    else:
        DSTOFFSET = STDOFFSET

    DSTDIFF = DSTOFFSET - STDOFFSET

    def utcoffset(self, dt):
        if self._isdst(dt):
            return LocalTimezone.DSTOFFSET
        else:
            return LocalTimezone.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return LocalTimezone.DSTDIFF
        else:
            return datetime.timedelta(0)

    def tzname(self, dt):
        return time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = time.mktime(tt)
        tt = time.localtime(stamp)
        return tt.tm_isdst > 0

class UTCTimezone(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)

class NodeInfo:
    def __init__(self):
        self.Timestamp = time.time()
        self.Hostname = 'Unknown'
        self.SfVersion = dict()
        self.SfVersionStringRaw = 'Unknown'
        self.TotalMemory = 0
        self.UsedMemory = 0
        self.CacheMemory = 0
        self.TotalCpu = 0.0
        self.CpuDetail = 'Unknown'
        self.CoresSinceStart = 0
        self.CoresTotal = 0
        self.NodeId = -1
        self.NvramMounted = False
        self.NvramDevice = ""
        self.EnsembleNode = False
        self.ClusterMaster = False
        self.Processes = dict()
        self.Nics = dict()
        self.Uptime = 0
        self.EnsembleLeader = False
        self.NodeType = 'Unknown'
        self.SlabMemory = 0
        self.FcHbas = dict()

class ProcessResourceUsage:
    def __init__(self):
        self.Pid = 0
        self.ProcessName = 'Unknown'
        self.ResidentMemory = 0
        self.PrivateMemory = 0
        self.SharedMemory = 0
        self.CpuUsage = 0
        self.Uptime = 0
        self.DiskDevice = None
        self.ReadThroughput = None
        self.WriteThroughput = None
        self.DeviceReadThroughput = None
        self.DeviceWriteThroughput = None

class SliceInfo:
    def __init__(self):
        self.ServiceId = 0
        self.SSLoad = 0
        self.PriCacheBytes = 0
        self.SecCacheBytes = 0
        self.FlusherThroughput = 0
        self.PreviousSecCacheBytes = -1


class NicResourceUsage:
    def __init__(self):
        self.Name = 'Unknown'
        self.MacAddress = 'Unknown'
        self.IpAddress = 'Unknown'
        self.TxThroughput = 0
        self.RxThroughput = 0
        self.TxBytes = 0
        self.RxBytes = 0
        self.TxPackets = 0
        self.RxPackets = 0
        self.TxDropped = 0
        self.RxDropped = 0
        self.Mtu = 0
        self.Up = False

class FcHbaResourceUsage:
    def __init__(self):
        self.Host = 'Unknown'
        self.Model = 'Unknown'
        self.PortWWN = 'Unknown'
        self.LinkState = 'Unknown'
        self.LinkSpeed = 'Unknown'
        self.TxFrames = 0
        self.RxFrames = 0
        self.TxFrameThroughput = 0
        self.RxFrameThroughput = 0

class ClusterInfo:
    def __init__(self):
        self.Timestamp = time.time()
        self.ClusterName = 'Unknown'
        self.Mvip = 'Unknown'
        self.Svip = 'Unknown'
        self.UniqueId = 'Unknown'
        self.ClusterMaster = 'Unknown'
        self.VolumeCount = 0
        self.SessionCount = 0
        self.AccountCount = 0
        self.BSCount = 0
        self.SSCount = 0
        self.CurrentIops = 0
        self.UsedSpace = 0
        self.TotalSpace = 0
        self.ProvisionedSpace = 0
        self.ClusterFullThreshold = sys.maxint
        self.DedupPercent = 0
        self.CompressionPercent = 0
        self.SameSoftwareVersions = True
        self.DebugSoftware = False
        self.NvramNotMounted = []
        self.NvramRamdrive = []
        self.OldCores = []
        self.NewCores = []
        self.SliceSyncing = "No"
        self.BinSyncing = "No"
        self.ClusterFaults = []
        self.WhitelistClusterFaults = []
        self.ClusterFaultsWarn = []
        self.ClusterFaultsError = []
        self.ClusterFaultsCrit = []
        self.NewEvents = []
        self.OldEvents = []
        self.LastGcStart = 0
        self.LastGcEnd = 0
        self.LastGcDiscarded = 0
        self.SliceServices = dict()
        self.SfMajorVersion = 0
        self.CustomBinary = False
        self.ExpectedBSCount = 0
        self.ExpectedSSCount = 0
        self.AvailableDriveCount = 0
        self.FailedDriveCount = 0
        self.NodeCount = 0
        self.LastGc = GCInfo()
        self.ClusterRepCount = 2
        self.NVRAMFaultsWarn = []
        self.NVRAMFaultsError = []
        self.MultipleMvips = False
        self.EnsembleLeader = 'Unknown'

class GCInfo(object):
    """
    Data structure containing information about a GC cycle
    """
    def __init__(self):
        self.StartTime = 0
        self.EndTime = 0
        self.DiscardedBytes = 0
        self.Rescheduled = False
        self.Generation = 0
        self.ParticipatingSSSet = set()
        self.EligibleBSSet = set()
        self.CompletedBSSet = set()

class ConsoleColors:
# Background colors
    if (platform.system().lower() == 'windows'):
        CyanBack = 11
    else:
        CyanBack = 6
    if (platform.system().lower() == 'windows'):
        BlueBack = 9
    else:
        BlueBack = 4
    if (platform.system().lower() == 'windows'):
        RedBack = 12
    else:
        RedBack = 1
    if (platform.system().lower() == 'windows'):
        YellowBack = 14
    else:
        YellowBack = 3
    if (platform.system().lower() == 'windows'):
        PurpleBack = 11
    else:
        PurpleBack = 5
    LightGreyBack = 7
    GreenBack = 2
    BlackBack = 0
    PurpleBack = 5
# Foreground colors
    BlackFore = 0
    PinkFore = 13
    PurpleFore = 5
    WhiteFore = 15
    GreenFore = 10
    LightGreyFore = 7
    if (platform.system().lower() == 'windows'):
        CyanFore = 11
    else:
        CyanFore = 14
    if (platform.system().lower() == 'windows'):
        RedFore = 12
    else:
        RedFore = 9
    if (platform.system().lower() == 'windows'):
        YellowFore = 14
    else:
        YellowFore = 11

def HttpRequest(log, pUrl, pUsername, pPassword):
    if (pUsername != None):
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, pUrl, pUsername, pPassword)
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        opener = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    log.debug("Retreiving " + pUrl)
    response = None
    try:
        response = urllib2.urlopen(pUrl, None, 5 * 60)
    except urllib2.HTTPError as e:
        if (e.code == 401):
            print "Invalid cluster admin/password"
            sys.exit(1)
        else:
            if (e.code in BaseHTTPServer.BaseHTTPRequestHandler.responses):
                log.debug("HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code]))
                return None
            else:
                log.debug("HTTPError: " + str(e.code))
                return None
    except socket.error as e:
        log.debug("Failed HTTP request - socket error on " + pUrl + " : " + str(e))
        return None
    except urllib2.URLError as e:
        log.debug("URLError on " + rpc_url + " : " + str(e.reason))
        return None
    except httplib.BadStatusLine, e:
        log.debug("httplib.BadStatusLine: " + str(e))
        return None
    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.debug("Unhandled exception in HttpRequest: " + str(e) + " - " + traceback.format_exc())
        return None

    return response.read()

def CallApiMethod(log, pMvip, pUsername, pPassword, pMethodName, pMethodParams, version=1.0, port=443, timeout=3, printErrors=False):
    rpc_url = 'https://' + pMvip + ':' + str(port) + '/json-rpc/' + ("%1.1f" % version)
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, rpc_url, pUsername, pPassword)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)
    context = None
    try:
        # pylint: disable=no-member
        context = ssl._create_unverified_context()
        # pylint: enable=no-member
    except AttributeError:
        pass

    api_call = json.dumps( { 'method': pMethodName, 'params': pMethodParams, 'id': random.randint(100, 1000) } )
    log.debug("Calling " + api_call + " on " + rpc_url)
    response_obj = None
    api_resp = None
    try:
        if context:
            # pylint: disable=unexpected-keyword-arg
            api_resp = urllib2.urlopen(rpc_url, api_call, timeout * 60, context=context)
            # pylint: enable=unexpected-keyword-arg
        else:
            api_resp = urllib2.urlopen(rpc_url, api_call, timeout * 60)
    except urllib2.HTTPError as e:
        if e.code == 401:
            print "Invalid cluster admin/password"
            if sys.version_info[0:3] == (2,7,9):
                print "Sorry, this script does not work on python 2.7.9"
            sys.exit(1)
        else:
            if (e.code in BaseHTTPServer.BaseHTTPRequestHandler.responses):
                message = "Failed calling " + pMethodName + " - HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code])
            else:
                message = "Failed calling " + pMethodName + " - HTTPError: " + str(e.code)
            if printErrors:
                print message
            log.debug(message)
            return None
    except socket.error as e:
        message = "Failed calling " + pMethodName + " - socket error on " + pMvip + " : " + str(e)
        if printErrors:
            print message
        log.debug(message)
        return None
    except urllib2.URLError as e:
        message = "Failed calling " + pMethodName + " - URLError on " + rpc_url + " : " + str(e.reason)
        if printErrors:
            print message
        log.debug(message)
        return None
    except httplib.BadStatusLine as e:
        message = "Failed calling " + pMethodName + " - httplib.BadStatusLine: " + str(e)
        if printErrors:
            print message
        log.debug(message)
        return None
    except KeyboardInterrupt:
        raise
    except Exception as e:
        message = "Failed calling " + pMethodName + " - Unhandled exception in CallApiMethod: " + str(e) + " - " + traceback.format_exc()
        if printErrors:
            print message
        log.debug(message)
        return None

    if (api_resp != None):
        response_str = api_resp.read().decode('ascii')
        #log.debug(response_str)
        try:
            response_obj = json.loads(response_str)
        except ValueError:
            message = "Invalid JSON received from cluster"
            if printErrors:
                print message
            log.debug(message)
            return None

    if (response_obj == None or 'error' in response_obj):
        log.debug("Missing or error response from cluster")
        if 'error' in response_obj:
            log.debug(response_str)
            if printErrors:
                print response_str
        return None

    return response_obj['result']

def GetNodeInfo(log, pNodeIp, pNodeUser, pNodePass, pKeyFile=None):

    if not pKeyFile or not os.path.exists(pKeyFile): pKeyFile = None

    log.debug("Connecting to " + str(pNodeIp) + " user " + str(pNodeUser) + " pass " + str(pNodePass) + " keyfile " + str(pKeyFile))
    begin = datetime.datetime.now()
    #start_time = datetime.datetime.now()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    try:
        ssh.connect(pNodeIp, username=pNodeUser, password=pNodePass, key_filename=pKeyFile)
    except socket.error as e:
        log.debug(pNodeIp + " - Could not connect: " + str(e))
        return None
    except paramiko.BadAuthenticationType:
        message = pNodeIp + " - You must use SSH host keys to connect to this node (try adding your key to the node, or disabling OTP)"
        log.debug(message)
        print message
        sys.exit(1)
    except paramiko.AuthenticationException:
        try:
            ssh.connect(pNodeIp, username=pNodeUser, password=pNodePass)
        except paramiko.AuthenticationException:
            message =  pNodeIp + " - Authentication failed. Check the password or RSA key"
            log.debug(message)
            print message
            sys.exit(1)
    except paramiko.SSHException as e:
        message = pNodeIp + " - Could not connect: " + str(e)
        log.debug(message)
        print message
        sys.exit(1)

    #time_connect = datetime.datetime.now() - start_time
    #time_connect = time_connect.microseconds + time_connect.seconds * 1000000

    usage = NodeInfo()

    #
    # Get the node type, slab mem usage, hostname, sf version, node memory usage, core files, mounts
    #
    #start_time = datetime.datetime.now()
    start_timestamp = TimestampToStr(START_TIME, "%Y%m%d%H%M.%S", UTCTimezone())
    command = ""
    command += "sudo /sf/bin/sfplatform | \\grep NODE_TYPE"
    command += ";\\cat /proc/meminfo | \\grep Slab"
    command += ";echo hostname=`\\hostname`"
    command += ";/sf/bin/sfapp --Version -laAll 0"
    command += ";\\free -o"
    command += ";touch -t " + start_timestamp + " /tmp/timestamp;echo newcores=`find /sf -maxdepth 1 -name \"core*\" -newer /tmp/timestamp | wc -l`"
    command += ";echo allcores=`ls -1 /sf/core* | wc -l`"
    command += ";sudo \\cat /proc/mounts | \\egrep '^/dev|pendingDirtyBlocks'"
    command += ";sudo grep nodeID /etc/solidfire.json"
    ver_string = ""
    volumes = dict()
    #log.debug(command)
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    #log.debug("".join(data))
    for line in data:
        m = re.search(r'^NODE_TYPE=(.+)', line)
        if m:
            usage.NodeType = m.group(1)
            continue
        m = re.search(r'^Slab:\s+(\d+)', line)
        if m:
            usage.SlabMemory = int(m.group(1)) * 1024
            continue
        m = re.search(r'^hostname=(.+)', line)
        if (m):
            usage.Hostname = m.group(1)
            continue
        m = re.search(r'^sfapp', line)
        if (m):
            ver_string = line
            continue
        m = re.search(r'^Mem:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
        if (m):
            usage.TotalMemory = int(m.group(1)) * 1024
            used = int(m.group(2)) * 1024
            cache = int(m.group(6)) * 1024
            usage.UsedMemory = used - cache
            usage.CacheMemory = cache
            continue
        m = re.search(r'newcores=(\d+)', line)
        if (m):
            usage.CoresSinceStart = int(m.group(1))
            continue
        m = re.search(r'allcores=(\d+)', line)
        if (m):
            usage.CoresTotal = int(m.group(1))
            continue
        if line.startswith("/dev") or "pendingDirtyBlocks" in line:
            pieces = line.split()
            if pieces[0].startswith("/dev"):
                volumes[pieces[1]] = pieces[0].split("/")[-1]
            else:
                volumes[pieces[1]] = pieces[0]
            continue
        m = re.search(r'nodeID" : (\d+)', line)
        if (m):
            usage.NodeId = int(m.group(1))
            continue

    if "/mnt/pendingDirtyBlocks" in volumes:
        usage.NvramMounted = True
        usage.NvramDevice = volumes["/mnt/pendingDirtyBlocks"]
    else:
        usage.NvramMounted = False

    # sfapp Release UC Version: 4.06 sfdev: 1.90 Revision: f13bebca8736 Build date: 2011-12-20 10:02
    # sfapp Debug Version: 4.08 sfdev: 1.91 Revision: 58220b8bd90a Build date: 2012-01-07 14:28 Tag: TSIP4v1
    # sfapp BuildType=Release UC Release=lithium Version=3.45 sfdev=1.92 Revision=a7ca42f12f32 BuildDate=2012-02-13@12:20
    # sfapp BuildType=Release,UC Release=lithium Version=3.49 sfdev=1.93 Revision=80c48b6aab2f BuildDate=2012-02-21@14:22
    # sfapp BuildType=Release Element=boron Release=boron ReleaseShort=boron Version=5.1256 sfdev=5.18-p3 Repository=boron Revision=b2eb203dfaf6 BuildDate=2013-09-10@17:28 md5=d1e1ebf84d4f6de7293be55fed59e39b
    # sfapp BuildType=Release,UC,CXXFLAGS Element=carbon Release=carbon-joe-merklesyncing ReleaseShort=carbon-joe-merklesyncing Version=6.325 sfdev=6.21 Repository=joe-merklesyncing Revision=80614a6c4dfc BuildDate=2014-01-28@08:56 Tag='[joe-merklesyncing]' CXXFLAGS=-Wno-unused-local-typedefs md5=8f0e4bf47a994d5e982155e301f6bc86
    usage.SfVersionStringRaw = ver_string
    ver_info = {}
    parts = re.split("\s+", ver_string)
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=")
        usage.SfVersion[key] = value.strip("'")

    #time_ver = datetime.datetime.now() - start_time
    #time_ver = time_ver.microseconds + time_ver.seconds * 1000000

    #
    # Get a list of sf processes from 'ps'
    #
    #start_time = datetime.datetime.now()
    process_names2pids = dict()
    process_pids2names = dict()
    process_pids2disks = dict()
    stdin, stdout, stderr = ssh.exec_command("sudo \\ps -eo comm,pid,args --no-headers | \\egrep '^fibre|^iscsid|^dsm_sa_snmpd|^bulkvolume|^block|^slice|^master|^service_manager|^sfnetwd|^sfconfig|^java.+zookeeper'")
    data = stdout.readlines()
    for line in data:
        m = re.search(r'(\S+)\s+(\d+).+localdisk=(\S+)', line)
        if (m):
            name = m.group(1)
            pid = m.group(2)
            disk = m.group(3)
            if (name == 'java'):
                name = 'zookeeper'
            process_names2pids[name] = int(pid)
            process_pids2names[int(pid)] = name
            process_pids2disks[int(pid)] = disk
            #print "  " + name + " => " + pid
            continue
        m = re.search(r'(\S+)\s+(\d+)', line)
        if (m):
            name = m.group(1)
            pid = m.group(2)
            if (name == 'java'):
                name = 'zookeeper'
                usage.EnsembleNode = True
            process_names2pids[name] = int(pid)
            process_pids2names[int(pid)] = name
    #time_ps = datetime.datetime.now() - start_time
    #time_ps = time_ps.microseconds + time_ps.seconds * 1000000

    #
    # Fill in some basic info
    #
    for pid, name in process_pids2names.iteritems():
        proc = ProcessResourceUsage()
        proc.Pid = pid
        proc.ProcessName = name
        if pid in process_pids2disks.keys():
            proc.DiskDevice = process_pids2disks[pid]
            if proc.DiskDevice.startswith("/dev"):
                proc.DiskDevice = proc.DiskDevice.split("/")[-1]
            elif proc.DiskDevice.startswith("/mnt"):
                proc.DiskDevice = volumes[proc.DiskDevice]
            else:
                proc.DiskDevice = None
        usage.Processes[pid] = proc

    #
    # Run 'top', 'ifconfig', grep /proc/diskstatus, grep /proc/[pid]/io.
    # These all need multiple samples separated by a wait, so put them together and only wait once
    #
    #start_time = datetime.datetime.now()
    sample_interval = 2
    base_fc_path = "/sys/class/fc_host"
    command = ""
    command += "\\ifconfig | \\egrep -i 'eth|bond|lo|inet|RX bytes';"
    command += "sudo \\grep sd /proc/diskstats"
    if len(usage.Processes.keys()) > 0:
        command += ";sudo \\grep bytes"
        for pid in usage.Processes.iterkeys():
            command += " /proc/" + str(pid) + "/io"
    command += ";"
    if usage.NodeType == "FC0025" or usage.NodeType == "SFFC":
        command += "for host in `ls " + base_fc_path + "`; do echo \"HOST='$host' MODEL='`cat " + base_fc_path + "/$host/device/scsi_host/$host/model_name`' WWN='`cat " + base_fc_path + "/$host/device/fc_host/$host/port_name`' LINK_STATE='`cat " + base_fc_path + "/$host/device/scsi_host/$host/link_state`' LINK_SPEED='`cat " + base_fc_path + "/$host/speed`' TX_FRAMES='`cat " + base_fc_path + "/$host/statistics/tx_frames`' RX_FRAMES='`cat " + base_fc_path + "/$host/statistics/rx_frames`'\"; done;"
    command += "sudo \\top -b -d " + str(sample_interval) + " -n 2;"
    command += "\\ifconfig | \\egrep -i 'eth|bond|lo|inet|RX bytes|dropped|MTU';"
    if usage.NodeType == "FC0025" or usage.NodeType == "SFFC":
        command += "for host in `ls " + base_fc_path + "`; do echo \"HOST='$host' MODEL='`cat " + base_fc_path + "/$host/device/scsi_host/$host/model_name`' WWN='`cat " + base_fc_path + "/$host/device/fc_host/$host/port_name`' LINK_STATE='`cat " + base_fc_path + "/$host/device/scsi_host/$host/link_state`' LINK_SPEED='`cat " + base_fc_path + "/$host/speed`' TX_FRAMES='`cat " + base_fc_path + "/$host/statistics/tx_frames`' RX_FRAMES='`cat " + base_fc_path + "/$host/statistics/rx_frames`'\"; done;"
    command += "sudo \\grep sd /proc/diskstats"
    if len(usage.Processes.keys()) > 0:
        command += ";sudo \\grep bytes"
        for pid in usage.Processes.iterkeys():
            command += " /proc/" + str(pid) + "/io"
    #log.debug(command)
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    #log.debug("".join(data))
    current_nic_name = ""
    #current_nic = NicResourceUsage()
    top_frame = 0
    for line in data:

        # Parse 'ifconfig' lines
        m = re.search(r'(\S+)\s+Link.+HWaddr (.+)', line)
        if (m):
            current_nic_name = m.group(1)
            if (current_nic_name not in usage.Nics.keys()):
                nic = NicResourceUsage()
                nic.Name = current_nic_name
                nic.MacAddress = m.group(2).strip()
                usage.Nics[current_nic_name] = nic
            continue
        # This catches the loopback adapter
        m = re.search(r'(\S+)\s+Link', line)
        if (m and current_nic_name):
            current_nic_name = m.group(1)
            if (current_nic_name not in usage.Nics.keys()):
                nic = NicResourceUsage()
                nic.Name = current_nic_name
                nic.MacAddress = "Loopback"
                usage.Nics[current_nic_name] = nic
            continue
        m = re.search(r'addr:(.+?) .+Mask', line)
        if (m and current_nic_name):
            usage.Nics[current_nic_name].IpAddress = m.group(1)
            continue
        m = re.search(r'RX bytes:(\d+).+TX bytes:(\d+)', line)
        if (m and current_nic_name):
            if (usage.Nics[current_nic_name].RxBytes > 0):
                usage.Nics[current_nic_name].RxThroughput = (float(m.group(1)) - float(usage.Nics[current_nic_name].RxBytes)) / float(sample_interval)
                usage.Nics[current_nic_name].TxThroughput = (float(m.group(2)) - float(usage.Nics[current_nic_name].TxBytes)) / float(sample_interval)
            usage.Nics[current_nic_name].RxBytes = int(m.group(1))
            usage.Nics[current_nic_name].TxBytes = int(m.group(2))
            continue
        m = re.search(r'RX packets:(\d+).+dropped:(\d+)', line)
        if (m and current_nic_name):
            usage.Nics[current_nic_name].RxPackets = int(m.group(1))
            usage.Nics[current_nic_name].RxDropped = int(m.group(2))
            continue
        m = re.search(r'TX packets:(\d+).+dropped:(\d+)', line)
        if (m and current_nic_name):
            usage.Nics[current_nic_name].TxPackets = int(m.group(1))
            usage.Nics[current_nic_name].TxDropped = int(m.group(2))
            continue
        m = re.search(r'MTU:(\d+)', line)
        if (m and current_nic_name):
            usage.Nics[current_nic_name].Mtu = int(m.group(1))
            if "UP" in line:
                usage.Nics[current_nic_name].Up = True
            continue

        # Parse FC HBA lines
        m = re.search(r"HOST='(.+?)'\s+MODEL='(.+?)'\s+WWN='(.+?)'\s+LINK_STATE='(.+?)'\sLINK_SPEED='(.+?)'\s+TX_FRAMES='(.+?)'\s+RX_FRAMES='(.+?)'", line)
        if m:
            fc_host = m.group(1)
            if fc_host not in usage.FcHbas.keys():
                fc = FcHbaResourceUsage()
                fc.Model = m.group(2)
                # convert WWN to nicer format
                ugly_wwn = m.group(3)
                pretty_wwn = ''
                for i in range(2, 2*8+2, 2):
                    pretty_wwn += ':' + ugly_wwn[i:i+2]
                fc.PortWWN = pretty_wwn[1:]
                link_state = m.group(4)
                if "-" in link_state:
                    link_state = link_state[:-link_state.index("-")-1]
                fc.LinkState = link_state
                fc.LinkSpeed = m.group(5).replace("Gbit", "Gb")
                fc.TxFrames = int(m.group(6), 16)
                fc.RxFrames = int(m.group(7), 16)
                usage.FcHbas[fc_host] = fc
            else:
                usage.FcHbas[fc_host].TxFrameThroughput = float(int(m.group(6), 16) - usage.FcHbas[fc_host].TxFrames) / sample_interval
                usage.FcHbas[fc_host].RxFrameThroughput = float(int(m.group(7), 16) - usage.FcHbas[fc_host].RxFrames) / sample_interval
                usage.FcHbas[fc_host].TxFrames = int(m.group(6), 16)
                usage.FcHbas[fc_host].RxFrames = int(m.group(7), 16)

        # Parse diskstatus, io lines
        # /proc/diskstats has lines like this for each device/partition:
        #   8       3 sda3 1052 416 26360 570 242607268 50613976 3563502857 1523016620 0 18710830 1522894680
        #   8      16 sdb 785947718 27 2832720279 167863620 3239349 1269702 2326835507 16232680 0 94778360 183432230
        #
        # /proc/[pid]/io has lines like this for read/write (filename prepended by grep)
        #/proc/29717/io:read_bytes: 16384
        #/proc/29717/io:write_bytes: 6990434292224
        m = re.search(r'^/proc/(\d+)/io:read_bytes:\s+(\d+)', line)
        if (m):
            pid = int(m.group(1))
            read_bytes = int(m.group(2))
            if (usage.Processes[pid].ReadThroughput == None):
                usage.Processes[pid].ReadThroughput = read_bytes
            else:
                usage.Processes[pid].ReadThroughput = (read_bytes - usage.Processes[pid].ReadThroughput) / sample_interval
            continue
        m = re.search(r'^/proc/(\d+)/io:write_bytes:\s+(\d+)', line)
        if (m):
            pid = int(m.group(1))
            write_bytes = int(m.group(2))
            if (usage.Processes[pid].WriteThroughput == None):
                usage.Processes[pid].WriteThroughput = write_bytes
            else:
                usage.Processes[pid].WriteThroughput = (write_bytes - usage.Processes[pid].WriteThroughput) / sample_interval
            continue
        m = re.search(r'\s+\d+\s+\d+\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
        if (m):
            device_name = m.group(1)
            if (re.search(r'\d$', device_name)):
                read_index = 2
                write_index = 4
            else:
                read_index = 3
                write_index = 7
            device_read = int(m.group(read_index)) * 512
            device_write = int(m.group(write_index)) * 512
            for pid in usage.Processes.keys():
                if (usage.Processes[pid].DiskDevice == device_name):
                    if (usage.Processes[pid].DeviceReadThroughput == None):
                        usage.Processes[pid].DeviceReadThroughput = device_read
                        usage.Processes[pid].DeviceWriteThroughput = device_write
                    else:
                        usage.Processes[pid].DeviceReadThroughput = (device_read - usage.Processes[pid].DeviceReadThroughput) / sample_interval
                        usage.Processes[pid].DeviceWriteThroughput = (device_write - usage.Processes[pid].DeviceWriteThroughput) / sample_interval
                    break
            continue

        # Parse 'top' lines
        m = re.search(r'Cpu\(', line)
        if (m):
            top_frame += 1
        if (top_frame >= 2):
            # Parse total CPU info out of 'top'
            # 'Cpu(s): 10.2%us,  5.8%sy,  0.0%ni, 77.6%id,  4.1%wa,  0.0%hi,  2.4%si,  0.0%st'
            m = re.search(r'Cpu\(s\):(.+)', line)
            if (m):
                usage.CpuDetail = m.group(1)
            m = re.search(r'(\d+\.\d+)%id', line)
            if (m):
                usage.TotalCpu = 100.0 - float(m.group(1))

            # Parse per-process CPU info out of 'top'
            # '  PID USER      PR  NI  VIRT  RES  SHR S %CPU %MEM    TIME+  COMMAND'
            # '30914 root      20   0 1265m  99m  17m S    0  0.1  52:57.63 master-1'
            # ' 6893 root      20   0 1677m 656m 4108 S    0  0.9   1995:43 block35'
            m = re.search(r'^\s*\d+\s+\w+\s+\d+\s+\d+\s+[\w\.]+\s+[\w\.]+\s+[\w\.]+\s+\w\s+\d+\s+\d+\.\d+\s+[0-9:\.-]+\s+[\w-]+', line)
            if (m):
                pieces = line.split()
                pid = int(pieces[0])
                cpu = int(pieces[8])
                name = pieces[11]

                keep = False
                if (pid in process_pids2names.keys()):
                    keep = True
                #if (cpu >= 10):
                #    keep = True
                if (not keep):
                    continue

                if (not pid):
                    continue
                if (not cpu):
                    cpu = 0
                if (not name):
                    name = ''

                if (name == 'java' and pid in process_pids2names.keys()):
                    name = 'zookeeper'

                if (pid in usage.Processes.keys()):
                    usage.Processes[pid].CpuUsage = cpu
                else:
                    proc = ProcessResourceUsage()
                    proc.Pid = pid
                    proc.ProcessName = name
                    proc.CpuUsage = cpu
                    usage.Processes[pid] = proc
    #time_top = datetime.datetime.now() - start_time
    #time_top = time_top.microseconds + time_top.seconds * 1000000

    for nic_name in usage.Nics.keys():
        if ":" in nic_name:
            usage.ClusterMaster = True
            break

    #
    # Get resident size from /proc/[pid]/status
    #
    #start_time = datetime.datetime.now()
    command = ""
    for pid in usage.Processes.iterkeys():
        command += "echo " + str(pid) + " = `sudo \\grep VmRSS /proc/" + str(pid) + "/status`;"
    command = command.strip(";")
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    for line in data:
        m = re.search(r"(\d+) = VmRSS: (\d+) kB", line)
        if (m):
            pid = int(m.group(1))
            rss = int(m.group(2)) * 1024
            usage.Processes[pid].ResidentMemory = rss
    #time_rss = datetime.datetime.now() - start_time
    #time_rss = time_rss.microseconds + time_rss.seconds * 1000000

    #
    # Get current time, boot time from /proc/stat
    #
    #start_time = datetime.datetime.now()
    current_time = 0
    system_boot_time = 0
    stdin, stdout, stderr = ssh.exec_command("echo CurrentDate=`date +%s`;sudo cat /proc/stat")
    data = stdout.readlines()
    for line in data:
        m = re.search(r"CurrentDate=(\d+)", line)
        if (m):
            current_time = int(m.group(1))
        m = re.search(r"btime\s+(\d+)", line)
        if (m):
            system_boot_time = int(m.group(1))
    usage.Uptime = current_time - system_boot_time
    #time_time = datetime.datetime.now() - start_time
    #time_time = time_time.microseconds + time_time.seconds * 1000000

    #
    # Get uptime for each process from /proc/[pid]/stat
    #
    #start_time = datetime.datetime.now()
    command = ""
    for pid in usage.Processes.keys():
        command += "sudo \\cat /proc/" + str(pid) + "/stat;"
    command = command.strip(";")
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    for line in data:
        pieces = line.split()
        pid = int(pieces[0])
        process_start_time = int(pieces[21])
        usage.Processes[pid].Uptime = current_time - system_boot_time - (process_start_time/100)
    #time_uptime = datetime.datetime.now() - start_time
    #time_uptime = time_uptime.microseconds + time_uptime.seconds * 1000000

    if usage.EnsembleNode:
        zk_pid = process_names2pids["zookeeper"]
        # Find the IP and port ZK is listening on
        command = "sudo lsof -np " + str(zk_pid) + " | grep TCP | grep LISTEN | grep -v '*' | awk '{print $9}' | sort | head -1 | tr ':' ' '"
        stdin, stdout, stderr = ssh.exec_command(command)
        data = stdout.readlines()
        if data:
            ip_port = data[0].strip()
            # Query the mode of this ZK node
            command = "echo stat | sudo nc " + ip_port + " | grep Mode | awk '{print $2}'"
            stdin, stdout, stderr = ssh.exec_command(command)
            data = stdout.readlines()
            if data:
                mode = data[0].strip()
                if "leader" in mode:
                    usage.EnsembleLeader = True

    #if usage.NodeType == "FC0025" or usage.NodeType == "SFFC":
    #    base_fc_path = "/sys/class/fc_host"
    #    command = "for host in `ls " + base_fc_path + "`; do echo \"HOST=$host MODEL=`cat " + base_fc_path + "/$host/device/scsi_host/$host/model_name` LINK_STATE=`cat " + base_fc_path + "/$host/device/scsi_host/$host/link_state` LINK_SPEED=`cat " + base_fc_path + "/$host/speed`  TX_FRAMES='`cat " + base_fc_path + "/$host/statistics/tx_frames`' RX_FRAMES='`cat " + base_fc_path + "/$host/statistics/rx_frames`'\"; done"



    ssh.close()
    time_total = datetime.datetime.now() - begin
    time_total = time_total.microseconds + time_total.seconds * 1000000

    return usage

def ParseDateTime(pTimeString):
    known_formats = [
        "%Y-%m-%d %H:%M:%S.%f",     # old sf format
        "%Y-%m-%dT%H:%M:%S.%fZ"     # ISO format with UTC timezone
    ]
    parsed = None
    for fmt in known_formats:
        try:
            parsed = datetime.datetime.strptime(pTimeString, fmt)
            break
        except ValueError: pass

    return parsed

def ParseTimestamp(pTimeString):
    date_obj = ParseDateTime(pTimeString)
    if (date_obj != None):
        return calendar.timegm(date_obj.timetuple())
    else:
        return 0

def TimestampToStr(pTimestamp, pFormatString = "%Y-%m-%d %H:%M:%S", pTimeZone = LocalTimezone()):
    display_time = datetime.datetime.fromtimestamp(pTimestamp, pTimeZone)
    return display_time.strftime(pFormatString)

def GetClusterInfo(log, pMvip, pApiUser, pApiPass, pNodesInfo, previousClusterInfo=None, faultWhitelist=None):

    info = ClusterInfo()

    # table of nodeID => name
    nodeID2nodeName = {}
    node_list = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListAllNodes', {})
    if node_list:
        for node in node_list["nodes"]:
            nodeID2nodeName[node["nodeID"]] = node["name"]

    log.debug("checking node info")
    info.NodeCount = len(pNodesInfo.keys())

    # Loop through the node objects looking for various things
    info.SfMajorVersion = 0
    ver = None
    mvip_count = 0
    info.ExpectedBSCount = 0
    info.ExpectedSSCount = 0
    for node_ip in pNodesInfo.keys():
        if pNodesInfo[node_ip] == None:
            continue
        node = pNodesInfo[node_ip]

        # Major version of sfapp
        if info.SfMajorVersion == 0:
            m = re.search(r'Version=(\d+\.\d+)', node.SfVersionStringRaw)
            if m:
                ver_str = m.group(1)
                pieces = ver_str.split(".")
                info.SfMajorVersion = int(pieces[0])

        # Look for different sfapp versions on the nodes
        if ver == None:
            ver = node.SfVersionStringRaw
        if node.SfVersionStringRaw != ver:
            info.SameSoftwareVersions = False

        # Look for debug builds
        if "Debug" in node.SfVersionStringRaw:
            info.DebugSoftware = True

        # Look for custom binaries
        if "Tag" in node.SfVersionStringRaw or "UC" in node.SfVersionStringRaw or "CXXFLAGS" in node.SfVersionStringRaw:
            info.CustomBinary = True

        # Check for nodes that have the MVIP online
        for nic_name in node.Nics.keys():
            if node.Nics[nic_name].IpAddress == pMvip:
                mvip_count += 1

        # Check for core files on nodes
        if node.CoresSinceStart > 0:
            info.NewCores.append(node.Hostname)
        elif node.CoresTotal - node.CoresSinceStart > 0:
            info.OldCores.append(node.Hostname)

        # Check NVRAM mount
        if not node.NvramMounted and node.NodeType != "FC0025" and node.NodeType != "SFFC":
            info.NvramNotMounted.append(node.Hostname)
        if "ram" in node.NvramDevice:
            info.NvramRamdrive.append(node.Hostname)

        # Expected service counts
        if node.NodeId > 0:
            drive_config = None
            if info.SfMajorVersion >= 6:
                drive_config = CallApiMethod(log, node_ip, pApiUser, pApiPass, "GetDriveConfig", {}, version=6.0, port=442)
                if drive_config:
                    info.ExpectedBSCount += drive_config["driveConfig"]["numBlockExpected"]
                    info.ExpectedSSCount += drive_config["driveConfig"]["numSliceExpected"]
            if not drive_config:
                info.ExpectedSSCount += 1
                info.ExpectedBSCount += 9
                if node.TotalMemory < 100 * 1024 * 1024 * 1024:
                    # SF3010 have one more BS drive than 6010 or 9010
                    info.ExpectedBSCount += 1

    log.debug("Expecting " + str(info.ExpectedBSCount) + " BServices and " + str(info.ExpectedSSCount) + " SServices")
    if mvip_count > 1:
        info.MultipleMvips = True


    log.debug("gathering cluster info")

    # Find the ensemble leader
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetEnsembleConfig', {"force" : True}, version=5.0)
    if result:
        for node in result["nodes"]:
            if node["result"]["serverStats"]["Mode"].lower() == "leader":
                info.EnsembleLeader = node["result"]["nodeInfo"]["name"]
                break

    # get basic cluster info
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetClusterInfo', {})
    if result is None:
        log.debug("Failed to get cluster info")
        return None
    info.ClusterName = result['clusterInfo']['name']
    info.Mvip = result['clusterInfo']['mvip']
    info.Svip = result['clusterInfo']['svip']
    info.UniqueId = result['clusterInfo']['uniqueID']
    info.ClusterRepCount = result['clusterInfo']['repCount']

    if result['clusterInfo']['mvipNodeID'] in nodeID2nodeName:
        info.ClusterMaster = nodeID2nodeName[result['clusterInfo']['mvipNodeID']]
    else:
        info.ClusterMaster = "nodeID " + str(result['clusterInfo']['mvipNodeID'])

    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetClusterCapacity', {})
    if result is None:
        log.debug("Failed to get cluster capacity")
    else:
        info.SessionCount = int(result["clusterCapacity"]["activeSessions"])
        info.CurrentIops = int(result["clusterCapacity"]["currentIOPS"])
        info.UsedSpace = int(result["clusterCapacity"]["usedSpace"])
        info.TotalSpace = int(result["clusterCapacity"]["maxUsedSpace"])
        info.ProvisionedSpace = int(result["clusterCapacity"]["provisionedSpace"])
        info.DedupPercent = int(result["clusterCapacity"]["deDuplicationPercent"])
        info.CompressionPercent = int(result["clusterCapacity"]["compressionPercent"])


    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListActiveVolumes', {}, timeout=10)
    if result is None:
        log.debug("Failed to get active volumes")
        info.VolumeCount = 0
    else:
        if "volumes" in result.keys() and result["volumes"] != None:
            info.VolumeCount = len(result["volumes"])
        else:
            info.VolumeCount = 0
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListDeletedVolumes', {}, timeout=10)
    if result is None:
        log.debug("Failed to get deleted volumes")
        info.VolumeCount = 0
    else:
        if "volumes" in result.keys() and result["volumes"] != None:
            info.VolumeCount += len(result["volumes"])


    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListServices', {})
    bs_count = 0
    ss_count = 0
    ts_count = 0
    vs_count = 0
    ms_count = 0
    if result is None:
        log.debug("Failed to get service list")
    else:
        for service in result["services"]:
            service_info = service["service"]
            service_type = service_info["serviceType"]
            if (service_type == "slice"): ss_count += 1
            elif (service_type == "block"): bs_count += 1
            elif (service_type == "transport"): ts_count += 1
            elif (service_type == "volume"): vs_count += 1
            elif (service_type == "master"): ms_count += 1

    info.BSCount = bs_count
    info.SSCount = ss_count
    info.VSCount = vs_count
    info.TSCount = ts_count
    info.MSCount = ms_count

    if info.SfMajorVersion == 3 and info.TotalSpace > 0:
        full = info.TotalSpace - (3600 * 1000 * 1000 * 1000)
        # account for binary/si mismatch in solidfire calculation in Be and earlier
        full = int(float(full) / float(1024*1024*1024*1024) * float(1000*1000*1000*1000))
        info.ClusterFullThreshold = full
    elif info.SfMajorVersion >= 4:
        result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetClusterFullThreshold', {})
        if result:
            info.ClusterFullThreshold = result["fullness"]
        else:
            info.ClusterFullThreshold = ""

    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListAccounts', {})
    if result is None:
        log.debug("Failed to get account list")
    else:
        if "accounts" in result.keys() and result["accounts"] != None:
            info.AccountCount = len(result["accounts"])
        else:
            info.AccountCount = 0

    # Get the list of current faults
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, "ListClusterFaults", {"exceptions": 1, "faultTypes": "current"})
    if result is None:
        log.debug("Failed to get cluster faults")
    else:
        current_faults = set()
        for fault in result["faults"]:
            if fault["code"] == "nodeHardwareFault" and "NVRAM" in fault["details"]:
                m = re.search("{(.+)}", fault["details"])
                if m:
                    if fault["nodeID"] in nodeID2nodeName:
                        nv_fault = nodeID2nodeName[fault["nodeID"]] + " NVRAM " + m.group(1)
                    else:
                        nv_fault = "NodeID " + str(fault["nodeID"]) + " NVRAM " + m.group(1)
                    if fault["severity"] == "warning":
                        info.NVRAMFaultsWarn.append(nv_fault)
                    else:
                        info.NVRAMFaultsError.append(nv_fault)

            if fault["code"] in faultWhitelist:
                if fault["code"] not in info.WhitelistClusterFaults:
                    info.WhitelistClusterFaults.append(fault["code"])
                continue
            if fault["severity"] == "warning" and fault["code"] not in info.ClusterFaultsWarn:
                info.ClusterFaultsWarn.append(fault["code"])
                continue
            if fault["severity"] == "error" and fault["code"] not in info.ClusterFaultsError:
                info.ClusterFaultsError.append(fault["code"])
                continue
            if fault["severity"] == "critical" and fault["code"] not in info.ClusterFaultsCrit:
                info.ClusterFaultsCrit.append(fault["code"])
                continue

    # Check for slice syncing
    info.SliceSyncing = "No"
    if info.SfMajorVersion >= 5:
        # Get the slice assignments report
        result = HttpRequest(log, "https://" + pMvip + "/reports/slices.json", pApiUser, pApiPass)
        if result:
            slice_report = json.loads(result)

            # Make sure there are no unhealthy services
            if "services" in slice_report:
                for ss in slice_report["services"]:
                    if ss["health"] != "good":
                        log.debug("Slice sync - one or more SS are unhealthy")
                        info.SliceSyncing = "Yes"
                        break

            # Make sure there are no volumes with multiple live secondaries or any dead secondaries
            if "slices" in slice_report:
                for vol in slice_report["slices"]:
                    if "liveSecondaries" not in vol:
                        log.debug("Slice sync - one or more volumes have no live secondaries")
                        info.SliceSyncing = "Yes"
                        break
                    if len(vol["liveSecondaries"]) > 1:
                        log.debug("Slice sync - one or more volumes have multiple live secondaries")
                        info.SliceSyncing = "Yes"
                        break
                    if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                        log.debug("Slice sync - one or more volumes have dead secondaries")
                        info.SliceSyncing = "Yes"
                        break
            if "slice" in slice_report:
                for vol in slice_report["slice"]:
                    if "liveSecondaries" not in vol:
                        log.debug("Slice sync - one or more volumes have no live secondaries")
                        info.SliceSyncing = "Yes"
                        break
                    if len(vol["liveSecondaries"]) > 1:
                        log.debug("Slice sync - one or more volumes have multiple live secondaries")
                        info.SliceSyncing = "Yes"
                        break
                    if "deadSecondaries" in vol and len(vol["deadSecondaries"]) > 0:
                        log.debug("Slice sync - one or more volumes have dead secondaries")
                        info.SliceSyncing = "Yes"
                        break
    else:
        sync_html = HttpRequest(log, "https://" + pMvip + "/reports/slicesyncing", pApiUser, pApiPass)
        if (sync_html != None and "table" in sync_html):
            info.SliceSyncing = "Yes"

    # Check for bin syncing
    sync_html = HttpRequest(log, "https://" + pMvip + "/reports/binsyncing", pApiUser, pApiPass)
    if (sync_html != None and "table" in sync_html):
        complete = 0
        total = 0
        max_time = 0
        for match in re.finditer(r"(\d+)/(\d+) bins complete.+% (\d+) sec remaining", sync_html):
            bs_complete = int(match.group(1))
            bs_total = int(match.group(2))
            complete += bs_complete
            total += bs_total
            bs_time = int(match.group(3))
            if (bs_time > max_time):
                max_time = bs_time
        if (total > 0):
            sync_pct = 100 * float(complete) / float(total)
        else:
            sync_pct = 0
        info.BinSyncing = "%0.1f%% (%ds)" % (sync_pct, max_time)
    else:
        info.BinSyncing = "No"

    # Check for events in the eventlog
    event_list = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListEvents', {})
    if event_list is None:
        log.debug("failed to get event list")
    else:
        # Look for some known BAD events
        for i in range(len(event_list['events'])):
            event = event_list['events'][i]
            if 'xUnknownBlockID' in event['message']:
                event_time = ParseTimestamp(event['timeOfReport'])
                if (event_time == None):
                    info.NewEvents.append("xUnknownBlockID")
                elif (START_TIME < event_time and 'xUnknownBlockID' not in info.NewEvents):
                    info.NewEvents.append("xUnknownBlockID")
                elif ('xUnknownBlockID' not in info.OldEvents):
                    info.OldEvents.append("xUnknownBlockID")

            if 'FOUND DATA ON FAKE READ EVENT' in event['message']:
                event_time = ParseTimestamp(event['timeOfReport'])
                if (event_time == None):
                    info.NewEvents.append("FOUND_DATA_ON_FAKE_READ_EVENT")
                elif (START_TIME < event_time and 'FOUND DATA ON FAKE READ EVENT' not in info.NewEvents):
                    info.NewEvents.append("FOUND_DATA_ON_FAKE_READ_EVENT")
                elif ('FOUND DATA ON FAKE READ EVENT' not in info.OldEvents):
                    info.OldEvents.append("FOUND_DATA_ON_FAKE_READ_EVENT")

        gc_objects = dict()
        gc_info = GCInfo()
        # go through the list in chronological order
        for i in range(len(event_list['events'])-1, -1, -1):
            event = event_list['events'][i]
            if ("GCStarted" in event["message"]):
                gc_info = GCInfo()
                gc_info.StartTime = ParseTimestamp(event['timeOfReport'])
                if isinstance(event["details"], basestring):
                    m = re.search(r"GC generation:(\d+).+participatingSServices={(.+)}.+eligibleBSs={(.+)}", event["details"])
                    if m:
                        gc_info.Generation = int(m.group(1))
                        gc_info.ParticipatingSSSet = set(map(int, m.group(2).split(",")))
                        gc_info.EligibleBSSet = set(map(int, m.group(3).split(",")))
                        gc_objects[gc_info.Generation] = gc_info
                elif isinstance(event["details"], dict):
                    gc_info.Generation = event["details"]["generation"]
                    gc_info.ParticipatingSSSet = set(event["details"]["participatingSS"])
                    gc_info.EligibleBSSet = set(event["details"]["eligibleBS"])
                    gc_objects[gc_info.Generation] = gc_info

            if ("GCRescheduled" in event["message"]):
                if isinstance(event["details"], basestring):
                    m = re.search(r"GC rescheduled:(\d+)", event["details"])
                    if m:
                        generation = int(m.group(1))
                elif isinstance(event["details"], dict):
                    generation = event["details"]["paramGCGeneration"]

                if generation in gc_objects:
                    gc_objects[generation].Rescheduled = True
                    gc_objects[generation].EndTime = ParseTimestamp(event['timeOfReport'])
                else:
                    gc_info = GCInfo()
                    gc_info.Generation = generation
                    gc_info.StartTime = ParseTimestamp(event['timeOfReport'])
                    gc_info.Rescheduled = True
                    gc_objects[gc_info.Generation] = gc_info

            if ("GCCompleted" in event["message"]):
                if isinstance(event["details"], basestring):
                    pieces = event["details"].split(" ")
                    generation = int(pieces[0])
                    blocks_discarded = int(pieces[1])
                elif isinstance(event["details"], dict):
                    generation = event["details"]["generation"]
                    blocks_discarded = event["details"]["discardedBlocks"]
                service_id = int(event["serviceID"])
                end_time = ParseTimestamp(event['timeOfReport'])
                if generation in gc_objects:
                    gc_objects[generation].CompletedBSSet.add(service_id)
                    gc_objects[generation].DiscardedBytes += (blocks_discarded * 4096)
                    if end_time > gc_objects[generation].EndTime:
                        gc_objects[generation].EndTime = end_time

        gc_list = []
        for gen in sorted(gc_objects.keys()):
            gc_list.append(gc_objects[gen])
        if len(gc_list) > 0:
            info.LastGc = gc_list[-1]
        else:
            info.LastGc = None


    # Slice cache usage
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetCompleteStats', {})
    if not result:
        log.debug("failed to get complete stats")
    else:
        for node in result[info.ClusterName]["nodes"].itervalues():
            for service in node.itervalues():
                if "ssLoadPrimary" in service.keys() or "ssLoad" in service.keys():
                    service_id = service["serviceID"][0]
                    current_slice = SliceInfo()
                    current_slice.ServiceId = service_id
                    if "ssLoad" in service.keys():
                        current_slice.SSLoad = service["ssLoad"][0]
                    else:
                        current_slice.SSLoad = service["ssLoadPrimary"][0]
                    if "scacheBytesInUse" in service.keys():
                        current_slice.SecCacheBytes = service["scacheBytesInUse"][0]
                        if previousClusterInfo:
                            for oldssid in previousClusterInfo.SliceServices:
                                if oldssid == current_slice.ServiceId:
                                    current_slice.PreviousSecCacheBytes = previousClusterInfo.SliceServices[oldssid].SecCacheBytes
                    info.SliceServices[service_id] = current_slice

    # Look for failed/available drives
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, "ListDrives", {})
    if result:
        for drive in result["drives"]:
            if drive["status"] == "available":
                info.AvailableDriveCount += 1
            if drive["status"] == "failed":
                info.FailedDriveCount += 1

    return info

def HumanizeBytes(pBytes, pPrecision=1, pSuffix=None):
    if (pBytes == None):
        return "0 B"

    converted = float(pBytes)
    suffix_index = 0
    suffix = ['B', 'kiB', 'MiB', 'GiB', 'TiB']

    while (abs(converted) >= 1000):
        converted /= 1024.0
        suffix_index += 1
        if suffix[suffix_index] == pSuffix: break

    format_str = '%%0.%df %%s' % pPrecision
    return format_str % (converted, suffix[suffix_index])

def HumanizeDecimal(pNumber, pPrecision=1, pSuffix=None):
    if (pNumber == None):
        return "0"

    converted = float(pNumber)
    suffix_index = 0
    suffix = [' ', 'k', 'M', 'G', 'T']

    while (abs(converted) >= 1000):
        converted /= 1000.0
        suffix_index += 1
        if suffix[suffix_index] == pSuffix: break

    format_str = '%%0.%df %%s' % pPrecision
    return format_str % (converted, suffix[suffix_index])

def SecondsToElapsedStr(pSeconds):
    if type(pSeconds) is str: return pSeconds

    delta = datetime.timedelta(seconds=pSeconds)
    return TimeDeltaToStr(delta)

def TimeDeltaToStr(pTimeDelta):
    days = pTimeDelta.days
    hours = 0
    minutes = 0
    seconds = pTimeDelta.seconds
    if seconds >= 60:
        d, r = divmod(seconds, 60)
        minutes = d
        seconds = r
    if minutes >= 60:
        d, r = divmod(minutes, 60)
        hours = d
        minutes = r

    time_str = "%02d:%02d" % (minutes, seconds)
    if (hours > 0):
        time_str = "%02d:%02d:%02d" % (hours, minutes, seconds)
    if (days > 0):
        time_str = "%d-%02d:%02d:%02d" % (days, hours, minutes, seconds)

    return time_str

def DrawCellBorder(pStartX, pStartY, pCellWidth, pCellHeight):
    # print border
    current_line = 0
    screen.gotoXY(pStartX, pStartY)
    screen.set_color(ConsoleColors.PurpleFore)
    print '+' + '-' * (pCellWidth - 2) + '+'
    for i in range(pCellHeight):
        current_line += 1
        screen.gotoXY(pStartX, pStartY + current_line)
        print '|' + ' ' * (pCellWidth - 2) + '|'
    current_line += 1
    screen.gotoXY(pStartX, pStartY + current_line)
    print '+' + '-' * (pCellWidth - 2) + '+'

    screen.gotoXY(pStartX + 1, pStartY)

def LPadString(pStringToPad, pDesiredLength, pPadCharacter):
    string = pStringToPad
    while(len(string) < pDesiredLength):
        string = pPadCharacter + string
    return string

def DrawNodeInfoCell(pStartX, pStartY, pCellWidth, pCellHeight, pCompact, pSfappVerDisplay, pCellContent):

    top = pCellContent
    cell_width = pCellWidth
    cell_height = pCellHeight
    startx = pStartX
    starty = pStartY
    current_line = 0

    DrawCellBorder(pStartX, pStartY, pCellWidth, pCellHeight)

    current_line = 0
    screen.gotoXY(startx + 1, starty)

    if (pCellContent == None):
        return

    # first line - hostname, IP, cores, update time
    current_line += 1
    screen.gotoXY(startx + 1, starty + current_line)
    screen.set_color(ConsoleColors.CyanFore)
    print ' %-8s' % (top.Hostname),
    #if not pCompact:
    #    print '%-15s' % (node_ip),
    if top.NodeId > 0:
        if pCompact:
            print "[%s]" % top.NodeId,
        else:
            print " nodeID %s" % top.NodeId,
    elif top.NodeId == 0:
        if pCompact:
            print "pend",
        else:
            print " Pending",

    specials = ""
    if (top.EnsembleNode):
        specials += "*"
    if (top.ClusterMaster):
        specials += " ^"
    if (top.EnsembleLeader):
        specials += " !"
    specials = specials.strip()
    print "%-5s" % specials,

    print "  Up: " + SecondsToElapsedStr(top.Uptime),
    screen.reset()

    if (top.CoresTotal > 0):
        screen.set_color(ConsoleColors.YellowFore)
    if (top.CoresSinceStart > 0):
        screen.set_color(ConsoleColors.RedFore)
    if (top.CoresTotal > 0 or top.CoresSinceStart > 0):
        print " [Cores]",

    screen.reset()
    if pCompact:
        update_str = TimestampToStr(top.Timestamp, "%m-%d %H:%M:%S")
    else:
        update_str = ' Refresh: ' + TimestampToStr(top.Timestamp, "%m-%d %H:%M:%S")
    screen.gotoXY(startx + cell_width - len(update_str) - 2, starty + current_line)
    if time.time() - top.Timestamp > 60:
        screen.set_color(ConsoleColors.YellowFore)
    print update_str
    screen.reset()

    # second line - node software version
    current_line += 1
    screen.gotoXY(startx + 1, starty + current_line)
    screen.reset()

    ver_parts = []
    for key in pSfappVerDisplay:
        if key in top.SfVersion:
            ver_parts.append(top.SfVersion[key])

    display_ver = ",".join(ver_parts)
    if len(display_ver) > 86:
        display_ver = display_ver[:86]
    print ' %s' % (display_ver)

    if pCompact:
        # third line - CPU usage and MEM usage
        current_line += 1
        screen.gotoXY(startx + 1, starty + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        print ' CPU:',
        screen.reset()
        if (top.TotalCpu >= 90):
            screen.set_color(ConsoleColors.RedFore)
        elif (top.TotalCpu >= 80):
            screen.set_color(ConsoleColors.YellowFore)
        elif top.TotalCpu >= 50:
            screen.set_color(ConsoleColors.WhiteFore)
        else:
            screen.set_color(ConsoleColors.GreenFore)
        print '%4.1f%%' % (top.TotalCpu),
        screen.reset()
        screen.set_color(ConsoleColors.WhiteFore)
        print ' MEM:',
        if (top.TotalMemory > 0):
            mem_pct = 100.0 * float(top.UsedMemory)/float(top.TotalMemory)
        else:
            mem_pct = 0
        screen.reset()
        if (mem_pct >= 90):
            screen.set_color(ConsoleColors.RedFore)
        elif (mem_pct >= 80):
            screen.set_color(ConsoleColors.YellowFore)
        elif mem_pct >= 50:
            screen.set_color(ConsoleColors.WhiteFore)
        else:
            screen.set_color(ConsoleColors.GreenFore)
        print '%4.1f%%' % (mem_pct),
        screen.reset()
        print ' (%s / %s, %s cache)' % (HumanizeBytes(top.UsedMemory, 0, 'MiB'), HumanizeBytes(top.TotalMemory, 0, 'MiB'), HumanizeBytes(top.CacheMemory, 0, 'MiB'))
    else:
        # third line - CPU usage
        current_line += 1
        screen.gotoXY(startx + 1, starty + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        print ' CPU:',
        screen.reset()
        if (top.TotalCpu >= 90):
            screen.set_color(ConsoleColors.RedFore)
        elif (top.TotalCpu >= 80):
            screen.set_color(ConsoleColors.YellowFore)
        elif top.TotalCpu >= 50:
            screen.set_color(ConsoleColors.WhiteFore)
        else:
            screen.set_color(ConsoleColors.GreenFore)
        print '%5.1f%%' % (top.TotalCpu),
        screen.reset()
        print ' (%s)' % (top.CpuDetail)

        # fourth line - mem usage
        current_line += 1
        screen.gotoXY(startx + 1, starty + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        print ' MEM:',
        if (top.TotalMemory > 0):
            mem_pct = 100.0 * float(top.UsedMemory)/float(top.TotalMemory)
        else:
            mem_pct = 0
        screen.reset()
        if (mem_pct >= 90):
            screen.set_color(ConsoleColors.RedFore)
        elif (mem_pct >= 80):
            screen.set_color(ConsoleColors.YellowFore)
        elif mem_pct >= 50:
            screen.set_color(ConsoleColors.WhiteFore)
        else:
            screen.set_color(ConsoleColors.GreenFore)

        print '%5.1f%%' % (mem_pct),
        screen.reset()
        print ' (%s / %s, %s cache)' % (HumanizeBytes(top.UsedMemory, 0, 'MiB'), HumanizeBytes(top.TotalMemory, 0, 'MiB'), HumanizeBytes(top.CacheMemory, 0, 'MiB')),

        screen.reset()
        screen.set_color(ConsoleColors.WhiteFore)
        print ' Slab: ',
        screen.reset()
        print HumanizeBytes(top.SlabMemory)

    # process table
    #header line
    header = ""
    header += "."
    header += LPadString("Process", 19, ".")
    header += ".."
    header += LPadString("PID", 5, ".")
    header += ".."
    header += LPadString("CPU", 5, ".")
    header += ".."
    header += LPadString("RSS", 11, ".")
    header += ".."
    header += LPadString("Uptime", 11, ".")
    header += ".."
    if not pCompact:
        header += LPadString("Rd/s", 11, ".")
        header += ".."
        header += LPadString("Wr/s", 11, ".")
        header += "."
    header += "." * (cell_width - len(header) - 2)
    current_line += 1
    screen.gotoXY(startx + 1, starty + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print header
    screen.reset()

    # line for each process
    display_lines = []
    max_len = 0
    #for pid in sorted(top.Processes.iterkeys()):                                                   # sort by process id
    for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].ProcessName):     # sort by process name
    #for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].CpuUsage):       # sort by cpu
    #for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].ResidentMemory): # sort by mem
        if pCompact and "sfnetwd" in top.Processes[pid].ProcessName: continue
        if pCompact and "service_manager" in top.Processes[pid].ProcessName: continue

        line = " "
        if pCompact: line = "   "
        if top.Processes[pid].DiskDevice != None:
            fmt_str = "%%%ds (%%s)" % (13 - len(top.Processes[pid].DiskDevice))
            line += (fmt_str % (top.Processes[pid].ProcessName, top.Processes[pid].DiskDevice))
        else:
            line += ('%19s' % top.Processes[pid].ProcessName)
        line += ('  %5d' % top.Processes[pid].Pid)
        line += ('  %4d%%' % top.Processes[pid].CpuUsage)
        line += ('  %11s' % HumanizeBytes(top.Processes[pid].ResidentMemory, 2))
        line += ('  %11s' % SecondsToElapsedStr(top.Processes[pid].Uptime))
# This theoretically should show the difference between NVRAM and SSD IO by the slice, but in practice does not give predictable numbers
#        if ("slice" in top.Processes[pid].ProcessName):
#            line += ('  %11s' % (HumanizeBytes(top.Processes[pid].ReadThroughput - top.Processes[pid].DeviceReadThroughput, 0) + "/" + HumanizeBytes(top.Processes[pid].DeviceReadThroughput, 0)))
#            line += ('  %11s' % (HumanizeBytes(top.Processes[pid].WriteThroughput - top.Processes[pid].DeviceWriteThroughput, 0) + "/" + HumanizeBytes(top.Processes[pid].DeviceWriteThroughput, 0)))
#        else:
#            line += ('  %11s' % HumanizeBytes(top.Processes[pid].ReadThroughput, 0))
#            line += ('  %11s' % HumanizeBytes(top.Processes[pid].WriteThroughput, 0))
        if not pCompact:
            line += ('  %11s' % HumanizeBytes(top.Processes[pid].ReadThroughput, 0))
            line += ('  %11s' % HumanizeBytes(top.Processes[pid].WriteThroughput, 0))
        if len(line) > max_len:
            max_len = len(line)
        display_lines.append(line)

    for line in display_lines:
        if len(line) < max_len:
            line = " " * (max_len - len(line)) + line
        current_line += 1
        screen.gotoXY(startx + 1, starty + current_line)
        print line

    if pCompact:
        names_10g = ['bond1', 'Bond10G']
        for name in names_10g:
            if name in top.Nics.keys():
                nic_name = name
                break
        if nic_name:
            #current_line += 1
            current_line = pCellHeight
            screen.gotoXY(startx + 1, starty + current_line)
            screen.set_color(ConsoleColors.WhiteFore)
            print "              %7s  RX:" % (nic_name),
            screen.reset()
            print "%9s/s" % (HumanizeBytes(top.Nics[nic_name].RxThroughput)),
            screen.set_color(ConsoleColors.WhiteFore)
            print " TX:",
            screen.reset()
            print "%9s/s" % (HumanizeBytes(top.Nics[nic_name].TxThroughput))
    else:
        # next lines - network table
        current_line += 1
        screen.gotoXY(startx + 1, starty + current_line)
        #log.debug(top.Hostname + " network header starting at " + str(startx+1) + "," + str(starty+current_line))
        screen.set_color(ConsoleColors.WhiteFore)
        print '....%9s.%15s..%11s..%11s..%17s..%4s.......' % ("......NIC", ".....IP Address", ".........RX", ".........TX", "......MAC address", ".MTU")
        screen.reset()
        for nic_name in sorted(top.Nics.keys()):
            if nic_name in ["lo", "eth2", "eth3"]: continue
            if "bond" not in nic_name.lower() and top.NodeType != "SFFC" and top.NodeType != "FC0025": continue
            current_line += 1
            screen.gotoXY(startx + 1, starty + current_line)
            display_name = top.Nics[nic_name].Name
            #log.debug(top.Hostname + " " + display_name + " starting at " + str(startx+1) + "," + str(starty+current_line))
            if not top.Nics[nic_name].Up: display_name += "*"
            print '    %9s %15s  %9s/s  %9s/s  %17s  %4d' % (display_name, top.Nics[nic_name].IpAddress, HumanizeBytes(top.Nics[nic_name].RxThroughput), HumanizeBytes(top.Nics[nic_name].TxThroughput), top.Nics[nic_name].MacAddress, top.Nics[nic_name].Mtu)

        # next lines - fc table
        if len(top.FcHbas.keys()) > 0:
            current_line += 1
            screen.gotoXY(startx + 1, starty + current_line)
            screen.set_color(ConsoleColors.WhiteFore)
            print '..Host....Model..................PortWWN........LinkState............RX.............TX.'
            screen.reset()
            for fc_host in sorted(top.FcHbas.keys()):
                hba = top.FcHbas[fc_host]
                current_line += 1
                screen.gotoXY(startx + 1, starty + current_line)
                link_state = hba.LinkState
                if "up" in link_state.lower():
                    link_state = link_state + " - " + hba.LinkSpeed
                print '%6s  %7s  %23s  %15s  %7s Fr/s   %7s Fr/s' % (fc_host, hba.Model, hba.PortWWN, link_state, HumanizeDecimal(hba.RxFrameThroughput), HumanizeDecimal(hba.TxFrameThroughput))



def DrawClusterInfoCell(pStartX, pStartY, pCellWidth, pCellHeight, pClusterInfo):
    if (pClusterInfo == None):
        return

    DrawCellBorder(pStartX, pStartY, pCellWidth, pCellHeight)
    current_line = 0
    screen.gotoXY(pStartX + 1, pStartY)

    # cluster name, last updated
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.CyanFore)
    print ' Cluster Name: ' + pClusterInfo.ClusterName
    screen.reset()
    update_str = ' Refresh: ' + TimestampToStr(pClusterInfo.Timestamp, "%m-%d %H:%M:%S")
    screen.gotoXY(pStartX + pCellWidth - len(update_str) - 2, pStartY + current_line)
    print update_str
    screen.reset()

    # mvip and svip
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " MVIP: ",
    screen.reset()
    print "%-15s" % pClusterInfo.Mvip,
    screen.set_color(ConsoleColors.WhiteFore)
    print " SVIP: ",
    screen.reset()
    print "%-15s" % pClusterInfo.Svip,
    screen.set_color(ConsoleColors.WhiteFore)
    print " Ensemble Leader: ",
    screen.reset()
    print pClusterInfo.EnsembleLeader

    # cluster master, uid, bs count, ss count
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Cluster Master: ",
    screen.reset()
    print pClusterInfo.ClusterMaster,
    screen.set_color(ConsoleColors.WhiteFore)
    print "  UID: ",
    screen.reset()
    print "%s" % (pClusterInfo.UniqueId),
    screen.set_color(ConsoleColors.WhiteFore)
    print "  BS count: ",
    screen.reset()
    if pClusterInfo.BSCount < pClusterInfo.ExpectedBSCount:
        screen.set_color(ConsoleColors.YellowFore)
    else:
        screen.set_color(ConsoleColors.GreenFore)
    print "%d/%d" % (pClusterInfo.BSCount, pClusterInfo.ExpectedBSCount),
    screen.set_color(ConsoleColors.WhiteFore)
    print "  SS count: ",
    screen.reset()
    if pClusterInfo.SSCount < pClusterInfo.ExpectedSSCount:
        screen.set_color(ConsoleColors.YellowFore)
    else:
        screen.set_color(ConsoleColors.GreenFore)
    print "%d/%d" % (pClusterInfo.SSCount, pClusterInfo.ExpectedSSCount)

    # accounts, volumes, iSCSI sessions, IOPs
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Accounts: ",
    screen.reset()
    print "%-3d" % pClusterInfo.AccountCount,
    screen.set_color(ConsoleColors.WhiteFore)
    print " Volumes: ",
    screen.reset()
    print "%-4d" % pClusterInfo.VolumeCount,
    screen.set_color(ConsoleColors.WhiteFore)
    print " iSCSI Sessions: ",
    screen.reset()
    print "%-4d" % pClusterInfo.SessionCount,
    screen.set_color(ConsoleColors.WhiteFore)
    print " Current IOPS: ",
    screen.reset()
    print "%d" % pClusterInfo.CurrentIops

    # Capacity utilization, percent full
    if (pClusterInfo.TotalSpace > 0):
        used_pct = 100.0 * float(pClusterInfo.UsedSpace)/float(pClusterInfo.TotalSpace)
    else:
        used_pct = 0
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Capacity Utilization: ",
    if (used_pct >= 90):
        screen.set_color(ConsoleColors.RedFore)
    elif (used_pct >= 80):
        screen.set_color(ConsoleColors.YellowFore)
    else:
        screen.reset()
    print "%.1f%%" % used_pct,
    screen.reset()
    print " (%sB / %sB)" % (HumanizeDecimal(pClusterInfo.UsedSpace, 2), HumanizeDecimal(pClusterInfo.TotalSpace, 2)),

    if pClusterInfo.SfMajorVersion == 3:
        if (pClusterInfo.ClusterFullThreshold > 0):
            full_pct = 100.0 * float(pClusterInfo.UsedSpace)/float(pClusterInfo.ClusterFullThreshold)
        else:
            full_pct = 0
        screen.set_color(ConsoleColors.WhiteFore)
        print "  Cluster Full: ",
        if (full_pct >= 90):
            screen.set_color(ConsoleColors.RedFore)
        elif (full_pct >= 80):
            screen.set_color(ConsoleColors.YellowFore)
        else:
            screen.reset()
        print "%.1f%%" % full_pct
        screen.reset()
    elif pClusterInfo.SfMajorVersion >= 4:
        screen.set_color(ConsoleColors.WhiteFore)
        print "  Cluster Full: ",
        if "stage5" in str(pClusterInfo.ClusterFullThreshold) or "stage4" in str(pClusterInfo.ClusterFullThreshold):
            screen.set_color(ConsoleColors.RedFore)
        elif "stage3" in str(pClusterInfo.ClusterFullThreshold):
            screen.set_color(ConsoleColors.YellowFore)
        else:
            screen.set_color(ConsoleColors.GreenFore)
        print pClusterInfo.ClusterFullThreshold
        screen.reset()

    # Provisioned, dedup and compression
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Provisioned: ",
    screen.reset()
    print "%sB" % HumanizeDecimal(pClusterInfo.ProvisionedSpace),
    screen.set_color(ConsoleColors.WhiteFore)
    print "  Deduplication: ",
    screen.reset()
    if pClusterInfo.DedupPercent > 0:
        if pClusterInfo.DedupPercent < 95:
            screen.set_color(ConsoleColors.RedFore)
            if pClusterInfo.DedupPercent > 400:
                screen.set_color(ConsoleColors.YellowFore)
        print "%0.02fx" % (float(pClusterInfo.DedupPercent)/100),
    else:
        print "0.00",
    screen.set_color(ConsoleColors.WhiteFore)
    print "  Compression: ",
    screen.reset()
    if pClusterInfo.CompressionPercent > 0:
        if pClusterInfo.CompressionPercent < 95:
            screen.set_color(ConsoleColors.RedFore)
        if pClusterInfo.CompressionPercent > 400:
            screen.set_color(ConsoleColors.YellowFore)
        print "%0.02fx" % (float(pClusterInfo.CompressionPercent)/100)
    else:
        print "0.00"
    screen.reset()

    # GC info
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Last GC Start: ",
    screen.reset()
    if not pClusterInfo.LastGc:
        print "never"
    else:
        if pClusterInfo.LastGc.StartTime > 0:
            if pClusterInfo.LastGc.StartTime < time.time() - 60 * 60:
                screen.set_color(ConsoleColors.YellowFore)
            print TimestampToStr(pClusterInfo.LastGc.StartTime),
        else:
            print "never",
        screen.set_color(ConsoleColors.WhiteFore)
        screen.reset()
        if pClusterInfo.LastGc.EndTime > 0:
            screen.set_color(ConsoleColors.WhiteFore)
            print " Elapsed: ",
            delta = pClusterInfo.LastGc.EndTime - pClusterInfo.LastGc.StartTime
            if (delta >= 60 * 30):
                screen.set_color(ConsoleColors.RedFore)
            elif (delta >= 60 * 25):
                screen.set_color(ConsoleColors.YellowFore)
            else:
                screen.set_color(ConsoleColors.GreenFore)
            print TimeDeltaToStr(datetime.timedelta(seconds=delta)),
            screen.set_color(ConsoleColors.WhiteFore)
            print " Discarded: ",
            screen.reset()
            print HumanizeDecimal(pClusterInfo.LastGc.DiscardedBytes) + "B"
        if pClusterInfo.LastGc.Rescheduled:
            screen.set_color(ConsoleColors.YellowFore)
            print " Rescheduled"
    screen.reset()

    # Syncing
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Slice Syncing: ",
    screen.reset()
    if pClusterInfo.SliceSyncing.lower() == "no":
        screen.set_color(ConsoleColors.GreenFore)
    else:
        screen.set_color(ConsoleColors.YellowFore)
    print "%3s" % str(pClusterInfo.SliceSyncing),
    screen.reset()
    screen.set_color(ConsoleColors.WhiteFore)
    print "  Bin Syncing: ",
    screen.reset()
    if pClusterInfo.BinSyncing.lower() == "no":
        screen.set_color(ConsoleColors.GreenFore)
    else:
        screen.set_color(ConsoleColors.YellowFore)
    print str(pClusterInfo.BinSyncing)
    screen.reset()

    # Slice load/scache
    columns = 2
    if len(pClusterInfo.SliceServices.keys()) > 20:
        columns = 4
    count = 0
    for sliceid in sorted(pClusterInfo.SliceServices.keys()):
        x_offset = 1
        x_offset = count % columns * 43 + 1
        if count % columns == 0:
            current_line += 1
        count += 1

        ss = pClusterInfo.SliceServices[sliceid]
        #log.debug("slice " + str(ss.ServiceId) + " ss load = " + str(ss.SSLoad))
        screen.gotoXY(pStartX + x_offset, pStartY + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        sys.stdout.write(" slice%-3s" % str(ss.ServiceId))
        sys.stdout.write(" ssLoad: ")
        if ss.SSLoad < 30:
            screen.set_color(ConsoleColors.GreenFore)
        elif ss.SSLoad < 60:
            screen.set_color(ConsoleColors.WhiteFore)
        elif ss.SSLoad < 80:
            screen.set_color(ConsoleColors.YellowFore)
        else:
            screen.set_color(ConsoleColors.RedFore)
        sys.stdout.write(" %2d" % ss.SSLoad)
        screen.reset()

        screen.set_color(ConsoleColors.WhiteFore)
        sys.stdout.write("  sCache: ")
        if ss.PreviousSecCacheBytes >= 0 and ss.PreviousSecCacheBytes < ss.SecCacheBytes:
            screen.set_color(ConsoleColors.YellowFore)
        elif ss.PreviousSecCacheBytes >= 0 and ss.PreviousSecCacheBytes > ss.SecCacheBytes:
            screen.set_color(ConsoleColors.GreenFore)
        elif ss.SecCacheBytes <= 131072:
            screen.set_color(ConsoleColors.GreenFore)
        else:
            screen.set_color(ConsoleColors.WhiteFore)
        sys.stdout.write("%9s" % HumanizeBytes(ss.SecCacheBytes, 1))

        # Draw an up or down arrow as appropriate on non-windows
        if platform.system().lower() != 'windows':
            if ss.PreviousSecCacheBytes >= 0:
                if ss.PreviousSecCacheBytes < ss.SecCacheBytes > 0:
                    sys.stdout.write(u'\u25b2') # up triangle
                elif ss.PreviousSecCacheBytes > ss.SecCacheBytes < 0:
                    sys.stdout.write(u'\u25bC') # down triangle

    # software version warnings
    if (not pClusterInfo.SameSoftwareVersions):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        print " Software versions do not match on all cluster nodes"
        screen.reset()
    if pClusterInfo.DebugSoftware or pClusterInfo.CustomBinary:
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        if pClusterInfo.DebugSoftware and pClusterInfo.CustomBinary:
            print " Running debug build custom binary"
        elif pClusterInfo.CustomBinary:
            print " Running custom binary"
        elif pClusterInfo.DebugSoftware:
            print " Running debug build"
        screen.reset()

    # NVRAM mount
    if (len(pClusterInfo.NvramNotMounted) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        sys.stdout.write(" NVRAM not mounted on")
        for node in pClusterInfo.NvramNotMounted:
            sys.stdout.write(" " + node)
        print " "
        screen.reset()
    if (len(pClusterInfo.NvramRamdrive) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        sys.stdout.write(" NVRAM is a RAMdrive on")
        for node in pClusterInfo.NvramRamdrive:
            sys.stdout.write(" " + node)
        print " "
        screen.reset()

    # core files
    if (len(pClusterInfo.OldCores) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        print " Old cores found on",
        for node in pClusterInfo.OldCores:
            print " " + node,
        print " "
        screen.reset()
    if (len(pClusterInfo.NewCores) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        print " New cores found on",
        for node in pClusterInfo.NewCores:
            print " " + node,
        print " "
        screen.reset()

    # 'bad' events
    if (len(pClusterInfo.OldEvents) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        print " Old Errors Found:",
        for event in pClusterInfo.OldEvents:
            print " " + event,
        screen.reset()
        print " "
    if (len(pClusterInfo.NewEvents) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        print " New Errors Found:",
        for event in pClusterInfo.NewEvents:
            print " " + event,
        print " "
        screen.reset()

    # Failed or available drives
    if pClusterInfo.FailedDriveCount > 0:
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        if pClusterInfo.FailedDriveCount > 1:
            print " %d Failed Drives" % pClusterInfo.FailedDriveCount
        else:
            print " %d Failed Drive" % pClusterInfo.FailedDriveCount
    if pClusterInfo.AvailableDriveCount > 0:
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        if pClusterInfo.AvailableDriveCount > 1:
            print " %d Available Drives" % pClusterInfo.AvailableDriveCount
        else:
            print " %d Available Drive" % pClusterInfo.AvailableDriveCount

    # cluster faults
    if len(pClusterInfo.WhitelistClusterFaults) > 0:
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        print " Whitelisted Faults:",
        screen.reset()
        for fault in pClusterInfo.WhitelistClusterFaults:
            print " " + fault,
        print " "

        if len(pClusterInfo.ClusterFaultsWarn) + len(pClusterInfo.ClusterFaultsError) + len(pClusterInfo.ClusterFaultsCrit) > 0:
            current_line += 1
            screen.gotoXY(pStartX + 1, pStartY + current_line)
            screen.set_color(ConsoleColors.WhiteFore)
            print " Cluster Faults:",

            screen.set_color(fg=ConsoleColors.YellowFore, bk=ConsoleColors.RedBack)
            for fault in pClusterInfo.ClusterFaultsCrit:
                print " " + fault,
            screen.reset()

            screen.set_color(ConsoleColors.RedFore)
            for fault in pClusterInfo.ClusterFaultsError:
                print " " + fault,
            screen.reset()

            screen.set_color(ConsoleColors.YellowFore)
            for fault in pClusterInfo.ClusterFaultsWarn:
                print " " + fault,
            screen.reset()

    # NVRAM specific faults
    if len(pClusterInfo.NVRAMFaultsError) > 0 or len(pClusterInfo.NVRAMFaultsWarn) > 0:
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        print " NVRAM Faults:"
        if len(pClusterInfo.NVRAMFaultsError) > 0:
            screen.set_color(ConsoleColors.RedFore)
            for fault in pClusterInfo.NVRAMFaultsError:
                current_line += 1
                screen.gotoXY(pStartX + 1, pStartY + current_line)
                print "  " + fault
        if len(pClusterInfo.NVRAMFaultsWarn) > 0:
            screen.set_color(ConsoleColors.YellowFore)
            for fault in pClusterInfo.NVRAMFaultsWarn:
                current_line += 1
                screen.gotoXY(pStartX + 1, pStartY + current_line)
                print "  " + fault

previous_cluster_columns = ""
def LogClusterResult(pOutputDir, pClusterInfo):
    if (pClusterInfo == None): return

    filename = pOutputDir + "/" + TimestampToStr(START_TIME, "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + '_cluster_' + pClusterInfo.Mvip + ".csv"

    try:
        if not os.path.isfile(filename):
            log = open(filename, 'w')
        else:
            log = open(filename, 'a')
    except IOError:
        return

    columns = "Timestamp,Time,ClusterMaster,VolumeCount,SessionCount,AccountCount,BSCount,SSCount,CurrentIops,UsedSpace,ProvisionedSpace,Fullness,DedupPercent,CompressionPercent,LastGcStart,LastGcDurationSeconds,LastGcDiscarded"
    for ss in pClusterInfo.SliceServices.values():
        columns += ",slice" + str(ss.ServiceId) + " sCache Bytes"
    columns.strip(',')
    global previous_cluster_columns
    if not os.path.isfile(filename) or not previous_cluster_columns or previous_cluster_columns != columns:
        previous_cluster_columns = columns
        log.write(columns + "\n")

    log.write("\"" + str(pClusterInfo.Timestamp) + "\"")
    log.write(",\"" + TimestampToStr(pClusterInfo.Timestamp) + "\"")
    log.write(",\"" + str(pClusterInfo.ClusterMaster) + "\"")
    log.write(",\"" + str(pClusterInfo.VolumeCount) + "\"")
    log.write(",\"" + str(pClusterInfo.SessionCount) + "\"")
    log.write(",\"" + str(pClusterInfo.AccountCount) + "\"")
    log.write(",\"" + str(pClusterInfo.BSCount) + "\"")
    log.write(",\"" + str(pClusterInfo.SSCount) + "\"")
    log.write(",\"" + str(pClusterInfo.CurrentIops) + "\"")
    log.write(",\"" + str(pClusterInfo.UsedSpace) + "\"")
    log.write(",\"" + str(pClusterInfo.ProvisionedSpace) + "\"")
    log.write(",\"" + str(pClusterInfo.ClusterFullThreshold) + "\"")
    log.write(",\"" + str(pClusterInfo.DedupPercent) + "\"")
    log.write(",\"" + str(pClusterInfo.CompressionPercent) + "\"")
    if pClusterInfo.LastGc:
        log.write(",\"" + TimestampToStr(pClusterInfo.LastGc.StartTime) + "\"")
        log.write(",\"" + str(pClusterInfo.LastGc.EndTime - pClusterInfo.LastGc.StartTime) + "\"")
        log.write(",\"" + str(pClusterInfo.LastGc.DiscardedBytes) + "\"")
    else:
        log.write(",,,")
    for ss in pClusterInfo.SliceServices.values():
        log.write(",\"" + str(ss.SecCacheBytes) + "\"")
    log.write("\n")

    log.flush()
    log.close()

previous_columns = dict()
def LogNodeResult(pOutputDir, pNodeIp, pNodeInfo):
    if (pNodeInfo == None): return

    top = pNodeInfo
    filename = pOutputDir + "/" + TimestampToStr(START_TIME, "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + '_node_' + pNodeIp + ".csv"

    # Figure out the column order
    columns = 'Timestamp,Time,Hostname,SfVersion,TotalCPU,TotalMem,TotalUsedMem,SlabMem,'
    for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].ProcessName):
        columns += (top.Processes[pid].ProcessName + ' CPU,' + top.Processes[pid].ProcessName + ' ResidentMem,' + top.Processes[pid].ProcessName + ' Uptime,')
    for nic_name in sorted(top.Nics.keys()):
        if (nic_name == "lo"): continue
        if (":" in nic_name): continue
        columns += (nic_name + " TX," + nic_name + " RX," + nic_name + " Dropped,")
    columns.strip(',')

    # See if we need to create a new file/write out the column header
    global previous_columns
    if pNodeIp not in previous_columns:
        previous_columns[pNodeIp] = columns
        log = open(filename, 'w')
        log.write(columns + "\n")
        log.close()
    elif previous_columns[pNodeIp] != columns:
        previous_columns[pNodeIp] = columns
        log = open(filename, 'a')
        log.write(columns + "\n")
        log.close()

    with open(filename, 'a') as log:
        log.write("\"" + str(top.Timestamp) + "\"")
        log.write(",\"" + TimestampToStr(pNodeInfo.Timestamp) + "\"")
        log.write(",\"" + str(top.Hostname) + "\"")
        log.write(",\"" + str(top.SfVersion) + "\"")
        log.write(",\"" + str(top.TotalCpu) + "\"")
        log.write(",\"" + str(top.TotalMemory) + "\"")
        log.write(",\"" + str(top.UsedMemory) + "\"")
        log.write(",\"" + str(top.SlabMemory) + "\"")
        for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].ProcessName):
            log.write(",\"" + str(top.Processes[pid].CpuUsage) + "\"")
            log.write(",\"" + str(top.Processes[pid].ResidentMemory) + "\"")
            log.write(",\"" + str(top.Processes[pid].Uptime) + "\"")
        for nic_name in sorted(top.Nics.keys()):
            if (nic_name == "lo"): continue
            if (":" in nic_name): continue
            log.write(",\"" + str(top.Nics[nic_name].RxThroughput) + "\"")
            log.write(",\"" + str(top.Nics[nic_name].TxThroughput) + "\"")
            log.write(",\"" + str(top.Nics[nic_name].RxDropped + top.Nics[nic_name].TxDropped) + "\"")
        log.write("\n")
        log.flush()

def SingleNodeThread(log, pResults, pNodeIp, pNodeUser, pNodePass, pKeyFile=None):
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        top = GetNodeInfo(log, pNodeIp, pNodeUser, pNodePass, pKeyFile)
        pResults[pNodeIp] = top
    except IOError as e:
        if e.errno == 4:
            pass
        else:
            log.debug("exception: " + str(e) + " - " + traceback.format_exc())
    except Exception as e:
        log.debug("exception: " + str(e) + " - " + traceback.format_exc())

def GatherNodeInfoThread(log, pNodeResults, pInterval, pApiUser, pApiPass, pNodeIpList, pNodeUser, pNodePass, pKeyFile=None):
    try:
        manager = multiprocessing.Manager()
        # pylint: disable=no-member
        node_results = manager.dict()
        # pylint: enable=no-member
        while True:
            try:
                # Start one thread per node
                node_threads = []
                start_time = time.time()
                for node_ip in pNodeIpList:
                    node_results[node_ip] = None
                    th = multiprocessing.Process(target=SingleNodeThread, name="Node-" + node_ip + "-Thread", args=(log, node_results, node_ip, pNodeUser, pNodePass, pKeyFile))
                    th.daemon = True
                    th.start()
                    log.debug("started " + th.name + " process " + str(th.pid))
                    node_threads.append(th)

                # Wait for all threads to complete
                while True:
                    alldone = True
                    for th in node_threads:
                        if th.is_alive():
                            alldone = False
                            break
                    if alldone: break
                    # Kill any threads that haven't finished within 20 sec
                    if time.time() - start_time > 30:
                        for th in node_threads:
                            if th.is_alive():
                                log.debug("terminating " + th.name + " process " + str(th.pid))
                                th.terminate()
                        break
                    time.sleep(1)
                for th in node_threads:
                    log.debug("finished " + th.name + " process " + str(th.pid))

                # Copy the results into the global structure
                for node_ip in pNodeIpList:
                    if node_results[node_ip]:
                        pNodeResults[node_ip] = copy.deepcopy(node_results[node_ip])

                time.sleep(pInterval)
            except KeyboardInterrupt: raise
            except Exception as e:
                log.debug("exception: " + str(e) + " - " + traceback.format_exc())
    except KeyboardInterrupt:
        log.debug("KeyboardInterrupt")
    log.debug("exiting")

def ClusterInfoThread(log, pClusterResults, pNodeResults, pInterval, pMvip, pApiUser, pApiPass, faultWhitelist):
    try:
        previous_cluster_info = None
        while True:
            try:
                cluster_info = GetClusterInfo(log, pMvip, pApiUser, pApiPass, pNodeResults, previous_cluster_info, faultWhitelist)
                if cluster_info:
                    log.debug("got cluster info")
                    pClusterResults[pMvip] = copy.deepcopy(cluster_info)
                    previous_cluster_info = cluster_info
                else:
                    log.debug("no cluster info")
                time.sleep(pInterval)
            except KeyboardInterrupt: raise
            except Exception as e:
                log.debug("exception: " + str(e) + " - " + traceback.format_exc())
    except KeyboardInterrupt:
        log.debug("KeyboardInterrupt")
    log.debug("exiting")

def IsValidIpv4Address(pAddressString):
    pieces = pAddressString.split(".")
    last_octet = 0
    try:
        last_octet = int(pieces[-1])
    except ValueError: return False

    if len(pieces) != 4 or last_octet <= 0:
        return False
    try:
        socket.inet_pton(socket.AF_INET, pAddressString)
    except AttributeError: # inet_pton not available
        try:
            socket.inet_aton(pAddressString)
        except socket.error:
            return False
        pieces = pAddressString.split(".")
        return (len(pieces) == 4 and int(pieces[0]) > 0)
    except socket.error: # not a valid address
        return False

    return True

# Quick and dirty logging
class DebugLog():
    Enable = False
    def debug(self, message):
        if not self.Enable: return
        caller = inspect.stack()[1][3]
        if caller == "<module>":
            caller = "MainThread"
        with open("sf-top-debug.txt", 'a') as debug_out:
            message = TimestampToStr(time.time(), "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + "  " + caller + ": " + str(message)
            if not message.endswith("\n"): message += "\n"
            debug_out.write(message)
            debug_out.flush()

# This is for the debugger under MacOS to work
class FallbackTerminal:
    def set_color(self, fore = None, back = None): pass
    def gotoXY(self, x, y): pass
    def reset(self): pass
    def clear(self): pass
    def set_title(self): pass

START_TIME = time.time()
LOCAL_TZ = LocalTimezone()

def Abort():
    pass

if __name__ == '__main__':

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, version="%prog Version " + __version__, description="sf-top v" + __version__ + " - Monitor a SolidFire cluster, including cluster stats, resource utilization, etc.")

    parser.add_option("-m", "--mvip", type="string", dest="mvip", default=sfdefaults.mvip, help="the MVIP of the cluster")
    parser.add_option("-n", "--node_ips", type="string", dest="node_ips", default=sfdefaults.node_ips, help="the IP addresses of the nodes (if MVIP is not specified, or nodes are not in a cluster)")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default="sfadmin", help="the SSH username for the nodes [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=None, help="the SSH password for the nodes [%default]")
    parser.add_option("--api_user", type="string", dest="api_user", default="admin", help="the API username for the cluster [%default]")
    parser.add_option("--api_pass", type="string", dest="api_pass", default="admin", help="the API password for the cluster [%default]")
    parser.add_option("--keyfile", type="string", dest="keyfile", default=None, help="the full path to an RSA key for SSH auth")
    parser.add_option("--interval", type="int", dest="interval", default=1, help="the number of seconds between each refresh [%default]")
    parser.add_option("--cluster_interval", type="int", dest="cluster_interval", default=0, help="the number of seconds between each cluster refresh (leave at zero to use the same interval) [%default]")
    parser.add_option("--columns", type="int", dest="columns", default=3, help="the number of columns to use for display [%default]")
    parser.add_option("--compact", action="store_true", dest="compact", default=False, help="show a compact view (useful for large clusters)")
    parser.add_option("--noclusterinfo", action="store_false", dest="clusterinfo", default=True, help="do not gather/show cluster information (node info only)")
    parser.add_option("--export", action="store_true", dest="export", default=False, help="save the results in a file as well as print to the screen")
    parser.add_option("--output_dir", type="string", dest="output_dir", default="sf-top-out", help="the directory to save exported data")
    parser.add_option("--fault_whitelist", action="list", dest="fault_whitelist", default=None, help="ignore these faults when displaying cluster faults")
    parser.add_option("--nopending", action="store_false", dest="pending_nodes", default=True, help="do not gather/show pending node info (active nodes only)")
    parser.add_option("--sfapp_disp", action="list", dest="sfapp_disp", default=None, help="the list of sfapp version elements to display [Version,Repository,Revision,BuildDate,BuildType]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="write detailed debug info to a log file")

    (options, args) = parser.parse_args()
    mvip = options.mvip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    api_user = options.api_user
    api_pass = options.api_pass
    keyfile = options.keyfile
    compact = options.compact
    showclusterinfo = options.clusterinfo
    export = options.export
    columns = options.columns
    output_dir = options.output_dir
    interval = float(options.interval)
    cluster_interval = float(options.cluster_interval)
    if cluster_interval <= 0:
        cluster_interval = interval
    if compact:
        showclusterinfo = False
    fault_whitelist = options.fault_whitelist
    if fault_whitelist == None:
        fault_whitelist = sfdefaults.fault_whitelist
    monitor_pending_nodes = options.pending_nodes
    sfapp_disp = options.sfapp_disp
    if sfapp_disp == None:
        sfapp_disp = ['Version','Repository','Revision','BuildDate','BuildType']

    output_dir = os.path.expandvars(os.path.expanduser(output_dir))

    if keyfile:
        keyfile = os.path.expandvars(os.path.expanduser(keyfile))
        if not os.path.exists(keyfile):
            print "Can't find key file {}".format(keyfile)
            exit(1)

    log = DebugLog()
    log.Enable = options.debug
    log.debug("Starting main process " + str(os.getpid()))

    # Create a global handler for any uncaught exceptions
    def UnhandledException(extype, ex, tb):
        if extype == KeyboardInterrupt:
            raise ex
        else:
            log.debug("Suppressing " + str(extype) + "\n" + "".join(traceback.format_tb(tb)) + "\n" + str(ex))
    sys.excepthook = UnhandledException

    if mvip:
        try:
            print "Getting a list of nodes in cluster " + mvip
            node_ips = []
            api_result = CallApiMethod(log, mvip, api_user, api_pass, 'ListAllNodes', {}, printErrors=True)
            if not api_result:
                print "Could not call API on " + mvip
                sys.exit(1)
            for node_info in api_result["nodes"]:
                node_ips.append(str(node_info["mip"]))
            if monitor_pending_nodes:
                for node_info in api_result["pendingNodes"]:
                    node_ips.append(str(node_info["mip"]))
        except KeyboardInterrupt:
            sys.exit(0)
    else:
        if (type(options.node_ips) is list):
            node_ips = options.node_ips
        else:
            node_ips = []
            node_ips_str = options.node_ips
            node_pieces = node_ips_str.split(',')
            for ip in node_pieces:
                ip = ip.strip()
                if not IsValidIpv4Address(ip):
                    print "'" + ip + "' does not appear to be a valid address"
                    exit(1)
                node_ips.append(ip)
    if (len(node_ips) <= 0):
        print "Please supply at least one node IP address (--mvip=z.z.z.z OR --node_ips=\"x.x.x.x, y.y.y.y\")"
        exit(1)

    print "Gathering info from nodes " + str(node_ips) + " ..."

    if (not os.path.exists(output_dir)):
        os.makedirs(output_dir)

    # Make a default for the keyfile path on Windows
    if platform.system().lower() == 'windows' and not keyfile:
        keyfile = os.environ["HOMEDRIVE"] + os.environ["HOMEPATH"] + "\\ssh\\id_rsa"
        log.debug("windows - will look for a keyfile at " + keyfile)

    # Get a reference to the screen.  This extra complication is to make the debugger work in Mac OS
    try:
        import termios
    except: pass
    try:
        screen = terminal.get_terminal()
    except termios.error:
        screen = FallbackTerminal()


    table_height = 0
    if compact:
        cell_width = 67
    else:
        cell_width = 89
    cell_height = 0
    cluster_cell_height = cell_height
    cluster_cell_width = cell_width

    if (platform.system().lower() == 'windows'):
        originx = 0
        originy = 0
    else:
        originx = 1
        originy = 1

    # Shared data structures to hold the results in
    main_manager = multiprocessing.Manager()
    # pylint: disable=no-member
    node_results = main_manager.dict()
    # pylint: enable=no-member
    for n_ip in node_ips:
        node_results[n_ip] = None
    # pylint: disable=no-member
    cluster_results = main_manager.dict()
    # pylint: enable=no-member

    previous_node_list = set(node_results.keys())
    all_threads = dict()
    all_threads["GatherNodeInfoThread"] = None
    previous_table_height = 0
    previous_cell_height = 0
    table_height = 0
    clear_table = False
    cluster_name = None
    try:
        while True:
            #screen.gotoXY(originx, originy)
            if "GatherNodeInfoThread" not in all_threads or all_threads["GatherNodeInfoThread"] == None:
                th = multiprocessing.Process(target=GatherNodeInfoThread, name="GatherNodeInfoThread", args=(log, node_results, interval, api_user, api_pass, node_ips, ssh_user, ssh_pass, keyfile))
                th.start()
                log.debug("started GatherNodeInfoThread process " + str(th.pid))
                all_threads["GatherNodeInfoThread"] = th

                # Wait for at least one good result to come in
                got_results = False
                while True:
                    for n_ip in node_ips:
                        if node_results[n_ip]:
                            got_results = True
                            break
                    if got_results: break
                    time.sleep(2)

            # Look for the mvip if we don't already know it
            if not mvip:
                for n_ip in node_ips:
                    if node_results[n_ip] == None: continue
                    for nicname in node_results[n_ip].Nics.keys():
                        if (re.search("eth0:", nicname)):
                            mvip = node_results[n_ip].Nics[nicname].IpAddress
                            break
                        if (re.search("bond0:", nicname)):
                            mvip = node_results[n_ip].Nics[nicname].IpAddress
                            break
                        if (re.search("bond1g:", nicname, re.IGNORECASE)):
                            mvip = node_results[n_ip].Nics[nicname].IpAddress
                            break
                    if mvip:
                        break

            if mvip and not cluster_name:
                api_result = CallApiMethod(log, mvip, api_user, api_pass, 'GetClusterInfo', {})
                if api_result:
                    cluster_name = api_result["clusterInfo"]["name"]
                    if platform.system().lower() == 'windows':
                        screen.set_title(cluster_name)
                    else:
                        sys.stdout.write("\x1b]2;" + cluster_name + "\x07")

            # Start the cluster info thread if we haven't already
            if showclusterinfo and mvip and ("ClusterInfoThread" not in all_threads or all_threads["ClusterInfoThread"] == None):
                if mvip not in cluster_results:
                    cluster_results[mvip] = None
                cluster_info_thread = multiprocessing.Process(target=ClusterInfoThread, name="ClusterInfoThread", args=(log, cluster_results, node_results, cluster_interval, mvip, api_user, api_pass, fault_whitelist))
                cluster_info_thread.start()
                log.debug("started ClusterInfoThread process " + str(cluster_info_thread.pid))
                all_threads["ClusterInfoThread"] = cluster_info_thread

            # Determine how tall each cell needs to be
            cell_height = 0
            for node_ip in node_ips:
                if node_results[node_ip] == None: continue
                if compact:
                    node_cell_height = 4 + 1 + (len(node_results[node_ip].Processes.keys()) - 2) + 1
                else:
                    node_cell_height = 4 + 1 + len(node_results[node_ip].Processes.keys()) + 1 + (len(node_results[node_ip].Nics.keys()) - 4) - 1
                    if len(node_results[node_ip].FcHbas.keys()) > 0:
                        node_cell_height += 1 + len(node_results[node_ip].FcHbas.keys())
                if (node_cell_height > cell_height):
                    cell_height = node_cell_height

            if showclusterinfo and cluster_results.get(mvip):
                if len(cluster_results[mvip].SliceServices.keys()) > 20:
                    cluster_cell_width = cell_width * 2 - 1

            # Determine table height, based on cell height, columns, number of nodes + cell for cluster info
            previous_table_height = table_height
            if compact or not showclusterinfo:
                table_height =  math.ceil(float(len(node_ips)) / float(columns)) * (cell_height + 1) + originy
            else:
                if cluster_cell_width > cell_width:
                    table_height =  math.ceil(float(len(node_ips) + 2) / float(columns)) * (cell_height + 1) + originy
                else:
                    table_height =  math.ceil(float(len(node_ips) + 1) / float(columns)) * (cell_height + 1) + originy

            if cell_height != previous_cell_height:
                clear_table = True

            if clear_table:
                # Table height changed, node list changed, etc
                screen.clear()
                previous_cell_height = cell_height
                clear_table = False

            # Display/log node info
            for i in range(len(node_ips)):
                node_ip = node_ips[i]

                # Log to file
                if export and node_results[node_ip]:
                    try: LogNodeResult(output_dir, node_ip, node_results[node_ip])
                    except KeyboardInterrupt: raise
                    except Exception as e:
                        log.debug("exception in LogNodeResult: " + str(e) + " - " + traceback.format_exc())

                # Calculate where on the screen
                cell_row = i / columns
                cell_col = i % columns
                cell_x = cell_col * cell_width + originx
                if cell_col > 1: cell_x -= (cell_col - 1) # overlap the borders
                cell_y = cell_row * (cell_height + 1) + originy
                if (cell_x > originx): cell_x -= 1

                # Draw a table cell
                try:
                    DrawNodeInfoCell(cell_x, cell_y, cell_width, cell_height, compact, sfapp_disp, node_results[node_ip])
                except KeyboardInterrupt: raise
                except Exception as e:
                    log.debug("exception in DrawNodeInfoCell: " + str(e) + " - " + traceback.format_exc())

            # Display/log cluster info
            if not compact and cluster_results.get(mvip):
                cell_row = len(node_ips) / columns
                cell_col = len(node_ips) % columns
                if cluster_cell_width > cell_width and cell_col == columns - 1:
                    cell_row += 1
                    cell_col = 0
                cell_x = cell_col * cell_width + originx
                if cell_col > 1: cell_x -= (cell_col - 1) # overlap the borders
                cell_y = cell_row * (cell_height + 1) + originy
                if (cell_x > originx):
                    cell_x -= 1

                if export:
                    try: LogClusterResult(output_dir, cluster_results[mvip])
                    except KeyboardInterrupt: raise
                    except Exception as e:
                        log.debug("exception in LogClusterResult: " + str(e) + " - " + traceback.format_exc())
                try:
                    DrawClusterInfoCell(cell_x, cell_y, cluster_cell_width, cell_height, cluster_results[mvip])
                except KeyboardInterrupt: raise
                except Exception as e:
                    log.debug("exception in DrawClusterInfoCell: " + str(e) + " - " + traceback.format_exc())

            screen.reset()
            screen.gotoXY(originx, table_height + 1)
            print
            time.sleep(4)

            # Check to see if the node list has changed
            if options.mvip:
                log.debug("Refreshing list of nodes")
                api_result = CallApiMethod(log, mvip, api_user, api_pass, 'ListAllNodes', {})
                if api_result:
                    node_ips = []
                    for node_info in api_result["nodes"]:
                        node_ips.append(str(node_info["mip"]))
                    if monitor_pending_nodes:
                        for node_info in api_result["pendingNodes"]:
                            node_ips.append(str(node_info["mip"]))

                    if len(node_ips) > 0 and set(node_ips) != previous_node_list:
                        log.debug("Detected change in node list")
                        clear_table = True
                        for name, th in all_threads.items():
                            log.debug("Killing " + name)
                            th.terminate()
                            del all_threads[name]

                        for n_ip in node_ips:
                            node_results[n_ip] = None
                        previous_node_list = set(node_ips)

    except KeyboardInterrupt:
        log.debug("KeyboardInterrupt")
    finally:
        # Wait a short while for threads to finish, or kill them if they don't
        start_time = time.time()
        all_stopped = False
        while not all_stopped and time.time() - start_time < 5:
            all_stopped = True
            for th in all_threads.itervalues():
                if th.is_alive():
                    all_stopped = False
                    break
            if not all_stopped: time.sleep(1)
        if not all_stopped:
            for name, th in all_threads.iteritems():
                if th.is_alive():
                    log.debug("Killing " + name)
                    th.terminate()

    if table_height > 0:
        screen.gotoXY(originx, table_height + 1)
    elif previous_table_height > 0:
        screen.gotoXY(originx, previous_table_height + 1)
    print
    log.debug("exiting")
    exit(0)
