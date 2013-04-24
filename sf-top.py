#!/usr/bin/python

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

mvip = ""                           # The MVIP of the cluster
                                    # --mvip

node_ips = [                        # The IP addresses of the nodes to monitor (if mvip is not specified)
    #"192.168.133.0",               # --node_ips
]

ssh_user = "root"                   # The SSH username for the nodes
                                    # --ssh_user

ssh_pass = "password"              # The SSH password for the nodes
                                    # --ssh_pass

api_user = "admin"                  # The API username for the nodes
                                    # --api_user

api_pass = "password"              # The API password for the nodes
                                    # --api_pass

keyfile = ""                        # Keyfile where your RSA key is stored. Mostly interesting for Windows.
                                    # --keyfile

interval = 1                        # The number of seconds between each refresh
                                    # --interval

columns = 3                         # The number of columns to use for display
                                    # --columns

output_dir = "sf-top-out"           # The directory to save exported data/reports in
                                    # --output_dir

# ----------------------------------------------------------------------------

# cover a couple different ways of doing this
__version__ = '2.1'
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
import sys, os
import tarfile
import traceback
import multiprocessing
import signal
import copy
import inspect
import BaseHTTPServer

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
        DSTOFFSET = LocalTimezone.STDOFFSET

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

class NodeInfo:
    def __init__(self):
        self.Timestamp = time.time()
        self.Hostname = 'Unknown'
        self.SfVersion = 'Unknown'
        self.TotalMemory = 0
        self.UsedMemory = 0
        self.CacheMemory = 0
        self.TotalCpu = 0.0
        self.CpuDetail = 'Unknown'
        self.CoresSinceStart = 0
        self.CoresTotal = 0
        self.NodeId = -1
        self.NvramMounted = False
        self.EnsembleNode = False
        self.ClusterMaster = False
        self.Processes = dict()
        self.Nics = dict()
        self.SCache = dict()

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
        self.OldCores = []
        self.NewCores = []
        self.SliceSyncing = "No"
        self.BinSyncing = "No"
        self.ClusterFaults = []
        self.NewEvents = []
        self.OldEvents = []
        self.LastGcStart = 0
        self.LastGcEnd = 0
        self.LastGcDiscarded = 0
        self.SliceServices = dict()
        self.SfMajorVersion = 0

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
        RedBack = 4
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
    except KeyboardInterrupt: raise
    except: return None

    return response.read()

def CallApiMethod(log, pMvip, pUsername, pPassword, pMethodName, pMethodParams):
    rpc_url = 'https://' + pMvip + '/json-rpc/1.0'
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, rpc_url, pUsername, pPassword)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)

    api_call = json.dumps( { 'method': pMethodName, 'params': pMethodParams, 'id': random.randint(100, 1000) } )
    log.debug("Calling " + api_call + " on " + rpc_url)
    response_obj = None
    api_resp = None
    try:
        api_resp = urllib2.urlopen(rpc_url, api_call, 3 * 60)
    except urllib2.HTTPError as e:
        if (e.code == 401):
            print "Invalid cluster admin/password"
            sys.exit(1)
        else:
            if (e.code in BaseHTTPServer.BaseHTTPRequestHandler.responses):
                #log.debug("HTTPError: " + str(e.code) + " " + str(BaseHTTPServer.BaseHTTPRequestHandler.responses[e.code]))
                return None
            else:
                #log.debug("HTTPError: " + str(e.code))
                return None
    except urllib2.URLError as e:
        #mylog.warning("URLError on " + rpc_url + " : " + str(e.reason))
        return None
    except httplib.BadStatusLine, e:
        #log.debug("httplib.BadStatusLine: " + str(e))
        return None
    except Exception, e:
        return None

    if (api_resp != None):
        response_str = api_resp.read().decode('ascii')
        #log.debug(response_str)
        try:
            response_obj = json.loads(response_str)
        except ValueError:
            return None

    if (response_obj == None or 'error' in response_obj):
        return None

    return response_obj['result']

def GetNodeInfo(log, pNodeIp, pNodeUser, pNodePass, pKeyFile=None):
    if not os.path.exists(pKeyFile): pKeyFile = None
    if pKeyFile == '': pKeyFile = None

    begin = datetime.datetime.now()
    #start_time = datetime.datetime.now()
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys()
    try:
        ssh.connect(pNodeIp, username=pNodeUser, password=pNodePass, key_filename=pKeyFile)
    except paramiko.BadAuthenticationType:
        print pNodeIp + " - You must use SSH host keys to connect to this node (try adding your key to the node, or disabling OTP)"
        log.debug(pNodeIp + " - You must use SSH host keys to connect to this node (try adding your key to the node, or disabling OTP)")
        sys.exit(1)
    except paramiko.AuthenticationException:
        try:
            ssh.connect(pNodeIp, username=pNodeUser, password=pNodePass)
        except paramiko.AuthenticationException:
            print pNodeIp + " - Authentication failed. Check the password or RSA key"
            log.debug(pNodeIp + " - Authentication failed. Check the password or RSA key")
            sys.exit(1)

    #time_connect = datetime.datetime.now() - start_time
    #time_connect = time_connect.microseconds + time_connect.seconds * 1000000

    usage = NodeInfo()

    #
    # Get the hostname, sf version, node memory usage, core files, mounts
    #
    #start_time = datetime.datetime.now()
    timestamp = TimestampToStr(START_TIME, "%Y%m%d%H%M.%S")
    command = ""
    command += "echo hostname=`\\hostname`"
    command += ";/sf/bin/sfapp --Version -laAll 0"
    command += ";\\free -o"
    command += ";touch -t " + timestamp + " /tmp/timestamp;echo newcores=`find /sf -maxdepth 1 -name \"core*\" -newer /tmp/timestamp | wc -l`"
    command += ";echo allcores=`ls -1 /sf/core* | wc -l`"
    command += ";\\cat /proc/mounts | \\grep dev"
    command += ";grep nodeID /etc/solidfire.json"
    ver_string = ""
    volumes = dict()
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    for line in data:
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
        if line.startswith("/dev"):
            pieces = line.split()
            volumes[pieces[1]] = pieces[0].split("/")[-1]
            continue
        m = re.search(r'nodeID" : (\d+)', line)
        if (m):
            usage.NodeId = int(m.group(1))
            continue

    if "nvdisk" in volumes["/mnt/pendingDirtyBlocks"]:
        usage.NvramMounted = True
    else:
        usage.NvramMounted = False

    # sfapp Release UC Version: 4.06 sfdev: 1.90 Revision: f13bebca8736 Build date: 2011-12-20 10:02
    # sfapp Debug Version: 4.08 sfdev: 1.91 Revision: 58220b8bd90a Build date: 2012-01-07 14:28 Tag: TSIP4v1
    # sfapp BuildType=Release UC Release=lithium Version=3.45 sfdev=1.92 Revision=a7ca42f12f32 BuildDate=2012-02-13@12:20
    # sfapp BuildType=Release,UC Release=lithium Version=3.49 sfdev=1.93 Revision=80c48b6aab2f BuildDate=2012-02-21@14:22
    ver_string = re.sub(r"sfapp\s+", "", ver_string)
    ver_string = re.sub(r"BuildType=", "", ver_string)
    ver_string = re.sub(r"Release=\S+\s+", "", ver_string)
    ver_string = re.sub(r"Version", "Ver", ver_string)
    ver_string = re.sub(r"sfdev:\s+\d+\.\d+\s+", "", ver_string)
    ver_string = re.sub(r"sfdev=\d+\.\d+\s+", "", ver_string)
    ver_string = re.sub(r"Revision", "Rev", ver_string)
    ver_string = re.sub(r"md5=\S+\s+", "", ver_string)
    #ver_string = re.sub("Build date: ", "", ver_string)
    usage.SfVersion = ver_string.strip()
    #time_ver = datetime.datetime.now() - start_time
    #time_ver = time_ver.microseconds + time_ver.seconds * 1000000

    #
    # Get a list of sf processes from 'ps'
    #
    #start_time = datetime.datetime.now()
    process_names2pids = dict()
    process_pids2names = dict()
    process_pids2disks = dict()
    stdin, stdout, stderr = ssh.exec_command("\\ps -eo comm,pid,args --no-headers | \\egrep '^block|^slice|^master|^service_manager|^sfnetwd|^sfconfig|^java.+zookeeper'")
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
    # Get the secondary cache usage
    #
    command = ""
    for pname in process_names2pids.keys():
        if "slice" in pname:
            pid = process_names2pids[pname]
            disk = process_pids2disks[pid]
            command += ";echo `ls -1 " + disk + "/failed | wc -l` `du " + disk + "/failed`"
    command = command.strip(";")
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
    for line in data:
        pieces = line.split()
        #files = int(pieces[0])
        size = int(pieces[1]) * 1024
        disk = pieces[2]
        pieces = disk.split("/")
        disk = pieces[2]
        for pid, device in process_pids2disks.iteritems():
            if disk in device:
                proc_name = process_pids2names[pid]
                usage.SCache[proc_name] = size

    #
    # Run 'top', 'ifconfig', grep /proc/diskstatus, grep /proc/[pid]/io.
    # These all need multiple samples separated by a wait, so put them together and only wait once
    #
    #start_time = datetime.datetime.now()
    sample_interval = 2
    command = ""
    command += "\\ifconfig | \\egrep -i 'eth|bond|lo|inet|RX bytes';"
    command += "\\grep sd /proc/diskstats"
    if len(usage.Processes.keys()) > 0:
        command += ";\\grep bytes"
        for pid in usage.Processes.iterkeys():
            command += " /proc/" + str(pid) + "/io"
    command += ";"
    command += "\\top -b -d " + str(sample_interval) + " -n 2;"
    command += "\\ifconfig | \\egrep -i 'eth|bond|lo|inet|RX bytes|dropped|MTU';"
    command += "\\grep sd /proc/diskstats"
    if len(usage.Processes.keys()) > 0:
        command += ";\\grep bytes"
        for pid in usage.Processes.iterkeys():
            command += " /proc/" + str(pid) + "/io"
    stdin, stdout, stderr = ssh.exec_command(command)
    data = stdout.readlines()
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
        command += "echo " + str(pid) + " = `\\grep VmRSS /proc/" + str(pid) + "/status`;"
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
    stdin, stdout, stderr = ssh.exec_command("echo CurrentDate=`date +%s`;cat /proc/stat")
    data = stdout.readlines()
    for line in data:
        m = re.search(r"CurrentDate=(\d+)", line)
        if (m):
            current_time = int(m.group(1))
        m = re.search(r"btime\s+(\d+)", line)
        if (m):
            system_boot_time = int(m.group(1))
    #time_time = datetime.datetime.now() - start_time
    #time_time = time_time.microseconds + time_time.seconds * 1000000

    #
    # Get uptime for each process from /proc/[pid]/stat
    #
    #start_time = datetime.datetime.now()
    command = ""
    for pid in usage.Processes.keys():
        command += "\\cat /proc/" + str(pid) + "/stat;"
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

def GetClusterInfo(log, pMvip, pApiUser, pApiPass, pNodesInfo):

    info = ClusterInfo()

    log.debug("checking node info")

    sf_version = 0
    for node_ip in pNodesInfo.keys():
        m = re.search(r'Ver=(\d+\.\d+)', pNodesInfo[node_ip].SfVersion)
        if m:
            ver_str = m.group(1)
            pieces = ver_str.split(".")
            sf_version = int(pieces[0])
            break
    info.SfMajorVersion = sf_version

    # find the cluster master
    master = None
    for node_ip in pNodesInfo.keys():
        if pNodesInfo[node_ip] == None: continue
        for nic_name in pNodesInfo[node_ip].Nics.keys():
            if (re.search(r'eth0:', nic_name)):
                master = pNodesInfo[node_ip].Hostname
                break
            if (re.search(r'bond0:', nic_name)):
                master = pNodesInfo[node_ip].Hostname
                break
            if (re.search(r'bond1g:', nic_name, re.IGNORECASE)):
                master = pNodesInfo[node_ip].Hostname
                break
        if master != None:
            break
    if (master != None):
        info.ClusterMaster = master

    # compare node software versions
    same = True
    debug = False
    for n1 in pNodesInfo.keys():
        if pNodesInfo[n1] == None: continue
        if "debug" in pNodesInfo[n1].SfVersion.lower():
            debug = True
        for n2 in node_ips:
            if pNodesInfo[n2] == None: continue
            if pNodesInfo[n1].SfVersion != pNodesInfo[n2].SfVersion:
                same = False
    info.SameSoftwareVersions = same
    info.DebugSoftware = debug

    # Core files
    for node_ip in pNodesInfo.keys():
        if pNodesInfo[node_ip] == None: continue
        if pNodesInfo[node_ip].CoresSinceStart > 0:
            info.NewCores.append(pNodesInfo[node_ip].Hostname)
        elif pNodesInfo[node_ip].CoresTotal - pNodesInfo[node_ip].CoresSinceStart > 0:
            info.OldCores.append(pNodesInfo[node_ip].Hostname)

    # NVRAM mount
    for node_ip in pNodesInfo.keys():
        if pNodesInfo[node_ip] == None: continue
        if not pNodesInfo[node_ip].NvramMounted:
            info.NvramNotMounted.append(pNodesInfo[node_ip].Hostname)

    log.debug("gathering cluster info")

    # get basic cluster info
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetClusterInfo', {})
    if result is None:
        log.debug("Failed to get cluster info")
        return None
    info.ClusterName = result['clusterInfo']['name']
    info.Mvip = result['clusterInfo']['mvip']
    info.Svip = result['clusterInfo']['svip']
    info.UniqueId = result['clusterInfo']['uniqueID']

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


    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListActiveVolumes', {})
    if result is None:
        log.debug("Failed to get active volumes")
        info.VolumeCount = 0
    else:
        if "volumes" in result.keys() and result["volumes"] != None:
            info.VolumeCount = len(result["volumes"])
        else:
            info.VolumeCount = 0
    result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'ListDeletedVolumes', {})
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

    if sf_version == 3 and info.TotalSpace > 0:
        full = info.TotalSpace - (3600 * 1000 * 1000 * 1000)
        # account for binary/si mismatch in solidfire calculation
        full = int(float(full) / float(1024*1024*1024*1024) * float(1000*1000*1000*1000))
        info.ClusterFullThreshold = full
    elif sf_version >= 4:
        result = CallApiMethod(log, pMvip, pApiUser, pApiPass, 'GetClusterFullThreshold', {})
        if result:
            info.ClusterFullThreshold = result["fullness"]

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
        for fault in result["faults"]:
            if fault['code'] not in info.ClusterFaults:
                info.ClusterFaults.append(fault['code'])

    # Check for slice syncing
    sync_html = HttpRequest(log, "https://" + pMvip + "/reports/slicesyncing", pApiUser, pApiPass)
    if (sync_html != None and "table" in sync_html):
        info.SliceSyncing = "Yes"
    else:
        info.SliceSyncing = "No"

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

        # Look for GC info
        blocks_discarded = 0
        gc_generation = 0
        gc_complete_count = 0
        gc_start_time = 0
        gc_end_time = 0
        for i in range(len(event_list['events'])):
            event = event_list['events'][i]
            if ("GCStarted" in event["message"]):
                details = event["details"]
                m = re.search(r'GC generation:(\d+)', details)
                if (m):
                    if (int(m.group(1)) == gc_generation):
                        gc_start_time = ParseTimestamp(event['timeOfReport'])
                        break

            if ("GCCompleted" in event["message"]):
                details = event["details"]
                pieces = details.split()
                if (gc_generation <= 0): gc_generation = int(pieces[0])
                if (int(pieces[0]) == gc_generation):
                    gc_complete_count += 1
                    blocks_discarded += int(pieces[1])
                    end_time = ParseTimestamp(event['timeOfReport'])
                    if (end_time > gc_end_time):
                        gc_end_time = end_time
        info.LastGcStart = gc_start_time
        if (gc_complete_count >= info.BSCount):
            info.LastGcEnd = gc_end_time
            info.LastGcDiscarded = blocks_discarded * 4096

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
                    else:
                        for node in pNodesInfo.itervalues():
                            if "slice" + str(service_id) in node.SCache.keys():
                                current_slice.SecCacheBytes = node.SCache["slice" + service_id]
                                break
                    info.SliceServices[service_id] = current_slice

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

    if (abs(pNumber) < 1000):
        return str(pNumber)

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

def DrawNodeInfoCell(pStartX, pStartY, pCellWidth, pCellHeight, pCompact, pCellContent):

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
    if not pCompact:
        print '%-15s' % (node_ip),
    if (top.NodeId >= 0):
        if pCompact:
            print "[%s]" % top.NodeId,
        else:
            print " nodeID %s" % top.NodeId,
    if (top.EnsembleNode):
        print "*",
    if (top.ClusterMaster):
        print "^",
    screen.reset()
    if (top.CoresTotal > 0):
        screen.set_color(ConsoleColors.YellowFore)
    if (top.CoresSinceStart > 0):
        screen.set_color(ConsoleColors.RedFore)
    if (top.CoresTotal > 0 or top.CoresSinceStart > 0):
        if pCompact:
            print " [Cores]",
        else:
            print " [CoreFiles]",

    screen.reset()
    if pCompact:
        update_str = TimestampToStr(top.Timestamp)
    else:
        update_str = ' Updated: ' + TimestampToStr(top.Timestamp)
    screen.gotoXY(startx + cell_width - len(update_str) - 2, starty + current_line)
    if time.time() - top.Timestamp > 60:
        screen.set_color(ConsoleColors.YellowFore)
    print update_str
    screen.reset()

    # second line - node software version
    current_line += 1
    screen.gotoXY(startx + 1, starty + current_line)
    screen.reset()
    display_ver = top.SfVersion
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
        print '%5.1f%%' % (mem_pct),
        screen.reset()
        print ' (%s / %s, %s cache)' % (HumanizeBytes(top.UsedMemory, 0, 'MiB'), HumanizeBytes(top.TotalMemory, 0, 'MiB'), HumanizeBytes(top.CacheMemory, 0, 'MiB'))

    # process table
    #header line
    header = ""
    header += "."
    header += LPadString("Process", 16, ".")
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
        header += ".."
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
            line += ('%16s' % top.Processes[pid].ProcessName)
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
        screen.set_color(ConsoleColors.WhiteFore)
        print '....%9s.%15s..%11s..%11s..%17s..%4s.......' % ("......NIC", ".....IP Address", ".........RX", ".........TX", "......MAC address", ".MTU")
        screen.reset()
        for nic_name in sorted(top.Nics.keys()):
            if "bond" not in nic_name and "Bond" not in nic_name: continue
            if (nic_name == "lo"): continue
            current_line += 1
            screen.gotoXY(startx + 1, starty + current_line)
            display_name = top.Nics[nic_name].Name
            if not top.Nics[nic_name].Up: display_name += "*"
            print '    %9s %15s  %9s/s  %9s/s  %17s  %4d' % (display_name, top.Nics[nic_name].IpAddress, HumanizeBytes(top.Nics[nic_name].RxThroughput), HumanizeBytes(top.Nics[nic_name].TxThroughput), top.Nics[nic_name].MacAddress, top.Nics[nic_name].Mtu)

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
    update_str = ' Updated: ' + TimestampToStr(pClusterInfo.Timestamp)
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
    print "%-15s" % pClusterInfo.Svip

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
    print "%-2d" % pClusterInfo.BSCount,
    screen.set_color(ConsoleColors.WhiteFore)
    print "  SS count: ",
    screen.reset()
    print "%-2d" % pClusterInfo.SSCount

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
            screen.set_color(ConsoleColors.RedFore)
        else:
            screen.reset()
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
    print "%d%%" % pClusterInfo.DedupPercent,
    screen.set_color(ConsoleColors.WhiteFore)
    print "  Compression: ",
    screen.reset()
    print "%d%%" % pClusterInfo.CompressionPercent

    # GC info
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Last GC Start: ",
    screen.reset()
    if pClusterInfo.LastGcStart > 0:
        print TimestampToStr(pClusterInfo.LastGcStart),
    else:
        print " never",
    screen.set_color(ConsoleColors.WhiteFore)
    screen.reset()
    if pClusterInfo.LastGcEnd > 0:
        screen.set_color(ConsoleColors.WhiteFore)
        print "  Elapsed: ",
        delta = pClusterInfo.LastGcEnd - pClusterInfo.LastGcStart
        if (delta >= 60 * 30):
            screen.set_color(ConsoleColors.RedFore)
        elif (delta >= 60 * 25):
            screen.set_color(ConsoleColors.YellowFore)
        else:
            screen.reset()
        print TimeDeltaToStr(datetime.timedelta(seconds=delta)),
        screen.set_color(ConsoleColors.WhiteFore)
        print "  Discarded: ",
        screen.reset()
        print HumanizeDecimal(pClusterInfo.LastGcDiscarded)
    screen.reset()

    # Syncing
    current_line += 1
    screen.gotoXY(pStartX + 1, pStartY + current_line)
    screen.set_color(ConsoleColors.WhiteFore)
    print " Slice Syncing: ",
    screen.reset()
    if (pClusterInfo.SliceSyncing != "No"):
        screen.set_color(ConsoleColors.YellowFore)
    print "%3s" % str(pClusterInfo.SliceSyncing),
    screen.reset()
    screen.set_color(ConsoleColors.WhiteFore)
    print " Bin Syncing: ",
    screen.reset()
    if (pClusterInfo.BinSyncing != "No"):
        screen.set_color(ConsoleColors.YellowFore)
    print str(pClusterInfo.BinSyncing)
    screen.reset()

    # Slice load/scache
    count = 0
    for sliceid in sorted(pClusterInfo.SliceServices):
        count += 1
        x_offset = 1
        if count % 2 == 0:
            x_offset = 40
        else:
            current_line += 1

        screen.gotoXY(pStartX + x_offset, pStartY + current_line)
        screen.set_color(ConsoleColors.WhiteFore)
        ss = pClusterInfo.SliceServices[sliceid]
        sys.stdout.write(" slice%-3s" % str(ss.ServiceId))
        screen.reset()
        sys.stdout.write(" ssLoad: %2d  sCache: %9s" % (ss.SSLoad, HumanizeBytes(ss.SecCacheBytes, 1)))

    # software version warnings
    if (not pClusterInfo.SameSoftwareVersions):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        print " Software versions do not match on all cluster nodes"
        screen.reset()
    if (pClusterInfo.DebugSoftware):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.YellowFore)
        print " Debug software running on cluster"
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

    # cluster faults
    if (len(pClusterInfo.ClusterFaults) > 0):
        current_line += 1
        screen.gotoXY(pStartX + 1, pStartY + current_line)
        screen.set_color(ConsoleColors.RedFore)
        print " Cluster Faults:",
        for fault in pClusterInfo.ClusterFaults:
            print " " + fault,
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

def LogClusterResult(pOutputDir, pClusterInfo):
    if (pClusterInfo == None): return

    filename = pOutputDir + "/" + TimestampToStr(START_TIME, "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + '_cluster_' + pClusterInfo.Mvip + ".csv"

    if not os.path.isfile(filename):
        log = open(filename, 'w')
        columns = "Timestamp,Time,ClusterMaster,VolumeCount,SessionCount,AccountCount,BSCount,SSCount,CurrentIops,UsedSpace,ProvisionedSpace,Fullness,DedupPercent,CompressionPercent,LastGcStart,LastGcDurationSeconds,LastGcDiscarded"
        log.write(columns)
        log.write("\n")
    else:
        log = open(filename, 'a')


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
    log.write(",\"" + TimestampToStr(pClusterInfo.LastGcStart) + "\"")
    log.write(",\"" + str(pClusterInfo.LastGcEnd - pClusterInfo.LastGcStart) + "\"")
    log.write(",\"" + str(pClusterInfo.LastGcDiscarded) + "\"")
    log.write("\n")

    log.flush()
    log.close()

previous_columns = dict()
def LogNodeResult(pOutputDir, pNodeIp, pNodeInfo):
    if (pNodeInfo == None): return

    top = pNodeInfo
    filename = pOutputDir + "/" + TimestampToStr(START_TIME, "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + '_node_' + pNodeIp + ".csv"

    # Figure out the column order
    columns = 'Timestamp,Time,Hostname,SfVersion,TotalCPU,TotalMem,TotalUsedMem,'
    for pid in sorted(top.Processes.iterkeys(), key=lambda pid:top.Processes[pid].ProcessName):
        columns += (top.Processes[pid].ProcessName + ' CPU,' + top.Processes[pid].ProcessName + ' ResidentMem,' + top.Processes[pid].ProcessName + ' Uptime,')
    for nic_name in sorted(top.Nics.keys()):
        if (nic_name == "lo"): continue
        if (":" in nic_name): continue
        columns += (nic_name + " TX," + nic_name + " RX," + nic_name + " Dropped,")
    columns.strip(',')

    # See if we need to create a new file/write out the column header
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
    except Exception as e:
        log.debug("exception: " + str(e) + " - " + traceback.format_exc())

def GatherNodeInfoThread(log, pNodeResults, pInterval, pNodeIpList, pNodeUser, pNodePass, pKeyFile=None):
    try:
        manager = multiprocessing.Manager()
        node_results = manager.dict()
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
                    if time.time() - start_time > 20:
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

def ClusterInfoThread(log, pClusterResults, pNodeResults, pInterval, pMvip, pApiUser, pApiPass):
    try:
        while True:
            try:
                cluster_info = GetClusterInfo(log, pMvip, pApiUser, pApiPass, pNodeResults)
                if cluster_info: log.debug("got cluster info")
                else: log.debug("no cluster info")

                pClusterResults[pMvip] = copy.deepcopy(cluster_info)
                #if shutdown_event.is_set(): break
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
        with open("sf-top-debug.txt", 'a') as debug_out:
            message = TimestampToStr(time.time(), "%Y-%m-%d-%H-%M-%S", LOCAL_TZ) + "  " + caller + ": " + message
            if not message.endswith("\n"): message += "\n"
            debug_out.write(message)
            debug_out.flush()

# This is for the debugger under MacOS to work
class FallbackTerminal:
    def set_color(self, fore = None, back = None): pass
    def gotoXY(self, x, y): pass
    def reset(self): pass
    def clear(self): pass

START_TIME = time.time()
LOCAL_TZ = LocalTimezone()

if __name__ == '__main__':

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "node_ips", "ssh_user", "ssh_pass", "api_user", "api_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            vars()[vname] = os.environ[env_name]
    if isinstance(node_ips, basestring):
        node_ips = node_ips.split(",")

    # Parse command line arguments
    parser = OptionParser(version="%prog Version " + __version__)

    parser.add_option("--mvip", type="string", dest="mvip", default=mvip, help="the MVIP of the cluster")
    parser.add_option("--node_ips", type="string", dest="node_ips", default=node_ips, help="the IP addresses of the nodes (if MVIP is not specified, or nodes are not in a cluster)")
    parser.add_option("--ssh_user", type="string", dest="ssh_user", default=ssh_user, help="the SSH username for the nodes [%default]")
    parser.add_option("--ssh_pass", type="string", dest="ssh_pass", default=ssh_pass, help="the SSH password for the nodes [%default]")
    parser.add_option("--api_user", type="string", dest="api_user", default=api_user, help="the API username for the cluster [%default]")
    parser.add_option("--api_pass", type="string", dest="api_pass", default=api_pass, help="the API password for the cluster [%default]")
    parser.add_option("--keyfile", type="string", dest="keyfile", default=keyfile, help="the full path to your RSA key")
    parser.add_option("--interval", type="int", dest="interval", default=interval, help="the number of seconds between each refresh [%default]")
    parser.add_option("--columns", type="int", dest="columns", default=columns, help="the number of columns to use for display [%default]")
    parser.add_option("--compact", action="store_true", dest="compact", help="show a compact view (useful for large clusters)")
    parser.add_option("--export", action="store_true", dest="export", help="save the results in a file as well as print to the screen")
    parser.add_option("--output_dir", type="string", dest="output_dir", default=output_dir, help="the directory to save exported data")
    parser.add_option("--debug", action="store_true", dest="debug", help="write detailed debug info to a log file")

    (options, args) = parser.parse_args()
    mvip = options.mvip
    ssh_user = options.ssh_user
    ssh_pass = options.ssh_pass
    api_user = options.api_user
    api_pass = options.api_pass
    keyfile = options.keyfile
    compact = options.compact
    export = options.export
    columns = options.columns
    output_dir = options.output_dir
    interval = float(options.interval)

    log = DebugLog()
    log.Enable = options.debug
    log.debug("Starting main process " + str(os.getpid()))

    # Create a global handler for any uncaught exceptions
    def UnhandledException(extype, ex, tb):
        if extype == KeyboardInterrupt:
            raise ex
        else:
            log.debug("Suppressing " + str(extype) + " " + "".join(traceback.format_tb(tb)))
    sys.excepthook = UnhandledException

    if mvip:
        try:
            print "Getting a list of nodes in cluster " + mvip
            node_ips = []
            api_result = CallApiMethod(log, mvip, api_user, api_pass, 'ListActiveNodes', {})
            if not api_result:
                print "Could not call API on " + mvip
                sys.exit(1)
            for node_info in api_result["nodes"]:
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
    if (platform.system().lower() == 'windows'):
        originx = 0
        originy = 0
    else:
        originx = 1
        originy = 1

    # Shared data structures to hold the results in
    main_manager = multiprocessing.Manager()
    node_results = main_manager.dict()
    for n_ip in node_ips:
        node_results[n_ip] = None
    cluster_results = main_manager.dict()

    all_threads = dict()
    previous_table_height = 0
    table_height = 0
    try:
        #screen.gotoXY(originx, originy)
        print "Gathering info from nodes " + str(node_ips) + " ..."
        th = multiprocessing.Process(target=GatherNodeInfoThread, name="GatherNodeInfoThread", args=(log, node_results, interval, node_ips, ssh_user, ssh_pass, keyfile))
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

        previous_cell_height = 0
        while True:
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

            # Start the cluster info thread if we haven't already
            if not compact and mvip and "ClusterInfoThread" not in all_threads:
                cluster_results[mvip] = None
                cluster_info_thread = multiprocessing.Process(target=ClusterInfoThread, name="ClusterInfoThread", args=(log, cluster_results, node_results, interval, mvip, api_user, api_pass))
                cluster_info_thread.start()
                log.debug("started ClusterInfoThread process " + str(cluster_info_thread.pid))
                all_threads["ClusterInfoThread"] = cluster_info_thread

            # Determine how tall each cell needs to be
            cell_height = 0
            for node_ip in node_ips:
                if (node_results[node_ip] == None): continue
                if compact:
                    node_cell_height = 4 + 1 + (len(node_results[node_ip].Processes.keys()) - 2) + 1
                else:
                    node_cell_height = 4 + 1 + len(node_results[node_ip].Processes.keys()) + 1 + (len(node_results[node_ip].Nics.keys()) - 4) - 1
                if (node_cell_height > cell_height):
                    cell_height = node_cell_height

            if not compact and mvip and cluster_results[mvip]:
                if (cell_height < 9 + len(cluster_results[mvip].SliceServices)/2):
                    cell_height = 9 + len(cluster_results[mvip].SliceServices)/2

            # Determine table height, based on cell height, columns, number of nodes + 1 cell for cluster info
            previous_table_height = table_height
            if compact:
                table_height =  math.ceil(float(len(node_ips)) / float(columns)) * (cell_height + 1) + originy
            else:
                table_height =  math.ceil(float(len(node_ips) + 1) / float(columns)) * (cell_height + 1) + originy

            if cell_height != previous_cell_height:
                screen.clear()
                previous_cell_height = cell_height

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
                    DrawNodeInfoCell(cell_x, cell_y, cell_width, cell_height, compact, node_results[node_ip])
                except KeyboardInterrupt: raise
                except Exception as e:
                    log.debug("exception in DrawNodeInfoCell: " + str(e) + " - " + traceback.format_exc())

            # Display/log cluster info
            if not compact and mvip and cluster_results[mvip]:
                cell_row = len(node_ips) / columns
                cell_col = len(node_ips) % columns
                cell_x = cell_col * cell_width + originx
                if cell_col > 1: cell_x -= (cell_col - 1) # overlap the borders
                cell_y = cell_row * (cell_height + 1) + originy
                if (cell_x > originx):
                    cell_x -= 1

                if export:
                    try: LogClusterResult(output_dir, cluster_results[mvip])
                    except KeyboardInterrupt: raise
                    except:
                        log.debug("exception in LogClusterResult: " + str(e) + " - " + traceback.format_exc())
                try:
                    DrawClusterInfoCell(cell_x, cell_y, cell_width, cell_height, cluster_results[mvip])
                except KeyboardInterrupt: raise
                except Exception as e:
                    log.debug("exception in DrawClusterInfoCell: " + str(e) + " - " + traceback.format_exc())

            screen.reset()
            screen.gotoXY(originx, table_height + 1)
            print
            time.sleep(4)

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

