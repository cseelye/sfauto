
MYPORT = 58083
config_file = "sfclienthealthd.json"

import commands
import datetime
import json
import platform
import os
import re
import socket
from socket import *
import sys
import time
import math
# Add my parent directory to the path, so I can find libs
sys.path.append(os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + os.sep + ".."))
import lib.libsf as libsf
from lib.libsf import mylog

# Open a broadcast socket to the world
s = socket(AF_INET, SOCK_DGRAM)
s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 0))

detected_group = libsf.GuessHypervisor()
while True:
    group_name = ""
    try:
        # Read configuration file
        config_lines = ""
        with open(config_file, "r") as config_handle:
            config_lines = config_handle.readlines();

        # Remove comments from JSON before loading it
        new_config_text = ""
        for line in config_lines:
            line = re.sub("(//.+)", "", line)
            new_config_text += line
        config_json = json.loads(new_config_text)

        if "group" in config_json.keys():
            group_name = config_json["group"]
    except Exception as e:
        #print e.message;
        pass

    if not group_name:
        group_name = detected_group
        if not group_name:
            group_name = "Physical"

    # Get my hostname
    retcode, stdout, stderr = libsf.RunCommand("hostname")
    hostname = stdout.strip()
    if not hostname:
        hostname = "UNKNOWN"

    # Get a list of IP addresses and subnet masks
    iplist = []
    local_ip = ''
    if platform.system().lower().startswith("win"):
        retcode, stdout, stderr = libsf.RunCommand('wmic nicconfig where ipenabled=true get ipaddress,ipsubnet /value')
        current_ips = []
        for line in stdout.split("\n"):
            line = line.strip()
            if len(line) <= 0: continue
            pieces = line.split('=')
            if pieces[0].lower() == "ipaddress":
                pieces = pieces[1].strip('{}').split(',')
                for ip in pieces:
                    ip = ip.strip('"')
                    if not ":" in ip:
                        current_ips.append(ip)
            if pieces[0].lower() == "ipsubnet":
                pieces = pieces[1].strip('{}').split(',')
                for i in xrange(0, len(pieces)):
                    subnet = pieces[i].strip('"')
                    if subnet.count(".") == 3:
                        ip = current_ips[i]
                        if not ip.startswith("127"):
                            iplist.append((ip, subnet))
                current_ips = []
    else:
        retcode, stdout, stderr = libsf.RunCommand("/sbin/ifconfig")
        current_ip = ""
        for line in stdout.split("\n"):
            m = re.search("inet addr:(\S+)", line)
            if m:
                current_ip = m.group(1)
            m = re.search("Mask:(\S+)", line)
            if m:
                mask = m.group(1)
                if not current_ip.startswith("127"):
                    iplist.append((current_ip, mask))
                current_ip = ""

    # Find "THE" IP address we will report
    for ip, subnet in iplist:
        # Management network in BDR
        if ip.startswith("192.168"):
            local_ip = ip
            break
        # CFT private VM network in BDR
        if ip.startswith("172.30"):
            local_ip = ip
            break
        # CFT management network in BDR
        if ip.startswith("172.16"):
            local_ip = ip
            break
    if not local_ip and len(iplist) > 0:
        local_ip = iplist[0]

    # Get the MAC address of the system to use as a unique ID
    # Use the alphabetically first MAC addr - thanks to udev and the Windows equivalent the "first" MAC sometimes changes on reboot in VMs
    mac_list = []
    if platform.system().lower().startswith("win"):
        retcode, stdout, stderr = libsf.RunCommand("getmac.exe /v /nh /fo csv")
        # "Local Area Connection 12","vmxnet3 Ethernet Adapter #18","00-50-56-A4-38-2F","\Device\Tcpip_{1058FAE5-466C-4229-A75A-91FA71ABAC8E}"
        # "Local Area Connection 13","vmxnet3 Ethernet Adapter #19","00-50-56-A4-38-2E","\Device\Tcpip_{DC53B222-EE87-4492-81A1-D1FC1845F555}"
        for line in stdout.split("\n"):
            line = line.strip()
            if len(line) <= 0: continue
            pieces = line.split(",")
            mac = pieces[2].strip('"').replace("-","").lower()
            if mac == "000000000000": continue # Occasionally we see MACs that are all 0 from badly configured NICs/bonds
            mac_list.append(mac)
    else:
        retcode, stdout, stderr = libsf.RunCommand("ifconfig | grep HWaddr | awk '{print $5}' | sed 's/://g' | sort -u")
        for mac in stdout.split("\n"):
            mac = mac.strip()
            if len(mac) <= 0: continue
            if mac == "000000000000": continue # Occasionally we see MACs that are all 0 from badly configured NICs/bonds
            mac_list.append(mac)
    if len(mac_list) > 0:
        mac_list.sort()
        mac_addr = mac_list[0]
    else:
        mac_addr = "UNKNOWN"

    # Get my uptime
    uptime = -1
    if platform.system().lower().startswith("win"):
        retcode, stdout, stderr = libsf.RunCommand("wmic os get lastbootuptime | find /v \"Last\"")
        try:
            uptime = int(time.time() - libsf.CimDatetimeToTimestamp(stdout.strip()))
        except ValueError:
            pass
    else:
        retcode, stdout, stderr = libsf.RunCommand("cat /proc/uptime | awk '{print $1}'")
        try:
            uptime = int(math.ceil(float(stdout.strip())))
        except ValueError:
            pass

    # Check memory usage
    mem_usage = "-1"
    if platform.system().lower().startswith("win"):
        mem_total = 1
        mem_free = 0
        retcode, stdout, stderr = libsf.RunCommand("wmic os get totalvisiblememorysize,freephysicalmemory /value")
        for line in stdout.split("\n"):
            line = line.strip()
            if len(line) <= 0: continue
            pieces = line.split("=")
            if pieces[0] == "FreePhysicalMemory":
                mem_free = float(pieces[1])
                continue
            if pieces[0] == "TotalVisibleMemorySize":
                mem_total = float(pieces[1])
                continue
        if mem_total > 0:
            mem_usage = round(100 - (mem_free*100) / mem_total, 1)
    else:
        try:
            retcode, stdout, stderr = libsf.RunCommand("cat /proc/meminfo | grep -m1 MemTotal | awk {'print $2'}")
            mem_total = float(stdout.strip())
            retcode, stdout, stderr = libsf.RunCommand("cat /proc/meminfo | grep -m1 MemFree | awk {'print $2'}")
            mem_free = float(stdout.strip())
            retcode, stdout, stderr = libsf.RunCommand("cat /proc/meminfo | grep -m1 Buffers | awk {'print $2'}")
            mem_buff = float(stdout.strip())
            retcode, stdout, stderr = libsf.RunCommand("cat /proc/meminfo | grep -m1 Cached | awk {'print $2'}")
            mem_cache = float(stdout.strip())

            mem_usage = round(100 - ((mem_free + mem_buff + mem_cache) * 100) / mem_total, 1)
        except ValueError: pass

    # Check CPU usage
    cpu_usage = "-1";
    if platform.system().lower().startswith("win"):
        retcode, stdout, stderr = libsf.RunCommand("wmic cpu get loadpercentage /value")
        for line in stdout.strip().split("\n"):
            line = line.strip()
            if len(line) <= 0: continue
            pieces = line.split("=")
            if pieces[0] == "LoadPercentage":
                try:
                    cpu_usage = round(float(pieces[1]), 1)
                except ValueError:
                    pass
                break
    else:
        try:
            retcode, stdout, stderr = libsf.RunCommand("top -b -d 2 -n 2 | grep Cpu | tail -1")
            m = re.search("(\d+\.\d+).id", stdout)
            if (m):
                cpu_usage = round(100.0 - float(m.group(1)), 1)
        except ValueError: pass

    # Check if vdbench is running here
    vdbench_count = -1
    if platform.system().lower().startswith("win"):
        retcode, stdout, stderr = libsf.RunCommand("wmic process where name=\"java.exe\" get commandline /value | find /c \"vdbench\"")
    else:
        retcode, stdout, stderr = libsf.RunCommand("ps -ef | grep -v grep | grep java | grep vdbench | wc -l")
    try:
        vdbench_count = int(stdout.strip())
    except ValueError:
        pass

    # See if we have a vdbench last exit status
    vdbench_exit = -1
    if platform.system().lower().startswith("win"):
        vdb_exit_path = r"C:\vdbench\last_vdbench_exit"
    else:
        vdb_exit_path = "/opt/vdbench/last_vdbench_exit"
    try:
        with open(vdb_exit_path, "r") as f:
            stat_text = f.read()
        vdbench_exit = int(stat_text.strip())
    except IOError:
        pass
    except ValueError:
        pass

    # Get the OS platform
    if platform.system().lower().startswith("win"):
        os_simple = "windows"
        ver = sys.getwindowsversion()
        if ver.major == 6 and ver.minor == 1:
            osdetail = "Windows2008R2"
        elif ver.major == 6 and ver.minor == 2:
            # Windows 8.1 lies about its version so we must double check
            retcode, stdout, stderr = libsf.RunCommand("ver")
            for line in stdout.split("\n"):
                m = re.search("Version (\S+)", line)
                if m:
                    pieces = m.group(1).split(".")
                    if pieces[1] == "2":
                        osdetail = "Windows2012"
                    elif pieces[1] == "3":
                        osdetail = "Windows2012R2"
                    else:
                        osdetail = "Windows " + m.group(1)
        elif ver.major == 6 and ver.minor == 3:
            osdetail = "Windows2012R2"

    elif platform.system().lower().startswith("linux"):
        os_simple = "linux"
        osdetail = platform.linux_distribution()[0] + platform.linux_distribution()[1]
    else:
        os_simple = "Unknown"
        osdetail = "Unknown"

    # My info to broadcast
    my_info = dict()
    my_info["timestamp"] = int(math.ceil(time.time()))
    my_info["ip"] = local_ip
    my_info["mac"] = mac_addr
    my_info["hostname"] = hostname
    my_info["vdbench_count"] = vdbench_count
    my_info["vdbench_last_exit"] = vdbench_exit
    my_info["group"] = group_name
    my_info["mem_usage"] = mem_usage
    my_info["cpu_usage"] = cpu_usage
    my_info["uptime"] = uptime
    my_info["os"] = os_simple
    my_info["os_detail"] = osdetail

    # Get the list of all broadcast networks to send out to
    bcasts = []
    for ip, subnet in iplist:
        bcasts.append(libsf.CalculateBroadcast(ip, subnet))

    # Send out to each network
    data = json.dumps(my_info)
    print "Sending " + data + " to " + ", ".join(bcasts)
    for dest in bcasts:
        try:
            s.sendto(data, (dest, MYPORT))      # It would be convinient to use '<broadcast>' but this doesn't work if there is no default gateway
        except: pass
    #time.sleep(2)
