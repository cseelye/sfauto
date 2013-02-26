
MYPORT = 58083
config_file = "sfclientd.json"

import sys, time
import socket
from socket import *
import commands
import re
import json
import platform

# Open a broadcast socket
s = socket(AF_INET, SOCK_DGRAM)
s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 0))

while True:
    group_name = ""
    try:
        # Read configuration file
        config_handle = open(config_file, "r")
    
        # Remove comments from JSON before loading it
        config_lines = config_handle.readlines();
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
        output = commands.getoutput("virt-what").strip()
        if "kvm" in output: group_name = "KVM"
        elif "hyperv" in output: group_name = "HyperV"
        elif "vmware" in output: group_name = "ESX"
        elif "xen" in output: group_name = "Xen"
        else: group_name = output
    
    # Get my hostname
    hostname = commands.getoutput("hostname").strip()
    
    # Find my IP address
    # Look for the first 192 address, then a 172.25.112 address, then a 172.25.107 address
    local_ip = ''
    for line in commands.getoutput("/sbin/ifconfig").split("\n"):
        m = re.search("inet addr:(\S+)", line)
        if m:
            # Management network in BDR
            if m.group(1).startswith("192"):
                local_ip = m.group(1)
                break
            # CFT private network in VWC
            if m.group(1).startswith("172.25.11"):
                local_ip = m.group(1)
                break
            # CFT public network in VWC
            if m.group(1).startswith("172.25.107"):
                local_ip = m.group(1)
                break
            # ENGR client network in VWC
            if m.group(1).startswith("172.25.106"):
                local_ip = m.group(1)
                break

    # Get the MAC address of the system to use as a unique ID
    # Use the alphabetically first MAC addr - thanks to udev the "first" MAC often changes on reboot in VMs
    mac_addr = commands.getoutput("ifconfig | grep HWaddr | awk '{print $5}' | sed 's/://g' | sort | head -1")
    
    # Get my uptime
    uptime = commands.getoutput("cat /proc/uptime | awk '{print $1}'")
    
    # Check memory usage
    mem_usage = "-1"
    try:
        mem_total = float(commands.getoutput("cat /proc/meminfo | grep -m1 MemTotal | awk {'print $2'}").strip())
        mem_free = float(commands.getoutput("cat /proc/meminfo | grep -m1 MemFree | awk {'print $2'}").strip())
        mem_buff = float(commands.getoutput("cat /proc/meminfo | grep -m1 Buffers | awk {'print $2'}").strip())
        mem_cache = float(commands.getoutput("cat /proc/meminfo | grep -m1 Cached | awk {'print $2'}").strip())
        mem_usage = "%.1f" % (100 - ((mem_free + mem_buff + mem_cache) * 100) / mem_total)
    except: pass
    
    # Check CPU usage
    cpu_usage = "-1";
    try:
        cpu_line = commands.getoutput("top -b -d 2 -n 2 | grep Cpu | tail -1")
        m = re.search("(\d+\.\d+)%id", cpu_line)
        if (m):
                cpu_usage = "%.1f" % (100.0 - float(m.group(1)))
    except Exception as e:
        print e.message;
        pass
    
    # Check if vdbench is running here
    output = commands.getoutput("ps -ef | grep -v grep | grep java | grep vdbench | wc -l")
    vdbench_count = 0
    try: vdbench_count = int(output.strip())
    except: pass
    
    # See if we have a vdbench last exit status
    vdbench_exit = -1
    status, output = commands.getstatusoutput("cat /opt/vdbench/last_vdbench_exit")
    if status == 0:
        try: vdbench_exit = int(output.strip())
        except:pass
    
    # My info to broadcast
    my_info = dict()
    my_info["timestamp"] = repr(time.time())
    my_info["ip"] = local_ip
    my_info["mac"] = mac_addr
    my_info["hostname"] = hostname
    my_info["vdbench_count"] = vdbench_count
    my_info["vdbench_last_exit"] = vdbench_exit
    my_info["group"] = group_name
    my_info["mem_usage"] = mem_usage
    my_info["cpu_usage"] = cpu_usage
    my_info["uptime"] = uptime

    # Get the list of all broadcast networks to send out to
    bcasts = []
    for line in commands.getoutput("ifconfig | grep 'inet ' | grep -v '127.0'").split("\n"):
        m = re.search("Bcast:(\S+)", line);
        if m:
            bcasts.append(m.group(1))
    data = json.dumps(my_info)
    print "Sending " + data + " to " + ",".join(bcasts)
    for dest in bcasts:
        try:
            s.sendto(data, (dest, MYPORT))      # It would be convinient to use '<broadcast>' but this doesn't work if there is no default gateway
        except: pass
    #time.sleep(2)
