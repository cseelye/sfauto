import select, socket
import json
import time
import re
import sys
sys.path.append("..")
import lib.libsf
from lib.libsf import mylog
from lib.libsf import SfError
from clientmon.libclientmon import ClientMon

bufferSize = 2048     # max message size
configFile = "sfclientmon.json"

# Read config file
config_lines = ""
with open(configFile, "r") as config_handle:
    config_lines = config_handle.readlines()

# Remove comments from JSON before loading it
new_config_text = ""
for line in config_lines:
    line = re.sub(r"(//.*)", "", line)
    if re.match(r"^\s*$", line):
        continue
    new_config_text += line
config_json = json.loads(new_config_text)

listenPort = config_json["listenPort"]
server = config_json["server"]
database = config_json["database"]
username = config_json["username"]
password = config_json["password"]
ipFilter = config_json["ipFilter"]
templateNames = config_json["templateNames"]

try:
    monitor = ClientMon(DbServer=server, DbUser=username, DbPass=password, DbName=database, IpFilter=ipFilter)
except SfError as e:
    mylog.error("Could not connect to monitor - " + str(e))
    sys.exit(1)

# Open the listening socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow other processes to bind to the port
try:
    s.bind(('0.0.0.0', listenPort))
except:
    print "Failed to bind to port"
    s.close()
    raise
s.setblocking(0)

# Listen and update the DB whenever we receive a message
while True:
    result = select.select([s], [], [])
    msg = result[0][0].recv(bufferSize)
    print "Received: " + msg

    try:
        client_info = json.loads(msg)
    except:
        print "Invalid JSON"
        continue

    # Skip template VMs
    for template_string in templateNames:
        if template_string in client_info["hostname"]:
            continue

    # Skip clients whose IP does not match our filter
    if ipFilter and not str(client_info["ip"]).startswith(ipFilter):
        continue

    # Ignore older clients that are not broadcasting their MAC address
    if "mac" not in client_info.keys():
        continue

    group_name = ""
    if "group" in client_info.keys():
        group_name = client_info["group"]
    cpu_usage = -1
    if "cpu_usage" in client_info.keys():
        cpu_usage = client_info["cpu_usage"]
    mem_usage = -1
    if "mem_usage" in client_info.keys():
        mem_usage = client_info["mem_usage"]

    try:
        monitor.UpdateClientStatus(client_info["mac"], client_info["hostname"], client_info["ip"], cpu_usage, mem_usage, client_info["vdbench_count"], client_info["vdbench_last_exit"], group_name, time.time())
    except SfError as e:
        mylog.error("Could not update status with monitor - " + str(e))
        continue
