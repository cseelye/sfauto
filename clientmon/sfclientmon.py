import MySQLdb;
import select, socket
import json
import time
import re
import sys
sys.path.append("..")
import libsf
from libsf import SfError
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
    line = re.sub("(//.*)", "", line)
    if re.match("^\s*$", line): continue
    new_config_text += line
config_json = json.loads(new_config_text)

listenPort = config_json["listenPort"]
server = config_json["server"]
database = config_json["database"]
username = config_json["username"]
password = config_json["password"]
ipFilter = config_json["ipFilter"]

try:
    monitor = ClientMon(DbServer=server, DbUser=username, DbPass=password, DbName=database, IpFilter=ipFilter)
except SfError as e:
    mylog.error("Could not connect to monitor - " + str(e))
    sys.exit(1)

## Connect to database
#try:
#    db = MySQLdb.connect(host=server, user=username, passwd=password, db=database)
#except MySQLdb.Error as e:
#    print "Error " + str(e.args[0]) + ": " + str(e.args[1])
#    sys.exit(1)
#db_cursor = db.cursor()

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
    result = select.select([s],[],[])
    msg = result[0][0].recv(bufferSize) 
    #print "Received: " + msg
    
    try:
        client_info = json.loads(msg)
    except:
        #print "Invalid JSON"
        continue
    
    # Skip template VMs
    if "template" in client_info["hostname"]: continue
    if "gold" in client_info["hostname"]: continue
    
    # Skip clients whose IP does not match our filter
    if ipFilter and not str(client_info["ip"]).startswith(ipFilter): continue

    # Ignore older clients that are not broadcasting their MAC address
    if "mac" not in client_info.keys(): continue
    
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
    
    #sql = """
    #        INSERT INTO clients 
    #            (
    #                `mac`,
    #                `hostname`, 
    #                `ip`, 
    #                `cpu_usage`,
    #                `mem_usage`,
    #                `vdbench_count`, 
    #                `vdbench_last_exit`, 
    #                `timestamp`,
    #                `group`
    #            ) 
    #            VALUES 
    #            (
    #                '""" + str(client_info["mac"]) + """', 
    #                '""" + str(client_info["hostname"]) + """', 
    #                '""" + str(client_info["ip"]) + """', 
    #                '""" + str(cpu_usage) + """',
    #                '""" + str(mem_usage) + """',
    #                '""" + str(client_info["vdbench_count"]) + """', 
    #                '""" + str(client_info["vdbench_last_exit"]) + """', 
    #                '""" + str(time.time()) + """',
    #                '""" + str(group_name) + """'
    #            ) 
    #            ON DUPLICATE KEY UPDATE
    #                `hostname`='""" + str(client_info["hostname"]) + """',
    #                `ip`='""" + str(client_info["ip"]) + """',
    #                `cpu_usage`='""" + str(cpu_usage) + """',
    #                `mem_usage`='""" + str(mem_usage) + """',
    #                `vdbench_count`='""" + str(client_info["vdbench_count"]) + """', 
    #                `vdbench_last_exit`='""" + str(client_info["vdbench_last_exit"]) + """', 
    #                `timestamp`='""" + str(time.time()) + """',
    #                `group`='""" + str(group_name) + """'
    #        """
    ##print sql + "\n"
    #try:
    #    db_cursor.execute(sql)
    #except MySQLdb.Error as e:
    #    print "Error " + str(e.args[0]) + ": " + str(e.args[1])
    #    continue

