import MySQLdb;
import select, socket
import json
import time
import sys

port = 58083        # port to listen on
bufferSize = 2048     # max message size

# Connect to database
try:
    db = MySQLdb.connect(host="localhost", user="root", passwd="password", db="monitor")
except MySQLdb.Error as e:
    print "Error " + str(e.args[0]) + ": " + str(e.args[1])
    sys.exit(1)
db_cursor = db.cursor()

# Open the listening socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow other processes to bind to the port
try:
    s.bind(('0.0.0.0', port))
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
    
    sql = """
            INSERT INTO clients 
                (
                    `mac`,
                    `hostname`, 
                    `ip`, 
                    `cpu_usage`,
                    `mem_usage`,
                    `vdbench_count`, 
                    `vdbench_last_exit`, 
                    `timestamp`,
                    `group`
                ) 
                VALUES 
                (
                    '""" + str(client_info["mac"]) + """', 
                    '""" + str(client_info["hostname"]) + """', 
                    '""" + str(client_info["ip"]) + """', 
                    '""" + str(cpu_usage) + """',
                    '""" + str(mem_usage) + """',
                    '""" + str(client_info["vdbench_count"]) + """', 
                    '""" + str(client_info["vdbench_last_exit"]) + """', 
                    '""" + str(time.time()) + """',
                    '""" + str(group_name) + """'
                ) 
                ON DUPLICATE KEY UPDATE
                    `hostname`='""" + str(client_info["hostname"]) + """',
                    `ip`='""" + str(client_info["ip"]) + """',
                    `cpu_usage`='""" + str(cpu_usage) + """',
                    `mem_usage`='""" + str(mem_usage) + """',
                    `vdbench_count`='""" + str(client_info["vdbench_count"]) + """', 
                    `vdbench_last_exit`='""" + str(client_info["vdbench_last_exit"]) + """', 
                    `timestamp`='""" + str(time.time()) + """',
                    `group`='""" + str(group_name) + """'
            """
    #print sql + "\n"
    try:
        db_cursor.execute(sql)
    except MySQLdb.Error as e:
        print "Error " + str(e.args[0]) + ": " + str(e.args[1])
        continue
