import calendar
import datetime
import json
import MySQLdb
import re
import sys
import time

sys.path.append("..")
import lib.libsf as libsf

mylog.info("Starting " + " ".join(sys.argv))

# Read config info
with open("dogfood_config.json", "r") as f:
    config = json.load(f)

# Get volume stats from cluster
volume_stats = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "ListVolumeStatsByVolume", {} )
print "Inserting stats with timestamp " + str(calendar.timegm(datetime.datetime.strptime(volume_stats["volumeStats"][0]["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").utctimetuple()))

db = MySQLdb.connect(host="192.168.154.10", user="root", passwd="solidfire", db="dogfood")
cursor = db.cursor(MySQLdb.cursors.DictCursor)

# Get the list of columns from the table - these are the keys to look for in the result from the cluster
keys = []
sql = "SHOW COLUMNS FROM cluster_capacity"
cursor.execute(sql)
row = cursor.fetchone()
while row is not None:
    keys.append(row["Field"])
    row = cursor.fetchone()

#keys = [
#    'actualIOPS',
#    'averageIOPSize',
#    'burstIOPSCredit',
#    'clientQueueDepth',
#    'latencyUSec',
#    'nonZeroBlocks',
#    'readBytes',
#    'readLatencyUSec',
#    'readOps',
#    'throttle',
#    'timestamp',
#    'unalignedReads',
#    'unalignedWrites',
#    'volumeID',
#    'volumeSize',
#    'volumeUtilization',
#    'writeBytes',
#    'writeLatencyUSec',
#    'writeOps',
#    'zeroBlocks'
#]

sql = "INSERT INTO volume_capacity ( `" + "`,`".join(sorted(keys)) + "` ) VALUES ( " + ",".join(['%s' for i in xrange(len(keys))]) + " )"
mylog.info(sql)

mylog.info("Inserting stats with timestamp " + str(calendar.timegm(datetime.datetime.strptime(cluster_stats["clusterCapacity"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ").utctimetuple())))
for volume in volume_stats["volumeStats"]:
    values = []
    for k in keys:
        if k not in volume:
            values.append(-1)
            continue

        if k == 'timestamp':
            values.append(calendar.timegm(datetime.datetime.strptime(volume["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ").utctimetuple()))
        else:
            values.append(volume[k])

    try:
        cursor.execute(sql, values)
    except MySQLdb.Error as e:
        print str(e)

cursor.close()
db.commit()
db.close()

mylog.info("Finished " + " ".join(sys.argv))
