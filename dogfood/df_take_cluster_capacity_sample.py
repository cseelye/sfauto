import calendar
import datetime
import json
from logging.handlers import RotatingFileHandler
import MySQLdb
import re
import sys
import time

sys.path.append("..")
import lib.libsf as libsf
from lib.libsf import mylog

mylog.info("Starting " + " ".join(sys.argv))

# Read config info
with open("dogfood_config.json", "r") as f:
    config = json.load(f)

# Get volume stats from cluster
cluster_stats = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "GetClusterCapacity", {} )

db = MySQLdb.connect(host="192.168.154.50", user="root", passwd="solidfire", db="dogfood")
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
#    'activeBlockSpace',
#    'activeSessions',
#    'averageIOPS',
#    'clusterRecentIOSize',
#    'currentIOPS',
#    'maxIOPS',
#    'maxOverProvisionableSpace',
#    'maxProvisionedSpace',
#    'maxUsedMetadataSpace',
#    'maxUsedSpace',
#    'nonZeroBlocks',
#    'peakActiveSessions',
#    'peakIOPS',
#    'provisionedSpace',
#    'timestamp',
#    'totalOps',
#    'uniqueBlocks',
#    'uniqueBlocksUsedSpace',
#    'usedMetadataSpace',
#    'usedMetadataSpaceInSnapshots',
#    'usedSpace',
#    'zeroBlocks'
#]

sql = "INSERT INTO cluster_capacity ( `" + "`,`".join(sorted(keys)) + "` ) VALUES ( " + ",".join(['%s' for i in xrange(len(keys))]) + " )"
mylog.info(sql)

values = []
for k in keys:
    if k not in cluster_stats["clusterCapacity"]:
        values.append(-1)
        continue

    if k == 'timestamp':
        values.append(calendar.timegm(datetime.datetime.strptime(cluster_stats["clusterCapacity"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ").utctimetuple()))
    else:
        values.append(cluster_stats["clusterCapacity"][k])

mylog.info("Inserting stats with timestamp " + str(calendar.timegm(datetime.datetime.strptime(cluster_stats["clusterCapacity"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ").utctimetuple())))
try:
    cursor.execute(sql, values)
except MySQLdb.Error as e:
    print str(e)

cursor.close()
db.commit()
db.close()

mylog.info("Finished " + " ".join(sys.argv))
