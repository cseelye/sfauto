from datetime import datetime
import json
import MySQLdb
import re
import string
import sys
import time
import xlsxwriter

sys.path.append("..")
import lib.libsf as libsf
from lib.libsf import mylog

# Get the volume stats from the database
#volumes = {}
volumes_by_timestamp = {}
date_samples = {}
db = MySQLdb.connect(host="192.168.154.50", user="root", passwd="solidfire", db="dogfood")
cursor = db.cursor(MySQLdb.cursors.DictCursor)
sql = "SELECT volumeID,nonZeroBlocks,timestamp FROM volume_capacity"
cursor.execute(sql)
row = cursor.fetchone()
while row is not None:
    # Collapse all of the samples on a single day into one
    timestamp = row["timestamp"]
    day = time.localtime(timestamp).tm_yday
    if day in date_samples.keys():
        timestamp = date_samples[day]
    else:
        date_samples[day] = timestamp
    
    if timestamp not in volumes_by_timestamp:
        volumes_by_timestamp[timestamp] = {}
    volumes_by_timestamp[timestamp][row['volumeID']] = row['nonZeroBlocks']
    
    row = cursor.fetchone()

last_timestamp = sorted(volumes_by_timestamp.keys())[-1]
last_sample = volumes_by_timestamp[last_timestamp]

# Read config info
with open("dogfood_config.json", "r") as f:
    config = json.load(f)

# Get volume IDs/names from cluster
volume_list = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "ListActiveVolumes", {} )
id2name = {}
for volume in volume_list['volumes']:
    id2name[volume['volumeID']] = volume['name']
volume_ids = sorted(id2name.keys())
cluster_capacity = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "GetClusterCapacity", {} )

# Create the workbook
workbook = xlsxwriter.Workbook("dogfood_volumes2.xlsx")
try:
    bold_format = workbook.add_format({'bold' : True})
    num_format = workbook.add_format({'num_format' : '###0.0'})
    date_format = workbook.add_format({'num_format' : 'mmm dd'})
    percent_format = workbook.add_format({'num_format' : '#0.0%'})

    
    # First tab
    worksheet = workbook.add_worksheet("UsageDataByVolume")

    maxrow = len(volumes_by_timestamp.keys()) + 1
    maxcol = len(volume_ids) + 2
    row = 0
    col = 0
    worksheet.write(row, col, "SampleTimestamp", bold_format)
    col += 1
    worksheet.write(row, col, "SampleTime", bold_format)
    volname2col = {}
    for vid in volume_ids:
        col += 1
        worksheet.write(row, col, id2name[vid], bold_format)
        volname2col[id2name[vid]] = col

    worksheet.set_column(0, 1, 17)
    worksheet.set_column(1, 1, 12)
    worksheet.set_column(2, col, 14)

    row = 1
    col = 0
    for timestamp in sorted(volumes_by_timestamp.keys()):
        worksheet.write(row, col, timestamp)
        col += 1
        worksheet.write_datetime(row, col, datetime.fromtimestamp(timestamp), date_format)
        col += 1
        for volume_id in volume_ids:
            sample = 0
            if volume_id in volumes_by_timestamp[timestamp]:
                sample = volumes_by_timestamp[timestamp][volume_id]
            worksheet.write(row, col, float(sample)*4096.0/1000000000.0, num_format)
            col += 1
        row += 1
        col = 0

    # Second tab
    source_sheet = worksheet.get_name()
    worksheet2 = workbook.add_worksheet("HistoryByVolume")

    chart_cols = 3
    insert_row = 0
    insert_col = 0
    count = 0
    for vid in sorted(last_sample, key=last_sample.get, reverse=True):
        chart = workbook.add_chart({"type" : "scatter", "subtype": "straight_with_markers"})
        source_col = volname2col[id2name[vid]]
        if source_col < 26:
            column_letter = string.uppercase[source_col]
        else:
            column_letter = string.uppercase[source_col / 26 - 1] + string.uppercase[source_col % 26]
        chart.add_series({"name" : id2name[vid],
                          "categories" : "=" + source_sheet + "!$B$2:$B$" + str(maxrow),
                          "values" : "=" + source_sheet + "!$" + column_letter + "$2:$" + column_letter + "$" + str(maxrow),
                          "line" : {"width" : 1.5},
                          "marker" : {"type" : "diamond", "size" : 3}
                          })
        chart.set_title({"name" : id2name[vid]})
        chart.set_x_axis({"name" : "Date", "date_axis" : True})
        chart.set_y_axis({"name" : "GB Used"})
        #chart.set_style(12)
        worksheet2.insert_chart(insert_row, insert_col, chart)

        count += 1
        if count % chart_cols == 0:
            insert_col = 0
            insert_row += 15
        else:
            insert_col = (count % chart_cols) * 8


    # Third tab
    worksheet3 = workbook.add_worksheet("ClusterUsageByVolume")
    row = 0
    col = 0
    worksheet3.write(row, col, "Volume", bold_format)
    col += 1
    worksheet3.write(row, col, "GB Used", bold_format)
    col += 1
    worksheet3.write(row, col, "Percent Used", bold_format)

    worksheet3.set_column(0, 2, 14)

    row = 1
    col = 0
    worksheet3.write(row, col, "Total")
    col += 1
    worksheet3.write(row, col, float(cluster_capacity["clusterCapacity"]["maxUsedSpace"]) / 1000000000.0, num_format)
    col += 1
    worksheet3.write(row, col, 1, percent_format)

    row += 1
    col = 0
    for vid in sorted(last_sample, key=last_sample.get, reverse=True):
        worksheet3.write(row, col, id2name[vid])
        col += 1
        worksheet3.write(row, col, float(last_sample[vid])*4096.0/1000000000.0, num_format)
        col += 1
        worksheet3.write(row, col, "=$B$" + str(row+1) + "/$B$2", percent_format)
        
        row += 1
        col = 0

    col = 0
    worksheet3.write(row, col, "Unused")
    col += 1
    worksheet3.write(row, col, "=B2 - SUM(B3:B" + str(maxcol-1) + ")", num_format)
    col += 1
    worksheet3.write(row, col, "=B" + str(row+1) + "/B2", percent_format)

    chart = workbook.add_chart({"type" : "pie"})
    chart.add_series({
        "name" : "Volume Usage",
        "categories" : [worksheet3.get_name(), 2, 0, 2 + len(volume_ids), 0],
        "values" : [worksheet3.get_name(), 2, 2, 2 + len(volume_ids), 2]
    })
    worksheet3.insert_chart(0, 3, chart)


finally:
    workbook.close()
