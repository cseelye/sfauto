import datetime
import json
import MySQLdb
import re
import string
import sys

import xlsxwriter

sys.path.append("..")
import lib.libsf as libsf
from lib.libsf import mylog

# Get the volume stats from the database
volumes = {}
db = MySQLdb.connect(host="192.168.154.50", user="root", passwd="solidfire", db="dogfood")
cursor = db.cursor(MySQLdb.cursors.DictCursor)
sql = "SELECT volumeID,nonZeroBlocks,timestamp FROM volume_capacity"
cursor.execute(sql)
row = cursor.fetchone()
while row is not None:
    volume_id = row['volumeID']
    sample = {}
    sample["timestamp"] = row['timestamp']
    sample["nonZeroBlocks"] = row['nonZeroBlocks']
    if volume_id not in volumes:
        volumes[volume_id] = []
    volumes[volume_id].append(sample)
    row = cursor.fetchone()

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
workbook = xlsxwriter.Workbook("dogfood_volumes.xlsx")
try:
    bold_format = workbook.add_format({'bold' : True})
    num_format = workbook.add_format({'num_format' : '###0.0'})
    date_format = workbook.add_format({'num_format' : 'mmm dd'})
    percent_format = workbook.add_format({'num_format' : '#0.0%'})

    worksheet = workbook.add_worksheet("UsageDataByVolume")

    row = 0
    col = 0
    worksheet.write(row, col, "SampleTimestamp", bold_format)
    col += 1
    worksheet.write(row, col, "SampleTime", bold_format)
    for vid in volume_ids:
        col += 1
        worksheet.write(row, col, id2name[vid], bold_format)

    worksheet.set_column(0, 0, 17)
    worksheet.set_column(0, 1, 12)
    worksheet.set_column(2, col, 14)

    row = 1
    col = 0
    maxrow = 0
    maxcol = 0
    for vid in volume_ids:
        if vid not in volumes:
            continue
        print "volumeID=" + str(vid)
        samples = volumes[vid]
        samples = sorted(samples, key=lambda s: s['timestamp'])
        for s in samples:
            if col == 0:
                worksheet.write(row, col, s["timestamp"])
                worksheet.write_datetime(row, col+1, datetime.datetime.fromtimestamp(s["timestamp"]), date_format)
                worksheet.write(row, col+2, float(s["nonZeroBlocks"])*4096.0/1000000000.0, num_format)
            else:
                worksheet.write(row, col, float(s["nonZeroBlocks"])*4096.0/1000000000.0, num_format)

            row += 1

        if col == 0:
            col += 3
        else:
            col += 1
        if row > maxrow:
            maxrow = row
        if col > maxcol:
            maxcol = col
        row = 1

    source_sheet = worksheet.get_name()
    worksheet2 = workbook.add_worksheet("HistoryByVolume")

    chart_cols = 3
    insert_row = 0
    insert_col = 0
    col = 2
    count = 0
    for vid in volume_ids:
        chart = workbook.add_chart({"type" : "scatter", "subtype": "straight_with_markers"})
        if col < 26:
            column_letter = string.uppercase[col]
        else:
            column_letter = string.uppercase[col / 26 - 1] + string.uppercase[col % 26]
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

        col += 1
        count += 1
        if count % chart_cols == 0:
            insert_col = 0
            insert_row += 15
        else:
            insert_col = (count % chart_cols) * 8

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

    col = 0
    row = 2
    for source_col in xrange(2, maxcol):
        worksheet3.write(row, col, "=" + source_sheet + "!$" + column_letter + "$1")
        col += 1
        if source_col < 26:
            column_letter = string.uppercase[source_col]
        else:
            column_letter = string.uppercase[source_col / 26 - 1] + string.uppercase[source_col % 26]
        worksheet3.write(row, col, "=" + source_sheet + "!$" + column_letter + "$" + str(maxrow - 1), num_format)
        col += 1
        worksheet3.write(row, col, "=" + source_sheet + "!$" + column_letter + "$" + str(maxrow - 1) + "/$B$2", percent_format )

        row += 1
        col = 0

    worksheet3.write(row, col, "Unused")
    col += 1
    worksheet3.write(row, col, "=B2 - SUM(B3:B" + str(row) + ")", num_format)
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
