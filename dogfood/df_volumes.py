import datetime
import json
import re
import string
import sys

sys.path.append("..")
import lib.libsf as libsf

# Read config info
with open("dogfood_config.json", "r") as f:
    config = json.load(f)

cluster_capacity = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "GetClusterCapacity", {} )

volume_list = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "ListActiveVolumes", {} )
volume_ids = []
id2name = {}
for volume in volume_list['volumes']:
    id2name[volume['volumeID']] = volume['name']

volume_stats = libsf.CallApiMethod(config["mvip"], config["username"], config["password"], "ListVolumeStatsByVolume", {} )

print
print "%18s   %-13s   %-13s" % ("Volume Name", "Used Capacity", "Provisioned Capacity")
print

for volume in reversed(sorted(volume_stats["volumeStats"], key=lambda v: v["nonZeroBlocks"])):
    print "%18s   %-13s   %-13s" % (id2name[volume["volumeID"]], libsf.HumanizeBytes(volume["nonZeroBlocks"]*4096), libsf.HumanizeBytes(volume["volumeSize"]))
print
