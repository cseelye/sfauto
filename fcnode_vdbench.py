import re
import lib.libsf as libsf
from lib.libsf import mylog


#
# This will go through the connected iSCSI devices and create a vdbench sd definition
# for the first LUN 0 of any iSCSI target
#


ssh = libsf.ConnectSsh("172.26.65.168", "sfadmin", "fastN7furious")
stdin, stdout, stderr = libsf.ExecSshCommand(ssh, "iscsiadm -m session -P3 | grep 'Target: \|Attached scsi disk\|Lun: '")
data = stdout.readlines()
iqn = None
lun = -1
devs_by_length = {}
dev2iqn = {}
for line in data:
    line = line.strip()
    if not line:
        continue
    m = re.search("^Target: (\S+)", line)
    if m:
        iqn = m.group(1)
        pieces = iqn.split(".")
        volume_id = int(pieces[-1])

# Uncomment these two lines and change the if statement to exclude particular volumes
#        if volume_id < 101:
#            iqn = None

        lun = -1
        continue
    m = re.search("Lun: (\d+)", line)
    if m:
        lun = int(m.group(1))
        continue
    m = re.search("Attached scsi disk (\w+)", line)
    if m:
        disk = m.group(1)

# Change this next line to match a particular LUN
        if lun == 0 and iqn:
            length = len(disk)
            if length not in devs_by_length.keys():
                devs_by_length[length] = []
            devs_by_length[length].append(disk)
            dev2iqn[disk] = iqn
            iqn = None
        continue


disks = []
for length in sorted(devs_by_length.keys(), key=int):
    devs_by_length[length].sort()
    disks += devs_by_length[length]

for i, dev in enumerate(disks):
    print "sd=sd" + str(i+1) + ",lun=/dev/" + dev + ",openflags=o_direct"
