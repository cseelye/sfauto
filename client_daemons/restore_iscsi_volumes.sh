#!/bin/bash

# Execute discovery on all known IPs
for ip in `ls /etc/iscsi/send_targets/ | cut -d',' -f1`; do
	logger -i -t restore_iscsi "Discovering targets on $ip"
	iscsiadm -m discovery -t sendtargets -p $ip
done

# Log in to all found volumes
logger -i -t restore_iscsi "Logging in to all targets"
iscsiadm -m node -L all
