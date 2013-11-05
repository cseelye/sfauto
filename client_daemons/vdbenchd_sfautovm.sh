PARM_PATH=/root/parm
OUT_PATH=/root/output

# Add a signal handler for when we are killed
stopme()
{
    # If we are being stopped, assume this is a gracefull shutdown and make the exit status reflect so
    /usr/bin/logger -s -i -t vdbenchd "Stop requested"
    /usr/bin/logger -s -i -t vdbenchd "Writing 0 to last_vdbench_exit"
    echo "0" > /opt/vdbench/last_vdbench_exit
    /usr/bin/logger -s -i -t vdbenchd "Killing vdbench"
    killall vdbench
    sleep 2
    # Make sure vdbench is dead
    killall -q -9 vdbench
    for pid in $(ps -ef | grep java | grep vdbench | awk '{print $2}'); do kill -9 $pid; done
    exit 0
}
trap "stopme" TERM INT

# Don't run unless first boot has finished
if [ ! -e /opt/sfauto/client_daemons/firstbootdone ]; then
    /usr/bin/logger -s -i -t vdbenchd "Not starting vdbench because firstboot is not done"
    exit
fi

# Try to read our VM name from the hypervisor
HYPERVISOR=$(/usr/sbin/virt-what | /usr/bin/head -1 | /usr/bin/awk '{ print tolower($0) }')
if [[ "$HYPERVISOR" == *xen* ]]; then
    VM_NAME=$(/usr/bin/xenstore-read name 2>/dev/null)
fi
if [[ "$HYPERVISOR" == *vmware* ]]; then
    VM_NAME=$(/usr/sbin/vmtoolsd --cmd "info-get guestinfo.hostname" 2>/dev/null)
fi
# Skip running vdbench if the VM name contains gold or template
if [[ -n "$VM_NAME" && ( "$VM_NAME" == *gold* || "$VM_NAME" == *template* ) ]]; then
    /usr/bin/logger -s -i -t vdbenchd "Not starting vdbench because my VM name looks like a template VM"
    exit
fi


/usr/bin/logger -s -i -t vdbenchd "Waiting for system uptime"
WAIT=30
if [[ "$HYPERVISOR" == *hyperv* ]]; then
    WAIT=120
fi

# Wait a short while before starting, to make sure the system is mostly done booting
while [ "$(echo "$(cut -d' ' -f1 /proc/uptime) < $WAIT" | bc)" -gt "0" ]; do
    sleep 5
done

/usr/bin/logger -s -i -t vdbenchd "Starting vdbench"
while true; do
    # Start vdbench and record its PID and start time
    START_TIME=$(date +"%Y-%m-%d-%H-%M-%S")
    /opt/vdbench/vdbench -o $OUT_PATH -f $PARM_PATH &
    PID=$!
    echo "$PID" > /opt/vdbench/last_vdbench_pid

    # Wait for vdbench to finish and record its exit status
    wait $PID
    STATUS=$?


    # Look for known issues caused by clock skew - ntpdate runs at startup and often moves the clock after vdbench has started, which causes vdbench to fail

    COUNT=`grep -c "start time greater than end time" $OUT_PATH/logfile.html`
    if [ "$COUNT" -gt "0" ]; then
        /usr/bin/logger -s -i -t vdbenchd "vdbench failed due to clock skew; restarting"
        sleep 10
        continue
    fi
    RESTART=0
    for outfile in `ls $OUT_PATH/*.stdout.html`; do
        COUNT=`grep -c "Unable to find bucket" $outfile`
        if [ "$COUNT" -gt "0" ]; then
            /usr/bin/logger -s -i -t vdbenchd "vdbench failed due to clock skew; restarting"
            sleep 10
            RESTART=1
            break
        fi
    done
    if [ "$RESTART" -gt "0" ]; then continue; fi

    break
done

echo "$STATUS" > /opt/vdbench/last_vdbench_exit
# Keep a copy of the output files if vdbench failed
if [ "$STATUS" -ne "0" ]; then
    mv $OUT_PATH $OUT_PATH.$START_TIME
fi
/usr/bin/logger -s -i -t vdbenchd "vdbench exited with status $STATUS"
exit $STATUS
