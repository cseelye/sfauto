PARM_PATH=/root/parm
OUT_PATH=/root/output

# Add a signal handler for when we are killed with TERM (from upstart)
stopme()
{
    # If we are being stopped, assume this is a gracefull shutdown and make the exit status reflect so
    logger -i -t vdbenchd "Stop requested"
    logger -i -t vdbenchd "Writing 0 to last_vdbench_exit"
    echo "0" > /opt/vdbench/last_vdbench_exit
    logger -s -i -t vdbenchd "Killing vdbench"
    echo "Killing vdbench"
    killall -9 vdbench
    killall -9 java
    #for pid in $(ps -ef | grep java | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
    exit 0
}
trap "stopme" TERM INT

logger -i -t vdbenchd "Starting vdbench"

# Wait a short while before starting, to make sure the system is mostly done booting
UPTIME=$(awk '{print $1}' /proc/uptime)
while [ "$UPTIME" -lt "120" ]; do
	sleep 5
	UPTIME=$(awk '{print $1}' /proc/uptime)
done

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
        logger -i -t vdbenchd "vdbench failed due to clock skew; restarting"
        sleep 10
        continue
    fi
    RESTART=0
    for outfile in `ls $OUT_PATH/*.stdout.html`; do
        COUNT=`grep -c "Unable to find bucket" $outfile`
        if [ "$COUNT" -gt "0" ]; then
            logger -i -t vdbenchd "vdbench failed due to clock skew; restarting"
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
logger -i -t vdbenchd "vdbench exited with status $STATUS"
exit $STATUS

