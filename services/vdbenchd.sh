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
    killall java
    killall vdbench
    for pid in $(ps -ef | grep java | grep -v grep | awk '{print $2}'); do kill -9 $pid; done
    exit 0
}
trap "stopme" TERM

logger -i -t vdbenchd "Starting vdbench"

# Wait a short while before starting
UPTIME=$(awk '{print $1}' /proc/uptime)
while [ "$UPTIME" -lt "120" ]; do
	sleep 5
	UPTIME=$(awk '{print $1}' /proc/uptime)
done

while true; do
    # Start vdbench and record its PID
    /opt/vdbench/vdbench -o $OUT_PATH -f $PARM_PATH &
    PID=$!
    echo "$PID" > /opt/vdbench/last_vdbench_pid

    # Wait for vdbench to finish and record its exit status
    wait $PID
    STATUS=$?
    COUNT=`grep -c "start time greater than end time" /root/vdbench/logfile.html`
    if [ "$COUNT" -gt "0" ]; then
        logger -i -t vdbenchd "vdbench failed due to clock skew; restarting"
        sleep 10
        continue
    fi
    COUNT=`grep -c "Unable to find bucket" /root/vdbench/localhost-0.stdout.html`
    if [ "$COUNT" -gt "0" ]; then
        logger -i -t vdbenchd "vdbench failed due to clock skew; restarting"
        sleep 10
        continue
    fi
        
    break
done
echo "$STATUS" > /opt/vdbench/last_vdbench_exit
logger -i -t vdbenchd "vdbench exited with status $STATUS"
exit $STATUS

