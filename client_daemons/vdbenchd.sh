PARM_PATH=/root/parm
OUT_PATH=/root/output

# Add a signal handler for when we are killed
stopme()
{
    # If we are being stopped, assume this is a gracefull shutdown and make the exit status reflect so
    logger -i -t vdbenchd "Stop requested"
    logger -i -t vdbenchd "Writing 0 to last_vdbench_exit"
    echo "0" > /opt/vdbench/last_vdbench_exit
    logger -s -i -t vdbenchd "Killing vdbench"
    echo "Killing vdbench"
    killall vdbench
    sleep 2
    # Make sure vdbench is dead
    killall -9 vdbench
    for pid in $(ps -ef | grep java | grep vdbench | awk '{print $2}'); do kill -9 $pid; done
    exit 0
}
trap "stopme" TERM INT HUP

logger -i -t vdbenchd "Starting vdbench"

# Start vdbench and record its PID and start time
START_TIME=$(date +"%Y-%m-%d-%H-%M-%S")
/opt/vdbench/vdbench -o $OUT_PATH -f $PARM_PATH &
PID=$!
echo "$PID" > /opt/vdbench/last_vdbench_pid

# Wait for vdbench to finish and record its exit status
wait $PID
STATUS=$?

echo "$STATUS" > /opt/vdbench/last_vdbench_exit
# Keep a copy of the output files if vdbench failed
if [ "$STATUS" -ne "0" ]; then
    mv $OUT_PATH $OUT_PATH.$START_TIME
    chmod -R 644 $OUT_PATH.$START_TIME
fi
logger -i -t vdbenchd "vdbench exited with status $STATUS"
exit $STATUS

