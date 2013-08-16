#!/usr/bin/python

# This script will combine multiple Event Logs into one file

# ----------------------------------------------------------------------------
# Configuration

num_eventlogs = 2                       # Number of Event Log files to be merged

eventlog1 = "EventLog_2"  # First event log filename

eventlog2 = "EventLog_3"  # Next event log filename

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import shutil
import time
import datetime
import lib.libsf as libsf
from lib.libsf import mylog



def main():

    # Open the first event log
    log1 = open(eventlog1, 'r')

    # Get the first line of the first event log, and extract the start time
    line = log1.readline()
    firstline1 = line.split(" ", 1)
    timestamp1 = firstline1[0]
    file1_start = datetime.datetime.strptime(timestamp1, "%Y-%m-%dT%H:%M:%S.%fZ")
    print("File 1 start time = " + str(file1_start))

    # Get the last line of the first event log, and extract the end time
    last1 = os.popen("tail -1 " + eventlog1).readline()
    lastline1 = last1.split(" ", 1)
    timestamp1 = lastline1[0]
    file1_end = datetime.datetime.strptime(timestamp1, "%Y-%m-%dT%H:%M:%S.%fZ")
    print("File 1 end time = " + str(file1_end))

    # Open the next file
    log2 = open(eventlog2, 'r')

    # Get the first line of the next event log, and extract the start time
    line2 = log2.readline()
    firstline2 = line2.split(" ", 1)
    timestamp2 = firstline2[0]
    file2_start = datetime.datetime.strptime(timestamp2, "%Y-%m-%dT%H:%M:%S.%fZ")
    print("File 2 start time = " + str(file2_start))

    # Get the last line of the next event log, and extract the end time
    last2 = os.popen("tail -1 " + eventlog2).readline()
    lastline2 = last2.split(" ", 1)
    timestamp2 = lastline2[0]
    file2_end = datetime.datetime.strptime(timestamp2, "%Y-%m-%dT%H:%M:%S.%fZ")
    print("File 2 end time = " + str(file2_end))

    print("First line of file 1 = " + line)
    print("First line of file 2 = " + line2)

    # Close and reopen the next event log, so that the reading of lines starts with the first line.
    log2.close()
    log2 = open(eventlog2, 'r')

    # Merge the files: Open the destination file and copy the first file to it
    merge1 = open('EventLog_merged', 'a')
    shutil.copyfile(eventlog1, 'EventLog_merged')

    # If the end time of the first file is out beyond the start time of the second file,
    # look for the line to start writing from the second file. 
    if (file1_end > file2_start):

        # Indicate that the File 2 entries are not yet being merged.
        merging_file2_entries = 0

        # Read the destination file. If there is no timestamp on a line, write it out to the
        # merge file (may be a continuation of a line). 
        for line2 in log2.readlines():
            entry = line2.split(" ", 1)
            timestamp2 = entry[0]
            if not timestamp2:
                if merging_file2_entries:
                    merge1.write(line2)
                continue

            # Convert the timestamp and compare it to the end time of the first source file.
            # Once it is beyond the file 1 end time, start writing the contents of file 2 to
            # the merge file.
            file2_timestamp = datetime.datetime.strptime(timestamp2, "%Y-%m-%dT%H:%M:%S.%fZ")
            if (file2_timestamp > file1_end):
                merging_file2_entries = 1
                merge1.write(line2)

    else:
        for line2 in log2.readlines():
            merge1.write(line2)

    # Close all logs.
    log1.close()
    log2.close()
    merge1.close()
    exit(0)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = libsf.ScriptTimer()
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)


