#!/usr/bin/python

# This script will create a simple vdbench input file based on the iscsi volumes
# connected to a list of clients

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

client_ips = [                  # The IP addresses of the clients
    "192.168.000.000",          # --client_ips
    "192.168.000.000",
]

client_user = "root"            # The username for the client
                                # --client_user

client_pass = "password"       # The password for the client
                                # --client_pass

filename = "vdbench_input"      # The name of the file to create
                                # --filename

data_errors = 50                # The number of errors to halt the test after
                                # --data_errors

compratio = 2                   # the compression ratio to use
                                # --compratio

dedupratio = 1                  # the dedup ratio to use
                                # --dedupratio

dedupunit = 4096                # the dedup unit to use
                                # --dedupratio

workload = "rdpct=80,seekpct=0,xfersize=4k"    # The workload specification
                                # --workload

run_time = "8h"               # Run time (how long to run IO)
                                # --run_time

interval = 60                   # How often to report results to the screen
                                # --interval

threads = 4                     # How many threads per sd (queue depth)
                                # --threads

nodatavalidation = False        # Skip data validation
                                # --nodatavalidation

volume_start = 1                # Volume number to start from
                                # --volume_start

volume_end = 0                  # Volume to end at (0 means all volumes)
                                # --volume_end

# ----------------------------------------------------------------------------

import sys,os
from optparse import OptionParser
import paramiko
import re
import socket
import platform
import time
import libsf
from libsf import mylog
from libclient import SfClient, ClientError, OsType

def main():
    global client_ips, client_user, client_pass, filename, data_errors, compratio, dedupratio, dedupunit, workload, run_time, interval, threads, volume_start, volume_end

    # Pull in values from ENV if they are present
    env_enabled_vars = [ "client_ips", "client_user", "client_pass" ]
    for vname in env_enabled_vars:
        env_name = "SF" + vname.upper()
        if os.environ.get(env_name):
            globals()[vname] = os.environ[env_name]
    if isinstance(client_ips, basestring):
        client_ips = client_ips.split(",")

    # Parse command line arguments
    parser = OptionParser()
    parser.add_option("--client_ips", type="string", dest="client_ips", default=",".join(client_ips), help="the IP addresses of the clients")
    parser.add_option("--client_user", type="string", dest="client_user", default=client_user, help="the username for the clients [%default]")
    parser.add_option("--client_pass", type="string", dest="client_pass", default=client_pass, help="the password for the clients [%default]")
    parser.add_option("--filename", type="string", dest="filename", default=filename, help="the file to create")
    parser.add_option("--data_errors", type="string", dest="data_errors", default=data_errors, help="the workload specification")
    parser.add_option("--compratio", type="int", dest="compratio", default=compratio, help="the compression ratio to use")
    parser.add_option("--dedupratio", type="int", dest="dedupratio", default=dedupratio, help="the dedupratio ratio to use")
    parser.add_option("--dedupunit", type="int", dest="dedupunit", default=dedupunit, help="the dedup unit to use")
    parser.add_option("--workload", type="string", dest="workload", default=workload, help="the workload specification")
    parser.add_option("--run_time", type="string", dest="run_time", default=run_time, help="the run time (how long to run vdbench/IO)")
    parser.add_option("--interval", type="int", dest="interval", default=interval, help="how often to report results to the screen")
    parser.add_option("--threads", type="int", dest="threads", default=threads, help="how many threads per sd (queue depth)")
    parser.add_option("--nodatavalidation", action="store_true", dest="nodatavalidation", help="skip data validation")
    parser.add_option("--volume_start", type="int", dest="volume_start", default=volume_start, help="sd number to start at")
    parser.add_option("--volume_end", type="int", dest="volume_end", default=volume_end, help="sd number to finish at (0 means all sds)")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    client_user = options.client_user
    client_pass = options.client_pass
    filename = options.filename
    data_errors = options.data_errors
    workload = options.workload
    run_time = options.run_time
    interval = options.interval
    threads = options.threads
    compratio = options.compratio
    dedupratio = options.dedupratio
    dedupunit = options.dedupunit
    nodatavalidation = options.nodatavalidation
    volume_start = options.volume_start
    volume_end = options.volume_end
    try:
        client_ips = libsf.ParseIpsFromList(options.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)
    if not client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)


    # Open output file and write common params and hds
    outfile = open(filename, 'w')
    outfile.write("data_errors=" + str(data_errors) + "\n")
    if not nodatavalidation:
        outfile.write("validate=read_after_write" + "\n")
    if compratio > 1:
        outfile.write("compratio=" + str(compratio) + "\n")
    if dedupratio > 1:
        outfile.write("dedupratio=" + str(dedupratio) + "\n")
        outfile.write("dedupunit=" + str(dedupunit) + "\n")

    # Connect to clients
    clients = dict()
    for client_ip in client_ips:
        mylog.info("Connecting to " + client_ip)
        client = SfClient()
        try:
            client.Connect(client_ip, client_user, client_pass)
        except ClientError as e:
            mylog.error(e.message)
            sys.exit(1)
        clients[client_ip] = client

    # Write the correct hd line for each client based on client OS
    host_number = 1
    for client_ip in client_ips:
        client = clients[client_ip]
        if client.RemoteOs == OsType.Windows:
            outfile.write("hd=hd" + str(host_number) + ",system=" + client_ip + ",vdbench=C:\\vdbench,shell=vdbench\n")
        else:
            outfile.write("hd=hd" + str(host_number) + ",system=" + client_ip + ",vdbench=/opt/vdbench,user=root,shell=ssh\n")
        outfile.flush()
        host_number += 1

    # Connect to each client and build the list of SDs
    host_number = 1
    for client_ip in client_ips:
        client = clients[client_ip]
        mylog.info("Querying connected iSCSI volumes on " + client.Hostname + "")
        devices = client.GetVdbenchDevices()
        if volume_start <= 0: volume_start = 1
        if volume_end <= 0: volume_end = len(devices)
        for sd_number in xrange(volume_start, volume_end + 1):
            device = devices[sd_number - 1]
            outfile.write("sd=sd" + str(host_number) + "_" + str(sd_number) + ",host=hd" + str(host_number))
            if client.RemoteOs == OsType.Windows:
                outfile.write(",lun=" + device + "\n")
            elif client.RemoteOs == OsType.SunOS:
                outfile.write(",lun=" + device + "\n")
            else:
                outfile.write(",lun=" + device + ",openflags=o_direct\n")
            outfile.flush()
        host_number += 1

    outfile.write("wd=default," + workload + ",sd=sd*\n")
    host_number = 1
    for client_ip in client_ips:
        outfile.write("wd=wd" + str(host_number) + ",host=hd" + str(host_number) + "\n")
        outfile.flush()
        host_number += 1

    outfile.write("rd=default,iorate=max,elapsed=" + run_time + ",interval=" + str(interval) + ",threads=" + str(threads) + "\n")
    outfile.write("rd=rd1,wd=wd*" + "\n")
    outfile.flush()
    outfile.close()


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
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







