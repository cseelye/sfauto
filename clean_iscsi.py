#!/usr/bin/python

"""
This script will log out of volumes and remove persistent iSCSI configuration
information.
"""

import sys,os
import argparse
import multiprocessing
from logging import DEBUG

from libsf import mylog, ParseIpsFromList
from libclient import ClientError, SfClient

def ClientThreadFunction(client_ip, results, index):
    client = SfClient()
    mylog.info(client_ip + ": Connecting to client")
    try:
        client.Connect(client_ip, opts.client_user, opts.client_pass)
    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        return

    mylog.info(client_ip + ": Cleaning iSCSI on client '" + client.Hostname + "'")

    # Whether or not I want /etc/iscsi/iscsid.conf blown away or not.
    default=True
    if opts.nodefault_conf:
        default=False

    try:
        client.CleanIscsi(default_iscsid=default)

    except ClientError as e:
        mylog.error(client_ip + ": " + e.message)
        sys.exit(1)

    results[index] = True
    return

def main():
    """ The main script function. """

    # Run the client operations in parallel if there are enough clients
    if len(opts.client_ips) <= opts.parallel_thresh:
        parallel_clients = 1
    else:
        parallel_clients = opts.parallel_max

    # Start the client threads
    thread_index = 0
    current_threads = []

    manager = multiprocessing.Manager()
    results = manager.dict()

    for client_ip in opts.client_ips:
        results[thread_index] = False
        th = multiprocessing.Process(target=ClientThreadFunction,
                                     args=(client_ip,
                                           results,
                                           thread_index))
        th.start()
        current_threads.append(th)
        thread_index += 1

        # Wait for at least one thread to finish
        while len(current_threads) >= parallel_clients:
            for i in range(len(current_threads)):
                if not current_threads[i].is_alive():
                    del current_threads[i]
                    break

    # Wait for all threads to be done
    for th in current_threads:
        th.join()
    # Check the results
    all_success = True
    for res in results.values():
        if not res:
            all_success = False

    if all_success:
        mylog.passed("Successfully cleaned iSCSI on all clients")
        sys.exit(0)
    else:
        mylog.error("Could not clean iSCSI on all clients")
        sys.exit(1)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    #-- Commandline options --#
    p = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                description="""
clean_iscsi.py - cleanup iSCSI on a client

Use this script to clean up any iSCSI mess on a client.  It will logout of all
volumes and remove any persistent configuration info by default.  There is an
option to leave iscsid.conf alone if you wish.

Examples
========================================

scripts/clean_iscsi.py --client_ips="192.168.140.47,192.168.140.122"

scripts/clean_iscsi.py --client_ips="192.168.140.47,192.168.140.122" --nodefault_conf


    """)

    p.add_argument("--client_ips", help="the IP addresses of the clients")
    p.add_argument("--client_user", default="root",
                   help="the username for the clients [%(default)s]")
    p.add_argument("--client_pass", default="password",
                   help="the password for the clients [%(default)s]")
    p.add_argument("--parallel_thresh", type=int, default=5,
                   help="do not thread clients unless there are more than this many [%(default)d]")
    p.add_argument("--parallel_max", type=int, default=10,
                   help="the max number of client threads to use [%(default)d]")
    p.add_argument("--nodefault_conf", action="store_true", default=False,
                   help="Do NOT change /etc/iscsi/iscsid.conf back to a default")

    p.add_argument("--debug", action="store_true",
                   help="display more verbose messages")

    opts = p.parse_args()

    #-- Some commandline verification --#
    try:
        opts.client_ips = ParseIpsFromList(opts.client_ips)
    except TypeError as e:
        mylog.error(e)
        sys.exit(1)

    if not opts.client_ips:
        mylog.error("Please supply at least one client IP address")
        sys.exit(1)

    if opts.debug != None:
        mylog.console.setLevel(DEBUG)

    #-- Do Stuff --#
    try:
        main()

    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

    sys.exit(0)
