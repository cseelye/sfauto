#!/usr/bin/env python2.7
"""
Discover SolidFire storage nodes by walking through an entire subnet of IP addresses and attempting to call
the SF API on each IP
"""

from __future__ import print_function
import json
from multiprocessing.pool import ThreadPool
import netaddr
import socket
import ssl
import time
import traceback
import urllib2

network = "10.117.144.0"
netmask = "255.255.255.0"
NODE_API_PORT = 442

def discoverNode(node_ip):
    # Try the API to see if this is really a node
    # Nodes that are in a cluster (Active or Pending) will fail with HTTP 401 and fall into the except
    try:
        api_result = callAPI(node_ip, "GetClusterConfig", {})
        node_info = api_result["result"]["cluster"]
        if "version" not in node_info:
            api_result = callAPI(node_ip, "GetVersionInfo", {})
            node_info["version"] = api_result["result"]["versionInfo"]["sfconfig"]["Version"]
        return node_info
    except (urllib2.HTTPError, urllib2.URLError) as ex:
        return {"state" : "Active"}

def callAPI(node_ip, method, params):
    endpoint = "https://{}:{}/json-rpc/5.0".format(node_ip, NODE_API_PORT)
    
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    api_call = json.dumps({'method': method, 'params': params})
    request = urllib2.Request(endpoint, api_call)
    request.add_header('Content-Type', 'application/json-rpc')

    api_response = urllib2.urlopen(request, timeout=6, context=context)
    response_str = api_response.read().decode('ascii')
    response_json = json.loads(response_str)

    return response_json

if __name__ == '__main__':
    start = time.time()

    all_ips = list(netaddr.IPNetwork("{}/{}".format(network, netmask)).iter_hosts())

    # Set up a threadpool to use - very wide pool because each thread does very little and waits on network IO most of the time
    threadpool = ThreadPool(processes=64)

    # Load the threadpool with all of the IPs
    print("Scanning {}/{} ({} hosts)".format(network, netmask, len(all_ips)))
    async_results = {}
    for ip in all_ips:
        res = threadpool.apply_async(discoverNode, args=(str(ip),))
        async_results[ip] = res

    # Get the results from the threads
    active_nodes = []
    available_nodes = {}
    for ip in sorted(async_results.keys()):
        res = async_results[ip]
        try:
            result = res.get(0xFFFF)
            if result:
                if result["state"] == "Available":
                    available_nodes[ip] = result
                else:
                    active_nodes.append(ip)
        except KeyboardInterrupt:
            break
        except Exception as ex:
            print(ex)
            traceback.print_exc()
            continue
    end = time.time()

    # Print tabel of active nodes
    if len(active_nodes) > 0:
        print()
        print("Active Nodes")
        print("------------")
        for ip in active_nodes:
            print(ip)
        print()

    if len(available_nodes.keys()) > 0:
        print("Available Nodes")
        print("---------------")
        for ip, info in available_nodes.iteritems():
            print("{:15} {:13} version {:11}  {}{}{}".format(ip,
                                                          info["name"],
                                                          info["version"],
                                                          info["state"],
                                                          "" if info["state"] == "Available" else "cluster ",
                                                          info["cluster"]))
        print()
    print("Found {} nodes in {:.1f} seconds".format(len(active_nodes) + len(available_nodes), end - start))
