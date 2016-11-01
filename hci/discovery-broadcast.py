#!/usr/bin/env python2.7
"""
Discover SolidFire storage nodes by listening to the bootstrapper broadcast that storage nodes send each other
"""

import json
import socket

# From ClusterLocator.h
kBeaconListenPort = 2010
kMaxMessageSize = 1024

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", kBeaconListenPort))
found_nodes = {}
while True:
    data, addr = sock.recvfrom(kMaxMessageSize)
    try:
        node = json.loads(data)
    except ValueError:
        print "Received invalid JSON from {}: {}".format(addr[0], data)
        continue
    # Ignore nodes associated with a cluster
    if node["cluster"]:
        continue
    # Ignore nodes we have already found
    if node["ip"] in found_nodes and found_nodes[node["ip"]] == node:
        continue
    found_nodes[node["ip"]] = node
    print "Found available node {}".format(node["ip"], node["version"])

# Sample broadcast packet:
# {
#   'nodeType': 'SF3010',
#   'uuid': '4C4C4544-0053-3110-8053-B6C04F435831',
#   'ip': '10.10.5.124',
#   'hostname': 'BDR-EN124',
#   'cluster': '',
#   'version': '9.0.0.1554',
#   'chassisType': 'R620',
#   'mip': '192.168.133.124'
# }
