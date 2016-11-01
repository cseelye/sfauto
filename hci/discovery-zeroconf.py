#!/usr/bin/env python2.7
"""
Discover SolidFire storage nodes by using DNS-SD to find advertised node and bootstrap API services
"""

from __future__ import print_function
import socket
import threading
import time
from zeroconf import ServiceBrowser, ServiceStateChange, Zeroconf

SERVICE_TYPE_BOOTSTRAP_API = "_sf-btstrp-api._tcp.local."
SERVICE_TYPE_NODE_API = "_sf-node-api._tcp.local."

def on_service_state_change(zeroconf, service_type, name, state_change):

    if state_change == ServiceStateChange.Removed:
        info = zeroconf.get_service_info(service_type, name)
        if info:
            addr = socket.inet_ntoa(info.address)
            with result_lock:
                if addr in results:
                    del results[addr]

    if state_change == ServiceStateChange.Added:
        info = zeroconf.get_service_info(service_type, name)
        if info:
            print("Found service: {}".format(info))
            addr = socket.inet_ntoa(info.address)
            hostname = info.server.split(".")[0]
            api_type = info.type.split(".")[0]
            endpoints = map(float, info.properties["endpoints"].split(","))

            with result_lock:
                if addr not in results:
                    results[addr] = {}
                if "apis" not in results[addr]:
                    results[addr]["apis"] = {}

                results[addr]["hostname"] = hostname
                results[addr]["version"] = info.properties["version"]
                results[addr]["apis"][api_type] = {}
                results[addr]["apis"][api_type]["port"] = info.port
                results[addr]["apis"][api_type]["path"] = info.properties["path"]
                results[addr]["apis"][api_type]["endpoints"] = endpoints
                results[addr]["apis"][api_type]["name"] = info.properties["friendly_name"]


if __name__ == '__main__':
    results = {}
    result_lock = threading.Lock()

    zeroconf = Zeroconf()
    browser = ServiceBrowser(zeroconf, SERVICE_TYPE_BOOTSTRAP_API, handlers=[on_service_state_change])
    browser2 = ServiceBrowser(zeroconf, SERVICE_TYPE_NODE_API, handlers=[on_service_state_change])

    time.sleep(3)
    zeroconf.close()

    print()
    for addr in sorted(results.keys()):
        print ("-"*60)
        print("{} ({})".format(addr, results[addr]["hostname"]))
        for api_type in sorted(results[addr]["apis"].keys()):
            api = results[addr]["apis"][api_type]
            api_url = "https://{}:{}{}{}".format(addr, api["port"], api["path"], max(api["endpoints"]))
            print("    {:>13}: {}".format(api["name"], api_url))
    print ("-"*60)
