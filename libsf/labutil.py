#!/usr/bin/env python2.7
"""SF lab related info"""

import re
import time
from . import AutotestAPI, SolidFireError
from .logutil import GetLogger

# AT2 lookups are slow, so cache results and reuse them
class AT2Resources(object):
    """
    Caching list of AT2 resources
    """
    def __init__(self):
        self.api = AutotestAPI()
        self.lastRefresh = time.time()
        self.resourceCache = {}

    def ListResources(self):
        """
        Get a list of node and client resources from AT2

        Returns:
            A dictionary of clients and nodes (dict)
        """
        if not self.resourceCache:
            GetLogger().debug("Refreshing AT2 resource cache")
            try:
                self.resourceCache["clients"] = self.api.ListClientPool()
                self.resourceCache["nodes"] = self.api.ListNodePool()
            except SolidFireError:
                pass
        return self.resourceCache
_AT2_RESOURCES = AT2Resources()

def GetIPMIAddresses(managementIPs):
    """
    Get the IPMI address of a list of systems, given their management IP addresses

    Args:
        managementIPs:  the management IPs of the system (list of str)

    Returns:
        A dictionary of mip => IPMI IP (dict)
    """
    found_ips = dict.fromkeys(managementIPs, None)

    # Try to look up resources in AT2
    at2_resources = _AT2_RESOURCES.ListResources()

    if at2_resources:
        for res in at2_resources["nodes"] + at2_resources["clients"]:
            if "oneGigIP" in res and res["oneGigIP"] in managementIPs:
                found_ips[res["oneGigIP"]] = res["iDRACIP"]

    # Make sure we have a value for every IP
    for mip, ipmi_ip in found_ips.iteritems():
        if not ipmi_ip:
            # If we could not do the lookup in AT2, guess at the IP based on known subnets
        
            # 192 Boulder subnet, replace the first three octets
            if mip.startswith("192.168"):
                ipmi_ip = re.sub(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", r"172.24.165.\4", mip)
        
            # Special case for build rack subnet
            elif mip.startswith("172.24.58"):
                ipmi_ip = re.sub(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", r"\1.\2.59.\4", mip)
        
            # Other subnets, add a 1 in front of the 3rd subnet
            else:
                ipmi_ip = re.sub(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", r"\1.\2.1\3.\4", mip)
            found_ips[mip] = ipmi_ip
    
    return found_ips

def GetNetworkProfile(managementIPs):
    """
    Get the network info (netmask, gateway, hostname, PXE server, etc) for the list of systems

    Args:
        managementIPs:  the management IPs of the system (list of str)

    Returns:
        A dictionary of mip => network info dict (dict)
    """
    at2_resources = _AT2_RESOURCES.ListResources()
    found = dict.fromkeys(managementIPs, None)
    for res in at2_resources["nodes"] + at2_resources["clients"]:
        if "oneGigIP" in res and res["oneGigIP"] in managementIPs:
            found[res["oneGigIP"]] = {}
            found[res["oneGigIP"]]["mip"] = res["oneGigIP"]
            found[res["oneGigIP"]]["netmask"] = res["networkProfile"]["netmask1G"]
            found[res["oneGigIP"]]["gateway"] = res["networkProfile"]["gateway1G"]
            found[res["oneGigIP"]]["pxe"] = res["networkProfile"]["pxeServerAddress"]
            found[res["oneGigIP"]]["ipmi"] = res["iDRACIP"]
            found[res["oneGigIP"]]["hostname"] = res.get("nodeName", None) or res.get("clientName", None)

    if not all(v != None for k,v in found.iteritems()):
        raise SolidFireError("Could not find network info for all IPs")

    return found
