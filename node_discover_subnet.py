#!/usr/bin/env python

"""
This action will discover nodes in a subnet
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.util import ValidateAndDefault, IPv4SubnetType
from libsf import threadutil
from libsf.netutil import IPSubnet
from libsf import SolidFireNodeAPI
from libsf import SolidFireError, UnauthorizedError
import socket

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "subnet": (IPv4SubnetType, None),
})
def NodeDiscoverSubnet(subnet):
    """
    Discover nodes in a subnet

    Args:
        subnet:     the subnet to search, in CIDR or network/netmask
    """
    log = GetLogger()

    all_ips = IPSubnet(subnet).AllHosts()
    log.info("Searching {} IPs for nodes...".format(len(all_ips)))

    pool = threadutil.ThreadPool(64)
    results = []
    for idx, node_ip in enumerate(all_ips):
        results.append(pool.Post(_NodeThread, node_ip))

    found = 0
    for idx, node_ip in enumerate(all_ips):
        try:
            node_info = results[idx].Get()
            if node_info:
                log.info("  {:15}  version {:11}  {}{}{}".format(node_ip,
                                                               node_info["version"],
                                                               node_info["state"],
                                                               " cluster " if node_info["state"] != "Available" else "",
                                                               node_info["cluster"] if node_info["state"] != "Available" else ""))
                found += 1
        except SolidFireError as ex:
            log.error("{}: {}".format(node_ip, str(ex)))

    log.info("Found {} nodes".format(found))
    return True

@threadutil.threadwrapper
def _NodeThread(node_ip):
    """Check if this IP is a node"""
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    known_auth = [
        ("admin", "admin"),
        ("admin", "solidfire")
    ]

    SFCONFIG_PORT = 442

    sock = socket.socket()
    sock.settimeout(0.5)
    try:
        sock.connect((node_ip, SFCONFIG_PORT))
        sock.close()
        log.debug("Port {} socket connect succeeded".format(SFCONFIG_PORT))
    except (socket.timeout, socket.error, socket.herror, socket.gaierror):
        # If sfconfig is not running, this does not look like a node
        return None

    node_info = {}
    for user, passwd in known_auth:
        api = SolidFireNodeAPI(node_ip, user, passwd, maxRetryCount=0)
        try:
            result = api.Call("GetClusterConfig", {}, timeout=2)
            node_info["cluster"] = result["cluster"]["cluster"]
            node_info["name"] = result["cluster"]["name"]
            node_info["state"] = result["cluster"]["state"]
            if "version" in result["cluster"]:
                node_info["version"] = result["cluster"]["version"]
            else:
                result = api.Call("GetVersionInfo", {}, timeout=6)
                node_info["version"] = result["versionInfo"]["sfconfig"]["Version"]
            break
        except UnauthorizedError:
            continue
        except SolidFireError as ex:
            log.debug(str(ex))
            return None

    if not node_info:
        return None

    if not node_info["cluster"]:
        node_info["cluster"] = "Available"

    return node_info


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--subnet", type=IPv4SubnetType, required=True, help="The subnet to search, in either CIDR or network/netmask format")
    args = parser.parse_args_to_dict()

    app = PythonApp(NodeDiscoverSubnet, args)
    app.Run(**args)
