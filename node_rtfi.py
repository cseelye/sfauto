#!/usr/bin/env python2.7

"""
This action will RTFI a list of nodes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SelectionType, BoolType, StrType
from libsf.util import GetFilename, SolidFireVersion, ParseTimestamp
from libsf import sfdefaults, threadutil, pxeutil, labutil
from libsf import SSHConnection, SolidFireError, ConnectionError, InvalidArgumentError, TimeoutError
import json
import random
import time

KNOWN_STATE_TIMEOUTS = {
    "DEFAULT" : 180,
    "PreparePivotRootKexecLoad" : 60,
    "Start" : 30,
    "DriveUnlock" : 300,
    "Backup" : 180,
    "BackupKexecLoad" : 60,
    "UpgradeFirmware" : 300,
    "DriveErase" : 300,
    "Partition" : 30,
    "Image" : 600, # This can take a long time on FDVA on spinning disk
    "Configure" : 90,
    "Restore" : 60,
    "InternalConfiguration" : 20,
    "PostInstallPre" : 300, # Updating ELF headers is slow
    "PostInstall" : 120,
    "PostInstallKexecLoad" : 60,
    "PostInstallKexec" : 180,
    "Stop" : 180,
    "Coldboot" : 600,
}

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "node_ips" : (ItemList(IPv4AddressType), None),
    "repo" : (StrType, None),
    "version" : (StrType, None),
    "rtfi_type" : (SelectionType(["pxe", "irtfi"]), "irtfi"),
    "preserve_networking" : (BoolType, True),
    "image_type" : (SelectionType(["rtfi", "fdva"]), "rtfi"),
    "fail" : (OptionalValueType(StrType), None),
    "ipmi_ips" : (OptionalValueType(ItemList(IPv4AddressType)), None),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "ipmi_user" : (StrType, sfdefaults.ipmi_user),
    "ipmi_pass" : (StrType, sfdefaults.ipmi_pass),
    "netmask" : (OptionalValueType(IPv4AddressType), None),
    "gateway" : (OptionalValueType(IPv4AddressType), None),
})
def RtfiNodes(node_ips,
              repo,
              version,
              rtfi_type,
              preserve_networking,
              image_type,
              fail,
              ipmi_ips,
              username,
              password,
              ipmi_user,
              ipmi_pass,
              netmask,
              gateway):
    """
    RTFI a list of nodes

    Args:
        node_ips:               the IP address of the nodes (string)
        username:               the node admin name (string)
        password:               the node admin password (string)
        repo:                   the repo/branch to RTFI to
        version:                the version number to RTFI to
        preserve_networking:    keep the current hostname/network config of the nodes
        rtfi_type:              type of RTFI (pxe, irtfi)
        image_type:             type of image (rtfi, fdva)
        fail:                   inject a failure during this RTFI state
        ipmi_ips:               the IPMI IPs of the nodes
        ipmi_user:              the IPMI username
        ipmi_pass:              the IPMI password
        netmask:                the netmask for the nodes (only PXE if AT2 is inaccessible)
        gateway:                the gateway for the nodes (only PXE if AT2 is inaccessible)
    """
    log = GetLogger()

    if ipmi_ips and len(node_ips) != len(ipmi_ips):
        raise InvalidArgumentError("If ipmi_ips is specified, it must have the same number of elements as node_ips")

    # Make sure the build exists
    iso_name = "solidfire-{}-{}-{}.iso".format(image_type, repo, version)
    log.info("Checking that {} exists".format(iso_name))
    with SSHConnection("192.168.137.1", "root", "solidfire") as ssh:
        try:
            retcode, _, _ = ssh.RunCommand(r"find /builds/iso/{} -type f -name '*.iso' -exec basename {{}} \; | grep -q {}".format(image_type, iso_name), exceptOnError=False)
        except SolidFireError as ex:
            log.error(str(ex))
            return False
    if retcode != 0:
        log.error("Cannot find build on jenkins. Check http://192.168.137.1/rtfi/ to see if the build is actually there")
        return False

    log.info("Looking up network profile for nodes")
    try:
        net_info = labutil.GetNetworkProfile(node_ips)
    except SolidFireError:
        net_info = {}

    # Add user supplied config info and check that we have all required info
    for idx, node_ip in enumerate(node_ips):
        if ipmi_ips:
            net_info[node_ip]["ipmi"] = ipmi_ips[idx]
        if netmask and gateway:
            net_info[node_ip]["netmask"] = netmask
            net_info[node_ip]["gateway"] = gateway
        if "hostname" not in net_info[node_ip] or not net_info[node_ip]["hostname"]:
            node = SFNode(node_ip, clusterUsername=username, clusterPassword=password)
            try:
                net_info[node_ip]["hostname"] = node.GetHostname()
            except SolidFireError:
                net_info[node_ip]["hostname"] = "node-{}".format(node_ip.replace(".", "-"))

        if rtfi_type == "pxe":
            if not net_info[node_ip].get("ipmi", None):
                raise SolidFireError("Could not find IPMI address for {}".format(node_ip))
            if not net_info[node_ip].get("netmask", None):
                raise SolidFireError("Could not find netmask for {}".format(node_ip))
            if not net_info[node_ip].get("gateway", None):
                raise SolidFireError("Could not find gateway for {}".format(node_ip))

    # Start a thread for each node
    log.info("RTFI {} nodes to {}-{}".format(len(node_ips), repo, version))
    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_NodeThread, rtfi_type, image_type, repo, version, preserve_networking, net_info[node_ip]["pxe"], node_ip, net_info[node_ip]["hostname"], net_info[node_ip]["netmask"], net_info[node_ip]["gateway"], fail, username, password, net_info[node_ip]["ipmi"], ipmi_user, ipmi_pass))

    # Wait for all threads to complete
    allgood = True
    for idx, node_ip in enumerate(node_ips):
        try:
            ret = results[idx].Get()
        except SolidFireError as e:
            log.error("  Error imaging node {}: {}".format(node_ip, e))
            allgood = False
            continue
        if not ret:
            allgood = False

    if allgood:
        log.passed("Successfully RTFI all nodes")
        return True
    else:
        log.error("Could not RTFI all nodes")
        return False


@threadutil.threadwrapper
def _NodeThread(rtfi_type, image_type, repo, version, preserve_networking, pxe_server, node_ip, node_hostname, netmask, gateway, fail, username, password, ipmi_ip, ipmi_user, ipmi_pass):
    """RTFI a node"""
    log = GetLogger()
    SetThreadLogPrefix(node_ip)

    # Generate a random identifier
    agent = "rtfiscript{}".format(random.randint(1000, 9999))

    node = SFNode(node_ip, clusterUsername=username, clusterPassword=password, ipmiIP=ipmi_ip, ipmiUsername=ipmi_user, ipmiPassword=ipmi_pass)
    rtfi_opts = [
        "sf_hostname={}-rtfi".format(node_hostname),
        "sf_agent={}".format(agent),
    ]
    if preserve_networking:
        rtfi_opts.extend(["sf_keep_hostname=1",
                          "sf_keep_network_config=1",
                          ])

    if fail:
        rtfi_opts.append("sf_status_inject_failure={}".format(fail))

    rtfi_options = ",".join(rtfi_opts)

    # PXE boot traditional RTFI
    if rtfi_type == "pxe":
        log.info("Getting MAC address from iDRAC")
        mac_address = node.GetPXEMacAddress()

        log.info("Creating PXE config file")
        pxeutil.DeletePXEFile(mac_address)
        pxeutil.CreatePXEFile(mac_address,
                              repo,
                              version,
                              rtfi_options,
                              imageType=image_type,
                              pxeServer=pxe_server,
                              ip=node_ip,
                              netmask=netmask,
                              gateway=gateway)

        log.info("Power cycling node")
        node.PowerOff()
        node.PowerOn(waitForUp=False)

        start_timeout = 360
        if SolidFireVersion(version) < SolidFireVersion("8.0.0.0"):
            log.warning("This RTFI version does not support monitoring other than power status")
            start_timeout = 960
        elif SolidFireVersion(version) < SolidFireVersion("9.0.0.0"):
            log.warning("This RTFI version supports incomplete status monitoring")

        # Wait for RTFI to complete
        try:
            _MonitorStatusServerPXE(node, startTimeout=start_timeout, agentID=agent)
        except TimeoutError as ex:
            log.error(str(ex))

            # Try to save the RTFI log
            logfile = GetFilename("{}-rtfi.log".format(node_ip))
            try:
                node.SaveRTFILog(logfile)
                log.info("Saved RTFI log to {}".format(logfile))
            except SolidFireError:
                pass

            log.error("Failed to RTFI")
            return False
        finally:
            log.info("Removing PXE config file")
            pxeutil.DeletePXEFile(mac_address)

        node.WaitForOff()

        log.info("Powering on node")
        node.PowerOn(waitForUp=False)

    # in place RTFI
    elif rtfi_type == "irtfi":
        log.info("Calling StartRtfi")
        node.StartRTFI(repo, version, rtfi_options)

        # Wait for RTFI to complete
        _MonitorStatusServerIRTFI(node, agentID=agent)

    # Wait for the node to be fully up
    if preserve_networking:
        node.WaitForUp(initialWait=0, timeout=300)
        log.info("Node is back up")

    return True


def _MonitorStatusServerIRTFI(node, startTimeout=360, timeout=900, agentID=None):
    """
    Monitor RTFI status and return when RTFI is complete
    """
    log = GetLogger()
    log.info("Monitoring RTFI status")
    status = None
    previous_status = []
    state_timeout = KNOWN_STATE_TIMEOUTS["DEFAULT"]
    state_start_time = 0
    start_time = time.time()
    log.debug("Waiting for RTFI status server timeout={}".format(startTimeout))
    while True:
        try:
            status = node.GetAllRTFIStatus()
        except ConnectionError as ex:
            if not ex.IsRetryable() and ex.code not in [404, 403, 54, 60, 61, 110]:
                log.warning(str(ex))
            status = None

        # Timeout if we haven't gotten any status within the startTimeout period
        if not previous_status and time.time() - start_time > startTimeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to start")

        # See if the current state has changed
        if status and status != previous_status:
            new_status = [stat for stat in status if stat not in previous_status]
            
            # Sanity check the status we received
            for stat in new_status:
                log.debug("status={}".format(stat))
                # If the status does not match our agent ID, this is a stale status and it is probably from an earlier RTFI
                if agentID and "sf_agent" in stat and stat["sf_agent"] != agentID and stat["sf_agent"] != "Manual":
                    log.debug("sf_caller does not match agentID={}".format(agentID))
                    raise SolidFireError("Node did not start RTFI (stale status)")

                # If the timestamp from the status is too old, then it is probably from an earlier RTFI, and this RTFI never started
                if ParseTimestamp(stat["time"]) < start_time - 30:
                    log.debug("Status timestamp is too old start_time={} status_time={}".format(start_time, ParseTimestamp(stat["time"])))
                    raise SolidFireError("Node did not start RTFI (stale status)")

            # Show the new status to the user
            for stat in new_status:
                log.info("  {}: {}".format(stat["state"], stat["message"]))

            # Log some info about how long state transitions take
            if previous_status:
                log.debug("PreviousState={} duration={} timeout={}".format(previous_status[-1]["state"], time.time() - state_start_time, state_timeout))
            else:
                log.debug("FirstState duration={}".format(time.time() - start_time))

            previous_status = status
            state_start_time = time.time()
            state_timeout = KNOWN_STATE_TIMEOUTS.get(previous_status[-1]["state"], None) or KNOWN_STATE_TIMEOUTS["DEFAULT"]
            log.debug("CurrentState={} timeout={}".format(previous_status[-1]["state"], state_timeout))
            log.debug("Waiting for a new status")

        # If the status indicates we are finished with iRTFI, break
        if status and status[-1]["state"] == "FinishSuccess":
            break

        # If the status indicates a failure, raise an error
        if status and status[-1]["state"] == "FinishFailure":
            log.error("RTFI failed, last status:\n{}".format(json.dumps(status[-1], indent=2, sort_keys=True)))
            raise SolidFireError("RTFI failed")

        # Timeout if the current state has lasted too long
        if state_start_time > 0 and time.time() - state_start_time > state_timeout:
            raise TimeoutError("Timeout in {} RTFI state".format(previous_status[-1]["state"]))

        # Timeout if the total time has been too long
        if time.time() - start_time > timeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to complete")

def _MonitorStatusServerPXE(node, startTimeout=360, timeout=900, agentID=None):
    """
    Monitor RTFI status and return when RTFI is complete
    """
    log = GetLogger()
    log.info("Monitoring RTFI status")
    status = None
    previous_status = []
    last_power_check = time.time()
    last_api_check = time.time()
    state_timeout = KNOWN_STATE_TIMEOUTS["DEFAULT"]
    state_start_time = 0
    start_time = time.time()
    log.debug("Waiting for RTFI status server timeout={}".format(startTimeout))
    while True:
        try:
            status = node.GetAllRTFIStatus()
        except ConnectionError as ex:
            if not ex.IsRetryable() and ex.code not in [404, 403, 54, 60, 61, 110]:
                log.warning(str(ex))
            status = None

        # Timeout if we haven't gotten any status within the startTimeout period
        if not previous_status and time.time() - start_time > startTimeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to start")

        # If we never got any status, never caught the node powering off, and the node is back up, it probably never PXE booted
        if not previous_status and time.time() - last_api_check > 20 * sfdefaults.TIME_SECOND:
            last_api_check = time.time()
            if node.IsUp():
                raise SolidFireError("Node did not start RTFI (API up)")

        # See if the current state has changed
        if status and status != previous_status:
            new_status = [stat for stat in status if stat not in previous_status]

            # Sanity check the status we received
            for stat in new_status:
                log.debug("status={}".format(stat))
                # If the status does not match our agent ID, this is a stale status and it is probably from an earlier RTFI
                if agentID and "sf_agent" in stat and stat["sf_agent"] != agentID and stat["sf_agent"] != "Manual":
                    log.debug("sf_caller does not match agentID={}".format(agentID))
                    raise SolidFireError("Node did not start RTFI (stale status)")

                # If the timestamp from the status is too old, then it is probably from an earlier RTFI, and this RTFI never started
                if ParseTimestamp(stat["time"]) < start_time - 30:
                    log.debug("Status timestamp is too old start_time={} status_time={}".format(start_time, ParseTimestamp(stat["time"])))
                    raise SolidFireError("Node did not start RTFI (stale status)")

            # Show the new status to the user
            for stat in new_status:
                log.info("  {}: {}".format(stat["state"], stat["message"]))

            if "Abort" in status[-1]["state"]:
                log.error("RTFI error:\n{}".format(json.dumps(status[-1], indent=2, sort_keys=True)))

            # Log some info about how long state transitions take
            if previous_status:
                log.debug("PreviousState={} duration={} timeout={}".format(previous_status[-1]["state"], time.time() - state_start_time, state_timeout))
            else:
                log.debug("FirstState duration={}".format(time.time() - start_time))

            previous_status = status
            state_start_time = time.time()
            state_timeout = KNOWN_STATE_TIMEOUTS.get(previous_status[-1]["state"], None) or KNOWN_STATE_TIMEOUTS["DEFAULT"]
            log.debug("CurrentState={} timeout={}".format(previous_status[-1]["state"], state_timeout))
            log.debug("Waiting for a new status")

        # If the status indicates we are finished with iRTFI, break
        if status and status[-1]["state"] == "FinishSuccess":
            break

        # If the status indicates a failure, raise an error
        if status and status[-1]["state"] == "FinishFailure":
            raise SolidFireError("RTFI failed")

        # If the node has powered off, assume that RTFI completed
        # Checking continuously exhausts the resources in the BMC leading to errors like
        #       Error in open session response message : insufficient resources for session
        #       Error: Unable to establish IPMI v2 / RMCP+ session
        if time.time() - last_power_check > 20 * sfdefaults.TIME_SECOND:
            last_power_check = time.time()
            if node.GetPowerState() == "off":
                log.warning("Node powered off, assuming it finished RTFI")
                break

        # Timeout if the current state has lasted too long
        if state_start_time > 0 and time.time() - state_start_time > state_timeout:
            raise TimeoutError("Timeout in {} RTFI state".format(previous_status[-1]["state"]))

        # Timeout if the total time has been too long
        if time.time() - start_time > timeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to complete")


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_node_list_args()
    parser.add_argument("--repo", type=str, required=True, default="fluorine", help="the repo/branch to RTFI to, e.g. fluorine or fluorine-branchname")
    parser.add_argument("--version", type=str, required=True, default="latest", help="the version number to RTFI to, e.g. 9.0.0.999")
    parser.add_argument("--image-type", required=True, choices=["rtfi", "fdva"],  default="rtfi", help="the type of RTFI image")
    parser.add_argument("--rtfi-type", required=True, choices=["pxe", "irtfi"],  default="irtfi", help="the type of RTFI to attempt")
    parser.add_argument("--nonet", action="store_false", dest="preserve_networking", default=True, help="do not preserve the current hostname/network config of the node")
    parser.add_argument("--fail", type=str, required=False, help="inject an error during this RTFI state")
    net_override_group = parser.add_argument_group("Network Overrides", description="These settings can be used to override the network settings from AT2")
    net_override_group.add_argument("--netmask", type=IPv4AddressType, required=False, help="use this netmask for all nodes during RTFI")
    net_override_group.add_argument("--gateway", type=IPv4AddressType, required=False, help="use this gateway for all nodes during RTFI")
    net_override_group.add_argument("-I", "--ipmi-ips", type=IPv4AddressType, metavar="IP1,IP2...", help="the IPMI IP addresses for the nodes")
    net_override_group.add_argument("--ipmi-user", type=str, default=sfdefaults.ipmi_user, metavar="USERNAME", help="the IPMI username for all nodes")
    net_override_group.add_argument("--ipmi-pass", type=str, default=sfdefaults.ipmi_pass, metavar="PASSWORD", help="the IPMI password for all nodes")
    args = parser.parse_args_to_dict()

    app = PythonApp(RtfiNodes, args)
    app.Run(**args)
