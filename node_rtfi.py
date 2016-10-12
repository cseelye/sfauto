#!/usr/bin/env python2.7

"""
This action will RTFI a list of nodes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SelectionType, BoolType, StrType
from libsf.util import GetFilename, SolidFireVersion, ParseTimestamp, PrettyJSON, EnsureKeys
from libsf.netutil import CalculateNetwork, IPInNetwork
from libsf import sfdefaults, threadutil, pxeutil, labutil
from libsf import SSHConnection, SolidFireError, ConnectionError, InvalidArgumentError, TimeoutError
import random
import re
import time

KNOWN_STATE_TIMEOUTS = {
    "DEFAULT" : 180,
    "PreparePivotRootKexecLoad" : 60,
    "Start" : 30,
    "DriveUnlock" : 300,
    "Backup" : 1200,
    "BackupKexecLoad" : 60,
    "UpgradeFirmware" : 900,
    "Firmware": 900,
    "DriveErase" : 600,
    "Partition" : 30,
    "Image" : 600,
    "Configure" : 90,
    "Restore" : 60,
    "InternalConfiguration" : 20,
    "PostInstallPre" : 300,
    "PostInstall" : 240,
    "PostInstallKexecLoad" : 60,
    "PostInstallKexec" : 180,
    "Stop" : 180,
    "Coldboot" : 660,
}

RETRYABLE_RTFI_STATUS_ERRORS = [404, 403, 51, 54, 60, 61, 110]

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
    "pxe_server" : (OptionalValueType(IPv4AddressType), sfdefaults.pxe_server),
    "pxe_user" : (OptionalValueType(str), sfdefaults.pxe_username),
    "pxe_pass" : (OptionalValueType(str), sfdefaults.pxe_password),
    "mac_addresses" : (OptionalValueType(ItemList(str)), None),
    "node_names" : (OptionalValueType(ItemList(str)), None),
    "vm_names" : (OptionalValueType(ItemList(str)), None),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(str), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(str), sfdefaults.vmware_mgmt_pass),
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
              gateway,
              pxe_server,
              pxe_user,
              pxe_pass,
              mac_addresses,
              node_names,
              vm_names,
              vm_mgmt_server,
              vm_mgmt_user,
              vm_mgmt_pass):
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
        netmask:                the netmask for the nodes (only for PXE, override auto discovery)
        gateway:                the gateway for the nodes (only for PXE, override auto discovery)
        pxe_server:             the PXE server for the nodes (only for PXE, override auto discovery)
        pxe_user:               the PXE server username for the nodes (only for PXE, override auto discovery)
        pxe_pass:               the PXE server password for the nodes (only for PXE, override auto discovery)
        mac_addresses:          the MAC addresses to use for each node (only for PXE, override auto discovery)
        node_names:             the hostnames to use for each node (override auto discovery)
        vm_names:               the VM names if these are virt nodes, to find and control the VMs during RTFI
        vm_mgmt_server:         the management server for the VMs (vSphere for VMware, hypervisor for KVM)
        vm_mgmt_user:           the management user for the VMs
        vm_mgmt_pass:           the management password for the VMs
    """
    log = GetLogger()

    if ipmi_ips and len(node_ips) != len(ipmi_ips):
        raise InvalidArgumentError("If ipmi_ips is specified, it must have the same number of elements as node_ips")
    if mac_addresses and len(node_ips) != len(mac_addresses):
        raise InvalidArgumentError("If mac_addresses is specified, it must have the same number of elements as node_ips")
    if vm_names and len(node_ips) != len(vm_names):
        raise InvalidArgumentError("If vm_names is specified, it must have the same number of elements as node_ips")

    #
    # Find the build to RTFI to
    #
    if version == "latest":
        log.info("Looking for the latest {} version".format(repo))
        with SSHConnection("192.168.137.1", "root", "solidfire") as ssh:
            try:
                retcode, stdout, stderr = ssh.RunCommand(r"find /builds/iso/{} -type f -name 'solidfire-{}-{}*.iso' -exec basename {{}} \;".format(image_type, image_type, repo), exceptOnError=False)
            except SolidFireError as ex:
                log.error(str(ex))
                return False
        if retcode != 0:
            log.error("Cannot list builds on jenkins: {}".format(stderr))
            return False

        version = "0.0.0.0"
        for line in stdout.split("\n"):
            m = re.search(r"solidfire-{}-{}-([0-9]\.[0-9]\.[0-9]\.[0-9]+).iso".format(image_type, repo), line)
            if not m:
                continue
            found_version = m.group(1)
            if int(found_version.split(".")[3]) > int(version.split(".")[3]):
                version = found_version
        if version == "0.0.0.0":
            log.error("Could not find any {} builds".format(repo))
            return False
        log.info("Found {}-{}".format(repo, version))
    else:
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

    #
    # Find all of the network and infrastructure info for the nodes
    #
    log.info("Determining network and infrastructure info for the nodes")
    at2_net_info = None
    net_info = {}

    # iRTFI of virtual nodes
    if rtfi_type == "irtfi" and vm_names:
        required_keys = ["ip", "hostname", "vm_name", "vm_mgmt_server", "vm_mgmt_user", "vm_mgmt_pass"]
    # iRTFI of physical nodes
    elif rtfi_type == "irtfi":
        required_keys = ["ip", "hostname"]
    # PXE RTFI of virtual nodes
    elif rtfi_type == "pxe" and vm_names:
        required_keys = ["ip", "hostname", "pxe", "pxe_user", "pxe_pass", "netmask", "gateway", "vm_name", "vm_mgmt_server", "vm_mgmt_user", "vm_mgmt_pass"]
    # PXE RTFI of physical nodes
    elif rtfi_type == "pxe":
        required_keys = ["ip", "hostname", "pxe", "pxe_user", "pxe_pass", "netmask", "gateway", "ipmi", "ipmi_user", "ipmi_pass"]

    for idx, node_ip in enumerate(node_ips):
        # Set defaults and any user supplied info
        net_info[node_ip] = {}
        EnsureKeys(net_info[node_ip], required_keys, None)
        net_info[node_ip]["ip"] = node_ip
        net_info[node_ip]["ipmi_user"] = ipmi_user
        net_info[node_ip]["ipmi_pass"] = ipmi_pass
        net_info[node_ip]["pxe_user"] = pxe_user
        net_info[node_ip]["pxe_pass"] = pxe_pass
        net_info[node_ip]["vm_mgmt_server"] = vm_mgmt_server
        net_info[node_ip]["vm_mgmt_user"] = vm_mgmt_user
        net_info[node_ip]["vm_mgmt_pass"] = vm_mgmt_pass
        net_info[node_ip]["netmask"] = netmask
        net_info[node_ip]["gateway"] = gateway
        net_info[node_ip]["pxe"] = pxe_server

        if ipmi_ips:
            net_info[node_ip]["ipmi"] = ipmi_ips[idx]
        if mac_addresses:
            net_info[node_ip]["mac"] = mac_addresses[idx]
        if node_names:
            net_info[node_ip]["hostname"] = node_names[idx]
        if vm_names:
            net_info[node_ip]["vm_name"] = vm_names[idx]
            net_info[node_ip]["hostname"] = vm_names[idx]

        # Look in AT2 for any missing information
        if not all([net_info[node_ip][key] for key in required_keys]):
            # Look up the info in AT2
            if not at2_net_info:
                try:
                    at2_net_info = labutil.GetNetworkProfile(node_ips)
                except SolidFireError:
                    pass
            if not at2_net_info or node_ip not in at2_net_info:
                continue
            # Fill in empty keys with AT2 info
            for key in required_keys:
                net_info[node_ip][key] = net_info[node_ip][key] or at2_net_info[node_ip].get(key, None)

        # Make sure we have all of the info we need
        missing = set(required_keys) - set(net_info[node_ip].keys())
        if missing:
            raise InvalidArgumentError("Could not find required info for {}: {}".format(node_ip, ", ".join(missing)))
        node_network = CalculateNetwork(net_info[node_ip]["ip"], net_info[node_ip]["netmask"])
        if not IPInNetwork(net_info[node_ip]["gateway"], node_network):
            raise InvalidArgumentError("The gateway ({}) must be on the same network as the node IP ({})".format(net_info[node_ip]["gateway"], net_info[node_ip]["ip"]))

    #
    # Start a thread to RTFI and monitor each node
    #
    log.info("RTFI {} nodes to {}-{}".format(len(node_ips), repo, version))
    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_NodeThread, rtfi_type,
                                              image_type,
                                              repo,
                                              version,
                                              preserve_networking,
                                              fail,
                                              net_info[node_ip],
                                              username,
                                              password))

    #
    # Wait for all threads to complete
    #
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
def _NodeThread(rtfi_type, image_type, repo, version, preserve_networking, fail, net_info, username, password):
    """RTFI a node"""
    log = GetLogger()
    SetThreadLogPrefix(net_info["ip"])

    # Generate a random identifier
    agent = "rtfiscript{}".format(random.randint(1000, 9999))

    node = SFNode(net_info["ip"],
                  clusterUsername=username,
                  clusterPassword=password,
                  ipmiIP=net_info.get("ipmi", None),
                  ipmiUsername=net_info.get("ipmi_user", None),
                  ipmiPassword=net_info.get("ipmi_pass", None),
                  vmName=net_info.get("vm_name", None),
                  vmManagementServer=net_info.get("vm_mgmt_server", None),
                  vmManagementUsername=net_info.get("vm_mgmt_user", None),
                  vmManagementPassword=net_info.get("vm_mgmt_pass", None))
    rtfi_opts = [
        "sf_hostname={}-rtfi".format(net_info["hostname"]),
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
        # Get the MAC address from the BMC or hypervisor
        if not net_info.get("mac", None):
            log.info("Getting MAC address from {}".format("hypervisor" if net_info.get("vm_name", None) else "BMC"))
            net_info["mac"] = node.GetPXEMacAddress()

        log.info("Creating PXE config file")
        pxeutil.DeletePXEFile(net_info["mac"],
                              pxeServer=net_info["pxe"],
                              pxeUser=net_info["pxe_user"],
                              pxePassword=net_info["pxe_pass"])
        pxeutil.CreatePXEFile(net_info["mac"],
                              repo,
                              version,
                              rtfi_options,
                              imageType=image_type,
                              pxeServer=net_info["pxe"],
                              pxeUser=net_info["pxe_user"],
                              pxePassword=net_info["pxe_pass"],
                              ip=net_info["ip"],
                              netmask=net_info["netmask"],
                              gateway=net_info["gateway"])

        # Make sure the boot order is correct
        node.SetPXEBoot()

        log.info("Power cycling node")
        node.PowerOff()
        node.PowerOn(waitForUp=False)

        # Wait for RTFI to complete
        try:
            if SolidFireVersion(version) < SolidFireVersion("8.0.0.0"):
                log.warning("This RTFI version does not support monitoring other than power status")
            elif SolidFireVersion(version) < SolidFireVersion("9.0.0.0"):
                log.warning("This RTFI version supports incomplete status monitoring")
            elif SolidFireVersion(version) < SolidFireVersion("10.0.0.0"):
                _monitor_v90(rtfi_type, node, net_info, startTimeout=360, preserveNetworking=preserve_networking)
            else:
                log.warning("RTFI status monitoring not implemented for this Element version")
                return False
        except TimeoutError as ex:
            log.error(str(ex))

            # Try to save the RTFI log
            logfile = GetFilename("{}-rtfi.log".format(net_info["ip"]))
            try:
                node.SaveRTFILog(logfile)
                log.info("Saved RTFI log to {}".format(logfile))
            except SolidFireError:
                pass

            log.error("Failed to RTFI")
            return False
        finally:
            log.info("Removing PXE config file")
            pxeutil.DeletePXEFile(net_info["mac"],
                                  pxeServer=net_info["pxe"],
                                  pxeUser=net_info["pxe_user"],
                                  pxePassword=net_info["pxe_pass"])

    # in place RTFI
    elif rtfi_type == "irtfi":
        log.info("Calling StartRtfi")
        node.StartRTFI(repo, version, rtfi_options)

        # Wait for RTFI to complete
        if SolidFireVersion(version) >= SolidFireVersion("9.0.0.0"):
            _monitor_v90(rtfi_type, node, net_info, agentID=agent)
        else:
            log.warning("RTFI status monitoring not implemented for this Element version")
            return False

    # Wait for the node to be fully up
    if preserve_networking:
        node.WaitForUp(initialWait=0, timeout=300)
        log.info("Node is back up")

    return True

def _monitor_v90(rtfiType, node, netInfo, startTimeout=360, timeout=3600, agentID=None, preserveNetworking=True):
    """
    Monitor Fluorine RTFI status and return when RTFI is complete
    """
    log = GetLogger()
    log.info("Waiting for RTFI status")
    removed_pxe = False
    status = None
    previous_status = []
    last_api_check = time.time()
    state_timeout = KNOWN_STATE_TIMEOUTS["DEFAULT"]
    state_start_time = 0
    start_time = time.time()
    log.debug("Waiting for status server timeout={}".format(startTimeout))
    while True:
        try:
            status = node.GetAllRTFIStatus()
        except ConnectionError as ex:
            if not ex.IsRetryable() and ex.code not in RETRYABLE_RTFI_STATUS_ERRORS:
                log.warning(str(ex))
            status = None

        # Timeout if we haven't gotten any status within the startTimeout period
        if not previous_status and time.time() - start_time > startTimeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to start")

        # If we never got any status, never caught the node powering off, and the node is back up, it probably never PXE booted
        if rtfiType == "pxe" and not previous_status and time.time() - last_api_check > 20 * sfdefaults.TIME_SECOND:
            last_api_check = time.time()
            if node.IsUp():
                log.debug("API is back up but never got any RTFI status")
                raise SolidFireError("Node did not RTFI, check PXE boot settings")

        # See if the current state has changed
        if status and status != previous_status:
            new_status = [stat for stat in status if stat not in previous_status]

            # Sanity check the status and display it to the user
            for stat in new_status:
                _check_and_log_new_status(stat, start_time, agentID)

            if "Abort" in status[-1]["state"]:
                log.error("RTFI error:\n{}".format(PrettyJSON(status[-1])))

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

        # If we got a good status, then we did successfully PXE boot and don't need the PXE config file anymore
        if rtfiType == "pxe" and previous_status and not removed_pxe:
            pxeutil.DeletePXEFile(node.GetPXEMacAddress(),
                                  pxeServer=netInfo["pxe"],
                                  pxeUser=netInfo["pxe_user"],
                                  pxePassword=netInfo["pxe_pass"])
            removed_pxe = True

        # If the status is Coldboot, wait for the node to power off and come back
        if status and status[-1]["state"] == "Coldboot":
            _handle_coldboot(node, preserveNetworking)
            if not preserveNetworking:
                break

        # If the status indicates we are finished with RTFI, break
        if status and status[-1]["state"] == "FinishSuccess":
            break

        # If the status indicates a failure, raise an error
        if status and status[-1]["state"] == "FinishFailure":
            raise SolidFireError("RTFI failed")

        # Timeout if the current state has lasted too long
        if state_start_time > 0 and time.time() - state_start_time > state_timeout:
            raise TimeoutError("Timeout in {} RTFI state".format(previous_status[-1]["state"]))

        # Timeout if the total time has been too long
        if time.time() - start_time > timeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to complete")


def _handle_coldboot(node, preserveNetworking):
    log = GetLogger()

    # Wait for the node to power down
    log.info("Waiting for node to power off")
    node.WaitForOff()

    # If this is a virt node, we must power it on because ACPI wakeup does not work
    if node.IsVirtual():
        log.info("Powering on node")
        node.PowerOn(waitForUp=False)

    # If this is not a virt node, wait for the node to auto wakeup
    else:
        log.info("Waiting for node to power on")
        time.sleep(50)
        node.WaitForOn(timeout=330)

    # Wait for the node to be up on the network again
    if preserveNetworking:
        node.WaitForPing()

def _check_and_log_new_status(status, startTime, agentID):
    log = GetLogger()
    log.debug("found new status:\n{}".format(PrettyJSON(status)))

    # If the status does not match our agent ID, this is probably a stale status from an earlier RTFI
    #   Older traditional RTFI does not honor sf_agent so don't check it (SF_AGENT=Manual)
    #   PreparePivotRoot state does not show the correct agent so don't check it
    if agentID and "sf_agent" in status and status["sf_agent"] != "Manual" and status["state"] != "PreparePivotRoot":
        if status["sf_agent"] != agentID:
            log.debug("sf_agent does not match agentID={}".format(agentID))
            raise SolidFireError("Node did not start RTFI (stale status)")

    # If the timestamp from the status is too old, then it is probably from an earlier RTFI, and this RTFI never started
    if ParseTimestamp(status["time"]) < startTime - 30:
        log.debug("Status timestamp is too old startTime={} statusTime={}".format(startTime, ParseTimestamp(status["time"])))
        raise SolidFireError("Node did not start RTFI (stale status)")

    if "percent" in status:
        state = "{}% - {}".format(status["percent"], status["state"])
    else:
        state = "{}".format(status["state"])
    log.info("  {}: {}".format(state, status["message"]))


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
    net_override_group.add_argument("--pxe-server", type=IPv4AddressType, default=sfdefaults.pxe_server, required=False, help="use this PXE server for all nodes during RTFI")
    net_override_group.add_argument("--pxe-user", type=str, default=sfdefaults.pxe_username, metavar="USERNAME", help="the PXE server username for all nodes")
    net_override_group.add_argument("--pxe-pass", type=str, default=sfdefaults.pxe_password, metavar="PASSWORD", help="the PXE server password for all nodes")
    net_override_group.add_argument("--netmask", type=IPv4AddressType, required=False, help="use this netmask for all nodes during RTFI")
    net_override_group.add_argument("--gateway", type=IPv4AddressType, required=False, help="use this gateway for all nodes during RTFI")
    net_override_group.add_argument("-I", "--ipmi-ips", type=ItemList(IPv4AddressType), metavar="IP1,IP2...", help="the IPMI IP addresses for the nodes")
    net_override_group.add_argument("--ipmi-user", type=str, default=sfdefaults.ipmi_user, metavar="USERNAME", help="the IPMI username for all nodes")
    net_override_group.add_argument("--ipmi-pass", type=str, default=sfdefaults.ipmi_pass, metavar="PASSWORD", help="the IPMI password for all nodes")
    net_override_group.add_argument("--mac-addresses", type=ItemList(str), metavar="MAC1,MAC2...", help="the MAC addresses for the nodes")
    net_override_group.add_argument("--node-names", type=ItemList(str), metavar="HOSTNAME1,HOSTNAME2...", help="the hostnames for the nodes")

    vm_group = parser.add_argument_group("Virtual Node Options", description="These settings are required for virt nodes, along with the network override settings.")
    vm_group.add_argument("--vm-names", type=str, metavar="NAME", help="the name of the VMs to RTFI")
    vm_group.add_argument("-s", "--vm-mgmt-server", type=IPv4AddressType, default=sfdefaults.vmware_mgmt_server, metavar="IP", help="the IP address of the hypervisor/management server")
    vm_group.add_argument("-e", "--vm-mgmt-user", type=str, default=sfdefaults.vmware_mgmt_user, help="the hypervisor admin username")
    vm_group.add_argument("-a", "--vm-mgmt-pass", type=str, default=sfdefaults.vmware_mgmt_pass, help="the hypervisor admin password")

    args = parser.parse_args_to_dict()

    app = PythonApp(RtfiNodes, args)
    app.Run(**args)
