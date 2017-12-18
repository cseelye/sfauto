#!/usr/bin/env python2.7

"""
This action will RTFI a list of nodes
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter, SUPPRESS
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.sfnode import SFNode
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, ItemList, SelectionType, StrType, BoolType
from libsf.util import GetFilename, SolidFireVersion, ParseTimestamp, PrettyJSON, EnsureKeys
from libsf.netutil import CalculateNetwork, IPInNetwork
from libsf import sfdefaults, threadutil, pxeutil, labutil, netutil
from libsf import SolidFireError, ConnectionError, InvalidArgumentError, TimeoutError, HTTPDownloader
import json
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
    "Partition" : 180,
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
    "node_ips" : (ItemList(IPv4AddressType), sfdefaults.node_ips),
    "repo" : (StrType, None),
    "version" : (StrType, None),
    "rtfi_type" : (SelectionType(["pxe", "irtfi"]), "irtfi"),
    "configure_network" : (SelectionType(sfdefaults.all_network_config_options), "keep"),
    "image_type" : (SelectionType(["rtfi", "fdva"]), "rtfi"),
    "extra_args" : (OptionalValueType(ItemList(StrType)), None),
    "fail" : (OptionalValueType(StrType), None),
    "test" : (BoolType, False),
    "ipmi_ips" : (OptionalValueType(ItemList(IPv4AddressType)), sfdefaults.ipmi_ips),
    "username" : (StrType, sfdefaults.username),
    "password" : (StrType, sfdefaults.password),
    "ipmi_user" : (StrType, sfdefaults.ipmi_user),
    "ipmi_pass" : (StrType, sfdefaults.ipmi_pass),
    "mip_netmask" : (OptionalValueType(IPv4AddressType), sfdefaults.mip_netmask),
    "mip_gateway" : (OptionalValueType(IPv4AddressType), sfdefaults.mip_gateway),
    "nameserver" : (OptionalValueType(IPv4AddressType), sfdefaults.nameserver),
    "domain" : (OptionalValueType(StrType), sfdefaults.domain),
    "cip_ips" : (OptionalValueType(ItemList(IPv4AddressType)), sfdefaults.cip_ips),
    "cip_netmask" : (OptionalValueType(IPv4AddressType), sfdefaults.cip_netmask),
    "cip_gateway" : (OptionalValueType(IPv4AddressType), sfdefaults.cip_gateway),
    "pxe_server" : (OptionalValueType(IPv4AddressType), sfdefaults.pxe_server),
    "pxe_user" : (OptionalValueType(StrType), sfdefaults.pxe_user),
    "pxe_pass" : (OptionalValueType(StrType), sfdefaults.pxe_pass),
    "jenkins_server" : (OptionalValueType(IPv4AddressType), sfdefaults.jenkins_server),
    "mac_addresses" : (OptionalValueType(ItemList(StrType)), sfdefaults.mac_addresses),
    "node_names" : (OptionalValueType(ItemList(StrType)), sfdefaults.node_names),
    "vm_names" : (OptionalValueType(ItemList(StrType)), sfdefaults.vm_names),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vm_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vm_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vm_mgmt_pass),
})
def RtfiNodes(node_ips,
              repo,
              version,
              rtfi_type,
              configure_network,
              image_type,
              extra_args,
              fail,
              test,
              ipmi_ips,
              username,
              password,
              ipmi_user,
              ipmi_pass,
              mip_netmask,
              mip_gateway,
              nameserver,
              domain,
              cip_ips,
              cip_netmask,
              cip_gateway,
              pxe_server,
              pxe_user,
              pxe_pass,
              jenkins_server,
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
        configure_network:      how to configure the hostname/network of the nodes (keep, clear, reconfigure)
        rtfi_type:              type of RTFI (pxe, irtfi)
        image_type:             type of image (rtfi, fdva)
        extra_args:             additional options to pass to RTFI1
        fail:                   inject a failure during this RTFI state
        test:                   test the config options and nodes but do not RTFI
        ipmi_ips:               the IPMI IPs of the nodes
        ipmi_user:              the IPMI username
        ipmi_pass:              the IPMI password
        netmask:                the netmask for the nodes (only for PXE, override auto discovery)
        gateway:                the gateway for the nodes (only for PXE, override auto discovery)
        nameserver:             the DNS server to discover the nodes after RTFI (only for PXE when clearing the network config, override auto discovery)
        domain:                 the DNS search domain to discover the nodes after RTFI (only for PXE when clearing the network config, override auto discovery)
        cip_ips:                the 10G IP addresses for the nodes (overrides auto discovery)
        cip_netmask:            the 10G netmask for the nodes (overrides auto discovery)
        cip_gateway:            the 10G gateway for the nodes (overrides auto discovery)
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
    if node_names and len(node_ips) != len(node_names):
        raise InvalidArgumentError("If node_names is specified, it must have the same number of elements as node_ips")
    if cip_ips and len(node_ips) != len(cip_ips):
        raise InvalidArgumentError("If cip_ips is specified, it must have the same number of elements as node_ips")

    #
    # Find all of the network and infrastructure info for the nodes
    #
    log.info("Determining network and infrastructure info for {} {}".format(len(node_ips), "node" if len(node_ips) == 1 else "nodes"))
    at2_net_info = None
    net_info = {}

    # iRTFI of virtual nodes
    if rtfi_type == "irtfi" and vm_names:
        required_keys = ["ip", "hostname", "image_list", "pxe", "netmask", "gateway", "vm_name", "vm_mgmt_server", "vm_mgmt_user", "vm_mgmt_pass"]
    # iRTFI of physical nodes
    elif rtfi_type == "irtfi":
        required_keys = ["ip", "hostname", "image_list", "pxe", "netmask", "gateway"]
    # PXE RTFI of virtual nodes
    elif rtfi_type == "pxe" and vm_names:
        required_keys = ["ip", "hostname", "image_list", "pxe", "pxe_user", "pxe_pass", "netmask", "gateway", "vm_name", "vm_mgmt_server", "vm_mgmt_user", "vm_mgmt_pass"]
    # PXE RTFI of physical nodes
    elif rtfi_type == "pxe":
        required_keys = ["ip", "hostname", "image_list", "pxe", "pxe_user", "pxe_pass", "netmask", "gateway", "ipmi", "ipmi_user", "ipmi_pass"]

    if configure_network == "clear" or configure_network == "reconfigure":
        required_keys += ["nameserver", "domain"]

    if configure_network == "reconfigure":
        required_keys += ["cip", "cip_netmask"]

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
        net_info[node_ip]["netmask"] = mip_netmask
        net_info[node_ip]["gateway"] = mip_gateway
        net_info[node_ip]["pxe"] = pxe_server
        net_info[node_ip]["domain"] = domain
        net_info[node_ip]["nameserver"] = nameserver

        if ipmi_ips:
            net_info[node_ip]["ipmi"] = ipmi_ips[idx]
        if mac_addresses:
            net_info[node_ip]["mac"] = mac_addresses[idx]
        if vm_names:
            net_info[node_ip]["vm_name"] = vm_names[idx]
            net_info[node_ip]["hostname"] = vm_names[idx]
        if node_names:
            net_info[node_ip]["hostname"] = node_names[idx]
        if cip_ips:
            net_info[node_ip]["cip"] = cip_ips[idx]
            net_info[node_ip]["cip_netmask"] = cip_netmask
            net_info[node_ip]["cip_gateway"] = cip_gateway
        if jenkins_server:
            net_info[node_ip]["image_list"] = "http://{}/rtfi".format(jenkins_server)


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
    for node_ip in node_ips:
        missing = set(required_keys) - set([key for key in net_info[node_ip].keys() if net_info[node_ip][key]])
        if missing:
            raise InvalidArgumentError("Could not find required info for {}: {}".format(node_ip, ", ".join(missing)))
        node_network = CalculateNetwork(net_info[node_ip]["ip"], net_info[node_ip]["netmask"])
        if not IPInNetwork(net_info[node_ip]["gateway"], node_network):
            raise InvalidArgumentError("The gateway ({}) must be on the same network as the node IP ({})".format(net_info[node_ip]["gateway"], net_info[node_ip]["ip"]))
    for node_ip in node_ips:
        log.debug2("net_info[{}] = {}".format(node_ip, json.dumps(net_info[node_ip], sort_keys=True, indent=2)))

    #
    # Find the build to RTFI to
    #
    log.info("Searching for available {} builds".format(repo))
    image_list_url = None
    for node_info in net_info.values():
        image_list_url = image_list_url or node_info["image_list"]
        if node_info["image_list"] != image_list_url:
            log.error("Nodes must all be at the same site")
            return False

    # Make a list of available builds for the given repo
    image_html = HTTPDownloader.DownloadURL(image_list_url)
    iso_regex = re.compile(r'href="(solidfire-{}-{}-[1-9].+\.iso)"'.format(image_type, repo))
    version_regex = re.compile(r'([0-9]+\.[0-9]\.[0-9]\.[0-9]+)')
    found_isos = iso_regex.findall(image_html)
    log.debug2("found_isos = [{}]".format(", ".join([str(iso) for iso in found_isos])))
    available_builds = []
    for iso_name in found_isos:
        match = version_regex.search(iso_name)
        if match:
            available_builds.append(SolidFireVersion(match.group(1)))
    log.debug2("available_builds = [{}]".format(", ".join([str(build) for build in available_builds])))
    if not available_builds:
        log.error("Could not find any builds for {}".format(repo))
        return False

    if version == "latest":
        # Get the latest version available
        version = str(max(available_builds))
    else:
        # Make sure the specified build exists
        if SolidFireVersion(version) not in available_builds:
            log.error("Could not find {} {}".format(repo, version))
            return False
    log.info("Found {} {}".format(repo, version))

    # Sanity check that the requested network config and RTFI type is possible for this element version
    if SolidFireVersion(version) < SolidFireVersion("9.0.0.0") and rtfi_type == "pxe" and configure_network == "keep":
        log.error("PXE RTFI cannot preserve the network config with this Element version. Use either iRTFI or reconfigure options")
        return False
    if configure_network == "keep":
        for node_ip in node_ips:
            node = SFNode(node_ip, clusterUsername=username, clusterPassword=password, vmManagementServer=vm_mgmt_server, vmManagementUsername=vm_mgmt_user, vmManagementPassword=vm_mgmt_pass)

            if node.IsUp() and node.IsDHCPEnabled():
                log.error("Cannot preserve network config with a node in DHCP ({})".format(node_ip))
                return False

    #
    # Start a thread to RTFI and monitor each node
    #
    log.info("RTFI {} node{} to {}-{}".format(len(node_ips), "s" if len(node_ips) >1 else "", repo, version))
    pool = threadutil.GlobalPool()
    results = []
    for node_ip in node_ips:
        results.append(pool.Post(_NodeThread, rtfi_type,
                                              image_type,
                                              repo,
                                              version,
                                              configure_network,
                                              fail,
                                              test,
                                              net_info[node_ip],
                                              username,
                                              password,
                                              extra_args))

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
def _NodeThread(rtfi_type, image_type, repo, version, configure_network, fail, test, net_info, username, password, extra_args=None):
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
        "sf_hostname={}{}".format(net_info["hostname"], sfdefaults.rtfi_hostname_suffix),
        "sf_agent={}".format(agent),
        "sf_allow_virtnode=1"
    ]
    if configure_network == "keep":
        rtfi_opts.extend(["sf_keep_hostname=1",
                          "sf_keep_network_config=1",
                          ])

    if fail:
        rtfi_opts.append("sf_status_inject_failure={}".format(fail))

    if extra_args:
        rtfi_opts.extend(extra_args)

    rtfi_options = ",".join(rtfi_opts)
    log.debug("RTFI options: {}".format(rtfi_options))

    # PXE boot traditional RTFI
    if rtfi_type == "pxe":
        # Get the MAC address from the BMC or hypervisor
        if not net_info.get("mac", None):
            log.info("Getting MAC address from {}".format("hypervisor" if net_info.get("vm_name", None) else "BMC"))
            net_info["mac"] = node.GetPXEMacAddress()

        if test:
            log.info("Test mode set; skipping RTFI node")
            return True

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
                              gateway=net_info["gateway"],
                              includeSerialConsole=False if net_info.get("vm_name") else True)

        # Make sure the boot order is correct
        node.SetPXEBoot()

        log.info("Power cycling node")
        node.PowerOff()
        node.PowerOn(waitForUp=False)

        # Wait for RTFI to complete
        try:
            if SolidFireVersion(version) < SolidFireVersion("8.0.0.0"):
                log.warning("This Element version does not support RTFI monitoring other than power status")
                _monitor_legacy(node)
            elif SolidFireVersion(version) < SolidFireVersion("9.0.0.0"):
                _monitor_v80(rtfi_type, node, net_info, startTimeout=sfdefaults.node_boot_timeout)
            elif SolidFireVersion(version) >= SolidFireVersion("9.0.0.0"):
                _monitor_v90(rtfi_type, node, net_info, startTimeout=sfdefaults.node_boot_timeout, configureNetworking=configure_network)
            else:
                log.warning("RTFI status monitoring not implemented for this Element version")
                return False
        except TimeoutError as ex:
            log.error(str(ex))

            # Try to save the RTFI log
            logfile = GetFilename("{}-rtfi.log".format(net_info["ip"]))
            try:
                node.SaveRTFILog(logfile)
                log.info("Saved RTFI log locally to {}".format(logfile))
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
        if not node.IsUp():
            raise SolidFireError("Node must be up and running to use iRTFI, try PXE instead")

        if test:
            log.info("Test mode set; skipping RTFI node")
            return True

        log.info("Calling StartRtfi")
        node.StartRTFI(repo, version, rtfi_options, buildServer=net_info["pxe"])

        # Wait for RTFI to complete
        if SolidFireVersion(version) >= SolidFireVersion("9.0.0.0"):
            _monitor_v90(rtfi_type, node, net_info, agentID=agent)
        else:
            log.warning("RTFI status monitoring not implemented for this Element version")
            return False

    # RTFI is done, now wait for the node to come back up
    if configure_network == "keep":
        node.WaitForUp(initialWait=0, timeout=sfdefaults.node_boot_timeout)
        log.info("Node is back up")
    else:
        # Try to resolve the node with DDNS
        log.info("Waiting for node DDNS registration")
        start_time = time.time()
        while True:
            if time.time() - start_time > sfdefaults.node_boot_timeout:
                raise TimeoutError("Timeout waiting for node DDNS. The node may have failed to boot up")

            # Original sites like BDR and ZDC resolve directly to the domain, like eng.solidfire.net
            # New PearlWest resolves to one.domain or ten.domain for 1G and 10G DHCP addresses
            # So we try both versions just in case
            for node_ddns_hostname in ["{}{}.{}".format(net_info["hostname"], sfdefaults.rtfi_hostname_suffix, net_info["domain"]),
                                       "{}{}.one.{}".format(net_info["hostname"], sfdefaults.rtfi_hostname_suffix, net_info["domain"])]:
                temp_node_ip = None
                try:
                    ans = netutil.ResolveHostname(node_ddns_hostname, net_info["nameserver"])
                    temp_node_ip = ans[0]
                    node._SetInternalManagementIP(temp_node_ip)
                    break
                except SolidFireError:
                    continue
            if temp_node_ip:
                break
            time.sleep(2)
        node.WaitForUp()
        log.info("Node is up on DHCP IP {}".format(temp_node_ip))

    # Configure the network
    if configure_network == "reconfigure":
        log.info("Setting hostname to {}".format(net_info["hostname"]))
        node.SetHostname(net_info["hostname"])
        log.info("Configuring network")
        node.SetNetworkInfo(onegIP=net_info["ip"],
                            onegNetmask=net_info["netmask"],
                            onegGateway=net_info["gateway"],
                            dnsIP=net_info["nameserver"],
                            dnsSearch=net_info["domain"],
                            tengIP=net_info["cip"],
                            tengNetmask=net_info["cip_netmask"],
                            tengGateway=net_info.get("cip_gateway", None))
        log.info("Node is configured to static IP {}".format(net_info["ip"]))

    log.passed("Successful RTFI")
    return True

def _monitor_legacy(node, timeout=3600):
    """
    Monitor old releases RTFI status and return when RTFI is complete
    """
    log = GetLogger()
    start_time = time.time()
    log.info("Waiting for node to power off")
    while True:
        time.sleep(20)
        if node.GetPowerState() == "off":
            node.PowerOn(waitForUp=False)
            break

        # Timeout if the total time has been too long
        if time.time() - start_time > timeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to complete")

def _monitor_v80(rtfiType, node, netInfo, startTimeout=sfdefaults.node_boot_timeout, timeout=3600, agentID=None):
    """
    Monitor Oxygen RTFI status and return when RTFI is complete
    """
    log = GetLogger()
    log.info("Waiting for RTFI status")
    removed_pxe = False
    status = None
    previous_status = []
    last_api_check = time.time()
    last_power_check = time.time()
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
            log.debug("Checking if node skipped RTFI and came back up")
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

        # If we got at least one good status, then we did successfully PXE boot and don't need the PXE config file anymore
        if rtfiType == "pxe" and previous_status and not removed_pxe:
            pxeutil.DeletePXEFile(node.GetPXEMacAddress(),
                                  pxeServer=netInfo["pxe"],
                                  pxeUser=netInfo["pxe_user"],
                                  pxePassword=netInfo["pxe_pass"])
            removed_pxe = True

        # If the status indicates we are finished with RTFI, break
        if status and status[-1]["state"] == "FinishSuccess":
            break

        # If the status indicates a failure, raise an error
        if status and status[-1]["state"] == "FinishFailure":
            raise SolidFireError("RTFI failed")

        # If we hit the Stop state, the node should now finish RTFI and power itself off
        if status and status[-1]["state"] == "Stop":
            _handle_coldboot(node)
            break

        # If we know RTFI was able to start and the node has now powered down, assume that RTFI finished successfully
        # This is in case we missed the final state, or the final state is unknown to this script
        if previous_status and time.time() - last_power_check > 20:
            last_power_check = time.time()
            log.debug("Checking if node finished RTFI and powered off")
            if node.GetPowerState() == "off":
                _handle_coldboot(node)
                break

        # Timeout if the current state has lasted too long
        if state_start_time > 0 and time.time() - state_start_time > state_timeout:
            raise TimeoutError("Timeout in {} RTFI state".format(previous_status[-1]["state"]))

        # Timeout if the total time has been too long
        if time.time() - start_time > timeout * sfdefaults.TIME_SECOND:
            raise TimeoutError("Timeout waiting for RTFI to complete")

def _monitor_v90(rtfiType, node, netInfo, startTimeout=sfdefaults.node_boot_timeout, timeout=3600, agentID=None, configureNetworking="keep"):
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
            log.debug("Checking if node skipped RTFI and came back up")
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

        # If we got at least one good status, then we did successfully PXE boot and don't need the PXE config file anymore
        if rtfiType == "pxe" and previous_status and not removed_pxe:
            pxeutil.DeletePXEFile(node.GetPXEMacAddress(),
                                  pxeServer=netInfo["pxe"],
                                  pxeUser=netInfo["pxe_user"],
                                  pxePassword=netInfo["pxe_pass"])
            removed_pxe = True

        # If the status is Coldboot, wait for the node to power off and come back
        if status and status[-1]["state"] == "Coldboot":
            _handle_coldboot(node)
            if configureNetworking != "keep":
                break
            log.info("Waiting for node to come up")

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

def _handle_coldboot(node):
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
        try:
            node.WaitForOn(timeout=310)
        except TimeoutError:
            log.warning("Node failed to auto wakeup")
            node.PowerOn(waitForUp=False)

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
    parser.add_argument("--repo", type=StrType, required=True, default="fluorine", help="the repo/branch to RTFI to, e.g. fluorine or fluorine-branchname")
    parser.add_argument("--version", type=StrType, required=True, default="latest", help="the version number to RTFI to, e.g. 9.0.0.999, or 'latest' to pick the latest published version")
    parser.add_argument("--image-type", required=True, choices=["rtfi", "fdva"],  default="rtfi", help="the type of RTFI image")
    parser.add_argument("--rtfi-type", required=True, choices=["pxe", "irtfi"],  default="irtfi", help="the RTFI process to use")
    parser.add_argument("--net-config", dest="configure_network", required=True, choices=sfdefaults.all_network_config_options, default="keep", help="how to configure the network on the node")

    net_override_group = parser.add_argument_group("Network Overrides", description="These settings can be used to override the network settings from AT2, or provide them if the node does not exist in the AT2 resource database")
    net_override_group.add_argument("-I", "--ipmi-ips", type=ItemList(IPv4AddressType), default=sfdefaults.ipmi_ips, metavar="IP1,IP2...", help="the IPMI IP addresses for the nodes")
    net_override_group.add_argument("--ipmi-user", type=StrType, default=sfdefaults.ipmi_user, metavar="USERNAME", help="the IPMI username for all nodes")
    net_override_group.add_argument("--ipmi-pass", type=StrType, default=sfdefaults.ipmi_pass, metavar="PASSWORD", help="the IPMI password for all nodes")
    net_override_group.add_argument("--mac-addresses", type=ItemList(StrType), default=sfdefaults.mac_addresses, metavar="MAC1,MAC2...", help="the MAC addresses for the nodes")
    net_override_group.add_argument("--node-names", type=ItemList(StrType), default=sfdefaults.node_names, metavar="HOSTNAME1,HOSTNAME2...", help="the hostnames for the nodes")
    net_override_group.add_argument("--pxe-server", type=IPv4AddressType, default=sfdefaults.pxe_server, metavar="IP", required=False, help="use this PXE server for all nodes during RTFI")
    net_override_group.add_argument("--pxe-user", type=StrType, default=sfdefaults.pxe_user, metavar="USERNAME", help="the PXE server username for all nodes")
    net_override_group.add_argument("--pxe-pass", type=StrType, default=sfdefaults.pxe_pass, metavar="PASSWORD", help="the PXE server password for all nodes")
    net_override_group.add_argument("--jenkins-server", type=IPv4AddressType, default=sfdefaults.jenkins_server, metavar="IP", required=False, help="use this jenkins server for all nodes to find available builds")
    net_override_group.add_argument("--mip-netmask", type=IPv4AddressType, default=sfdefaults.mip_netmask, required=False, help="use this netmask for all nodes during RTFI")
    net_override_group.add_argument("--mip-gateway", type=IPv4AddressType, default=sfdefaults.mip_gateway, required=False, help="use this gateway for all nodes during RTFI")
    net_override_group.add_argument("--nameserver", type=IPv4AddressType, required=False, help="use this DNS server for all nodes to find them in DNS after RTFI")
    net_override_group.add_argument("--domain", type=StrType, required=False, help="use this DNS search domain for all nodes to find them in DNS after RTFI")
    net_override_group.add_argument("--cip-ips", type=ItemList(IPv4AddressType), default=sfdefaults.cip_ips, metavar="CIP1,CIP2...", help="the 10G IP addresses for the nodes")
    net_override_group.add_argument("--cip-netmask", type=IPv4AddressType, default=sfdefaults.cip_netmask, required=False, help="the 10G netmask for all nodes")
    net_override_group.add_argument("--cip-gateway", type=IPv4AddressType, default=sfdefaults.cip_gateway, required=False, help="the 10G gateway for all nodes")

    vm_group = parser.add_argument_group("Virtual Node Options", description="These settings are required for virt nodes, along with the network override settings.")
    vm_group.add_argument("--vm-names", type=StrType, metavar="NAME", help="the name of the VMs to RTFI")
    vm_group.add_argument("-s", "--vm-mgmt-server", type=IPv4AddressType, default=sfdefaults.vm_mgmt_server, metavar="IP", help="the management server for the VMs (vSphere for VMware, hypervisor for KVM)")
    vm_group.add_argument("-e", "--vm-mgmt-user", type=StrType, default=sfdefaults.vm_mgmt_user, help="the VM management server username")
    vm_group.add_argument("-a", "--vm-mgmt-pass", type=StrType, default=sfdefaults.vm_mgmt_pass, help="the VM management server password")

    special_group = parser.add_argument_group("Special Options")
    special_group.add_argument("--test", action="store_true", default=False, help=SUPPRESS) # test the supplied options and nodes
    special_group.add_argument("--fail", type=StrType, help="inject an error during this RTFI state")

    args = parser.parse_args_to_dict(allowExtraArgs=True)

    app = PythonApp(RtfiNodes, args)
    app.Run(**args)
