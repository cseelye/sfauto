#!/usr/bin/env python2.7
"""
SolidFire node object and related data structures
"""
import platform
import re
import time

from . import netutil
from . import sfdefaults
from . import threadutil
from . import util
from . import SSHConnection, SolidFireClusterAPI, SolidFireBootstrapAPI, SolidFireNodeAPI, SolidFireError, UnknownObjectError, TimeoutError
from .shellutil import Shell
from .logutil import GetLogger
from .virtutil import VirtualMachine
import six

class NetworkInterfaceType(object):
    """Type of network interfaces in nodes"""
    Loopback = "Loopback"
    BondMaster = "BondMaster"
    BondSlave = "BondSlave"
    Vlan = "Vlan"
    VirtualBondMaster = "VirtualBondMaster"
    VirtualVlan = "VirtualVlan"

class DriveType(object):
    """Type of drives in cluster/node"""
    Any = "any"
    Block = "block"
    Slice = "volume"
    Unknown = "unknown"

class SFNode(object):
    """Common interactions with a SolidFire node"""
    
    DEFAULT_IPMI_USERNAME = "root"
    DEFAULT_IPMI_PASSWOD = "calvin"

    def __init__(self, ip, sshUsername=None, sshPassword=None, clusterMvip=None, clusterUsername=None, clusterPassword=None, ipmiIP=None, ipmiUsername=None, ipmiPassword=None, vmName=None, vmManagementServer=None, vmManagementUsername=None, vmManagementPassword=None):
        self.ipAddress = ip
        self.sshUsername = sshUsername
        self.sshPassword = sshPassword
        self.mvip = clusterMvip
        self.username = clusterUsername
        self.password = clusterPassword
        self.ipmiIP = ipmiIP
        self.ipmiUsername = ipmiUsername or SFNode.DEFAULT_IPMI_USERNAME
        self.ipmiPassword = ipmiPassword or SFNode.DEFAULT_IPMI_PASSWOD
        self.log = GetLogger()
        self.api = SolidFireNodeAPI(self.ipAddress,
                                    self.username,
                                    self.password,
                                    logger=self.log,
                                    maxRetryCount=5,
                                    retrySleep=20,
                                    errorLogThreshold=1,
                                    errorLogRepeat=1)
        self.clusterAPI = SolidFireClusterAPI(self.mvip,
                                              self.username,
                                              self.password,
                                              logger=self.log,
                                              maxRetryCount=5,
                                              retrySleep=20,
                                              errorLogThreshold=1,
                                              errorLogRepeat=1)
        self.vmName = vmName
        self.vm = None
        if self.vmName:
            self.vm = VirtualMachine.Attach(vmName, vmManagementServer, vmManagementUsername, vmManagementPassword)
            self.log.debug("Node {} is VM {}".format(self.ipAddress, str(self.vm)))

        self._unpicklable = ["log", "api", "clusterAPI"]


    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.items():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()
        self.api = SolidFireNodeAPI(self.ipAddress,
                                    self.username,
                                    self.password,
                                    logger=self.log,
                                    maxRetryCount=5,
                                    retrySleep=20,
                                    errorLogThreshold=1,
                                    errorLogRepeat=1)
        self.clusterAPI = SolidFireClusterAPI(self.mvip,
                                       self.username,
                                       self.password,
                                       logger=self.log,
                                       maxRetryCount=5,
                                       retrySleep=20,
                                       errorLogThreshold=1,
                                       errorLogRepeat=1)

        for key in self._unpicklable:
            assert hasattr(self, key)

    def __str__(self):
        return "SFNode {} version {}".format(self.ipAddress, self.GetNodeVersion())

    def IsVirtual(self):
        """
        Check if this is a virtual node

        Returns:
            True if this is a virt node, False otherwise
        """
        if self.vm:
            return True
        else:
            return False

    def GetHighestVersion(self):
        """
        Get the highest API version this node supports

        Returns:
            Floating point version number
        """
        result = self.api.CallWithRetry("GetAPI", {}, apiVersion=0.0)
        return float(result["supportedVersions"][-1])

    def GetNodeVersion(self):
        """
        Get the version of SF software on this node

        Returns:
            SolidFire version (str)
        """
        version = self.api.CallWithRetry("GetVersionInfo")
        for key in ["sfapp", "sfconfig"]:
            if key in version["versionInfo"]:
                return version["versionInfo"][key]["Version"]

    def GetNodeID(self):
        """
        Get the cluster nodeID of this node

        Returns:
            Integer node ID
        """
        result = self.clusterAPI.CallWithRetry("ListAllNodes", {})
        for node in result["nodes"] + result["pendingNodes"]:
            if node["mip"] == self.ipAddress:
                return node["nodeID"]

        raise UnknownObjectError("Could not find node {} in cluster {}".format(self.ipAddress, self.mvip))

    def FindIPMIAddress(self):
        """
        Find the IPMI management address of this node

        Returns:
            String containing the IPMI IP address
        """
        if self.vm:
            raise NotImplementedError("Cannot get IPMI IP for virtual machines")

        with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
            _, stdout, _ = ssh.RunCommand(r"ipmitool lan print | egrep 'IP Address\s+:' | awk '{print $4}'")
            return stdout.strip()

    def IPMICommand(self, command):
        """
        Execute an IPMI command against this node

        Returns:
            The stdout of the command (str)
        """
        if self.vm:
            raise NotImplementedError("Cannot execute IPMI commands against virtual machines")

        if "Darwin" in platform.system():
            retcode, _, _ = Shell("ipmitool -h 2>&1 | grep -- -I")
            if retcode != 0:
                raise SolidFireError("Please install an ipmitool that supports the -I option")

        cmd = "ipmitool -Ilanplus -U{} -P{} -H{} {}".format(self.ipmiUsername, self.ipmiPassword, self.ipmiIP, command)
        self.log.debug(cmd)
        retry = 3
        retcode = None
        stdout = ""
        stderr = ""
        while retry > 0:
            retcode, stdout, stderr = Shell(cmd)
            if retcode == 0:
                self.log.debug2(stdout)
                break
            retry -= 1
            time.sleep(sfdefaults.TIME_SECOND * 3)
        if retcode != 0:
            raise SolidFireError("ipmitool error: " + stdout + stderr)
        return stdout

    def GetCoreFileList(self, since=0):
        """
        Get a list of core files on this node

        Args:
            since: only check for cores that were created after this time (integer unix timestamp)

        Returns:
            A list of core filenames (strings) or an empty list if there are none
        """
        if self.GetHighestVersion() >= 7.0:
            result = self.api.CallWithRetry("ListCoreFiles", {})
            return result["coreFiles"]
        else:
            timestamp = util.TimestampToStr(since, "%Y%m%d%H%M.%S")
            command = "touch -t " + timestamp + " /tmp/timestamp;find /sf -maxdepth 1 \\( -name \"core*\" ! -name \"core.zktreeutil*\" \\) -newer /tmp/timestamp"
            with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
                _, stdout, _ = ssh.RunCommand(command)
                return [line.strip() for line in stdout.split("\n")]

    def Reboot(self, waitForUp=True):
        """
        Gracefully reboot this node (kexec on Element 8+)

        Args:
            waitForUp:  wait for the node to reboot and come back up
        """
        with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
            ssh.RunCommand("shutdown now -r")

        self.WaitForDown()

        if waitForUp:
            self.WaitForUp(initialWait=10)

    def HardReboot(self, waitForUp=True):
        """
        Hard reboot the node, skipping graceful shutdown and kexec, and fully rebooting the hardware
        
        Args:
            waitForUp:  wait for the node to reboot and come back up
        """
        with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
            ssh.RunCommand("reboot -f")

        self.WaitForDown()

        if waitForUp:
            wait = 20 if self.vm else 180
            self.WaitForUp(initialWait=wait)

    def PowerOn(self, waitForUp=True):
        """
        Power on this node

        Args:
            waitForUp:  wait for the node to turn on and come fully up
        """
        
        if self.vm:
            self.vm.PowerOn()

        else:
            self.IPMICommand("chassis power on")
            self.log.info("Waiting for {} to turn on".format(self.ipAddress))
            while True:
                if self.GetPowerState() == "on":
                    break
                time.sleep(sfdefaults.TIME_SECOND)

        if waitForUp:
            wait = 20 if self.vm else 180
            self.WaitForUp(initialWait=wait)

    def PowerOff(self):
        """
        Power on this node
        """
        if self.vm:
            self.vm.PowerOff()

        else:
            self.IPMICommand("chassis power off")
            self.log.info("Waiting for {} to turn off".format(self.ipAddress))
            while True:
                if self.GetPowerState() == "off":
                    break
                time.sleep(sfdefaults.TIME_SECOND)

    def GetPowerState(self):
        """
        Get the power state (on, off) of this node

        Returns:
            A string with the current power state (str)
        """
        if self.vm:
            return self.vm.GetPowerState()
        else:
            stdout = self.IPMICommand("chassis power status")
            m = re.search(r"Chassis Power is (\S+)", stdout)
            if m:
                return m.group(1)
            raise SolidFireError("Could not determine power state")

    def WaitForDown(self):
        """
        Wait for this node to be no longer responding on the network
        """
        self.log.info("Waiting for {} to go down".format(self.ipAddress))
        while (netutil.Ping(self.ipAddress)):
            time.sleep(sfdefaults.TIME_SECOND)

    def WaitForOff(self, timeout=180):
        """
        Wait for this node's power state to be OFF
        This is checking a state, not a transition
        """
        start_time = time.time()
        while True:
            if self.GetPowerState() == "off":
                break
            if time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for node {} to power off".format(self.ipAddress))
            time.sleep(2 * sfdefaults.TIME_SECOND)

    def WaitForOn(self, timeout=300):
        """
        Wait for this node's power state to be ON
        This is checking a state, not a transition
        """
        start_time = time.time()
        while True:
            if self.GetPowerState() == "on":
                break
            if time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for node {} to power on".format(self.ipAddress))
            time.sleep(2 * sfdefaults.TIME_SECOND)

    def WaitForPing(self, timeout=300):
        """
        Wait for the IP of this node t be pingable

        Args:
            timeout:        how long to wait for the node
        """
        start_time = time.time()
        self.log.info("Waiting for {} to be pingable".format(self.ipAddress))
        while (not netutil.Ping(self.ipAddress)):
            time.sleep(sfdefaults.TIME_SECOND)
            current_time = time.time()
            if current_time - start_time >= timeout:
                raise TimeoutError("Timeout waiting for node {} to come up".format(self.ipAddress))

    def WaitForUp(self, timeout=600, initialWait=0):
        """
        Wait for this node to be up on the network

        Args:
            timeout:        how long to wait for the node
            initialWait:    how log to wait before checking the first time
        """
        start_time = time.time()
        self.log.info("Waiting for {} to be pingable".format(self.ipAddress))
        time.sleep(sfdefaults.TIME_SECOND * initialWait)
        while (not netutil.Ping(self.ipAddress)):
            time.sleep(sfdefaults.TIME_SECOND)
            current_time = time.time()
            if current_time - start_time >= timeout:
                raise TimeoutError("Timeout waiting for node {} to come up".format(self.ipAddress))

        self.WaitForNodeAPI()

    def WaitForNodeAPI(self):
        """
        Wait for the node API to be up and ready
        """
        self.log.info("Waiting for {}:442 API to be up".format(self.ipAddress))
        self.api.WaitForUp()

    def WaitForBootstrapAPI(self):
        """
        Wait for the bootstrap API to be up and ready
        """
        bootstrap = SolidFireBootstrapAPI(self.ipAddress)
        bootstrap.WaitForUp()

    def IsUp(self):
        """
        Check if this node is up and running
        """

        # Test if the network is up
        if not netutil.Ping(self.ipAddress):
            return False

        # Test if the API is responding
        try:
            self.api.Call("GetAPI", timeout=10)
        except SolidFireError:
            return False

        return True

    def SetClusterName(self, clusterName):
        """
        Set the cluster name for this node

        Args:
            clusterName: the name of the cluster
        """
        params = {}
        params["cluster"] = {}
        params["cluster"]["cluster"] = clusterName
        self.api.CallWithRetry("SetClusterConfig", params)
        # Make sure the bootstrap API is back before returning
        self.log.debug("Waiting for bootstrap API to be ready")
        self.WaitForBootstrapAPI()

    def SetHostname(self, hostname):
        """
        Set the hostname for this node

        Args:
            hostname: the new hostname for this node
        """
        params = {}
        params["cluster"] = {}
        params["cluster"]["name"] = hostname
        self.api.CallWithRetry("SetConfig", params)

    def SetTime(self, timeString):
        """
        Set the time on this node

        Args:
            timeString: the time to set, as a string format that 'date' will accept
        
        Returns:
            An integer unix timestamp representing the time on the node after being set
        """
        with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
            ssh.RunCommand("date -s \"{}\"".format(timeString))
            _, stdout, _ = ssh.RunCommand("date +%s")
            return int(stdout.strip())

    def _SetInternalManagementIP(self, ipAddress):
        """
        Update the internal model for this node object

        Args:
            ipAddress:      the management IP of this node (str)
        """
        self.log.debug2("Using managment IP {} for node".format(ipAddress))
        self.ipAddress = ipAddress
        self.api = SolidFireNodeAPI(self.ipAddress,
                                    self.username,
                                    self.password,
                                    logger=self.log,
                                    maxRetryCount=5,
                                    retrySleep=20,
                                    errorLogThreshold=1,
                                    errorLogRepeat=1)

    def SetNetworkConfig(self, managementIP=None, managementNetmask=None, managementGateway=None, dnsIP=None, dnsSearch=None, storageIP=None, storageNetmask=None, storageGateway=None):
        """
        Set the network info on this node. All params are optional and only the iones supplied will be changed

        Args:
            managementIP:       the IP address for the managment NIC (MIPI)
            managementNetmask:  the netmask for the managment NIC (MIPI)
            managementGateway:  the gateway for the managment NIC (MIPI)
            dnsIP:              the IP of the DNS server
            dnsSearch:          the search string for DNS lookups
            storageIP:          the IP address for the storage NIC (SIPI)
            storageNetmask:     the netmask for the storage NIC (SIPI)
            storageGateway:     the gateway for the storage NIC (SIPI)
        """
        all_config = self.api.CallWithRetry("GetConfig")
        mgmt_nic = all_config["config"]["cluster"]["mipi"]
        stg_nic = all_config["config"]["cluster"]["sipi"]
        params = { "network" : all_config["config"]["network"] }
        # Filter the NICs we are interested in
        params["network"] = { nic : params["network"][nic] for nic in params["network"] if nic in (mgmt_nic, stg_nic) }

        update_network = {
            mgmt_nic : {
                "address" : managementIP or params["network"][mgmt_nic]["address"],
                "netmask" : managementNetmask or params["network"][mgmt_nic]["netmask"],
                "gateway" : managementGateway or params["network"][mgmt_nic]["gateway"],
                "dns-nameservers" : dnsIP or params["network"][mgmt_nic]["dns-nameservers"],
                "dns-search" : dnsSearch or params["network"][mgmt_nic]["dns-search"],
            },
            stg_nic : {
                "address" : storageIP or params["network"][stg_nic]["address"],
                "netmask" : storageNetmask or params["network"][stg_nic]["netmask"],
                "gateway" : storageGateway or params["network"][stg_nic]["gateway"],
            }
        }
        params["network"][mgmt_nic].update(update_network[mgmt_nic])
        params["network"][stg_nic].update(update_network[stg_nic])

        # Send the API call in a thread, because after changing the IP the old IP is no longer responsive and the API call hangs
        pool = threadutil.GlobalPool()
        result = pool.Post(self.api.Call, "SetNetworkConfig", params)
        try:
            result.GetWithTimeout(30)
        except TimeoutError:
            pass

        # Wait for the network to be up on the new IP
        self._SetInternalManagementIP(managementIP)
        self.WaitForPing(60)

        # Wait for the API to be ready on the new IP
        self.WaitForNodeAPI()

    def GetHostname(self):
        """
        Get the hostname of this node
        
        Returns:
            String containing the hostname (str)
        """
        result = self.api.CallWithRetry("GetClusterConfig")
        return result["cluster"]["name"]

    def IsDHCPEnabled(self):
        """
        Check if DHCP is enabled on any interfaces in this node

        Returns:
            True if DHCP is enabled, False otherwise (bool)
        """
        res = self.api.CallWithRetry("GetNetworkConfig")
        for interface in res["network"].values():
            if interface["method"] == "dhcp":
                return True
        return False

    def GetSfappVersion(self):
        """
        Get the version of sfapp running on this node
        
        Returns:
            Dictionary of version info (dict)
        """
        return self.api.CallWithRetry("GetVersionInfo")["versionInfo"]["sfapp"]

    def KillMasterService(self):
        """
        Send a kill signal to the MS on this node
        """
        if self.GetHighestVersion() >= 7.3:
            result = self.api.CallWithRetry("GetProcessInfo", {"processes" : ["sfapp"]}, apiVersion=7.0)
            for proc in result["processInfo"]["processes"]:
                if "master" in proc["command"]:
                    pid = proc["pid"]
            result = self.api.CallWithRetry("KillProcesses", {"pids" : [pid], "signal" : 9}, apiVersion=7.3)
        else:
            with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
                _, stdout, _ = ssh.RunCommand("ps -eo pid,args | grep sfapp | grep master | awk '{print $1}'")
                pid = stdout.strip()
                ssh.RunCommand("kill -9 {}".format(pid))

    def AddNetworkRoute10G(self, network, subnetMask, gateway):
        """
        Add a 10G network route to this node

        Args:
            network:    the network of the route to add
            subnetMask: the netmask of the route to add
            gateway:    the gateway for the route to add
        """
        params = {}
        params["network"] = {}
        params["network"]["Bond10G"] = {}
        params["network"]["Bond10G"]["routes"] = []
        route = {}
        route["type"] = "net"
        route["target"] = network
        route["netmask"] = subnetMask
        route["gateway"] = gateway
        params["network"]["Bond10G"]["routes"].append(route)
        self.api.CallWithRetry("SetConfig", params)

    def ListNetworkNamespaceInfo(self):
        """
        Get a list of network spaces on this node.  This will always include a single "base" namespace entry, plus
        any others that exist on the node

        Returns:
            A dictionary of namespace name => namespace info dictionary, one for each namespace
        """
        result = self.api.CallWithRetry("ListNetworkNamespaceInfo", {}, apiVersion=9.0)
        return result["namespaces"]

    def ListNetworkInterfaces(self, interfaceType=None):
        """
        Get a list of network interfaces on this node.

        Args:
            type:   Type of interface(s) to list - this may be a scalar or a list. Use None to get all interfaces

        Returns:
            A list of interface dictionaries
        """
        result = self.api.CallWithRetry("ListNetworkInterfaces", {}, apiVersion=7.0)
        if not interfaceType:
            return result["interfaces"]

        if not isinstance(interfaceType, list):
            interfaceType = [interfaceType]
        filtered = []
        for iface in result["interfaces"]:
            if iface["type"] in interfaceType:
                filtered.append(iface)
        return filtered

    def GetExpectedDriveCount(self, driveType=DriveType.Any):
        """
        Get the expected number of drives for this node

        Args:
            driveType:  the type of drives to count

        Returns:
            An integer number of drives
        """
        result = self.api.CallWithRetry("GetDriveConfig", {}, apiVersion=6.0)
        if driveType == DriveType.Any:
            return result["driveConfig"]["numTotalExpected"]
        elif driveType == DriveType.Block:
            return result["driveConfig"]["numBlockExpected"]
        elif driveType == DriveType.Slice:
            return result["driveConfig"]["numSliceExpected"]

    def GetDriveConfig(self):
        """
        Get the drive config for this node

        Returns:
            A
        """
        result = self.api.CallWithRetry("GetDriveConfig", {}, apiVersion=6.0)
        return result["driveConfig"]

    def StartRTFI(self, repository, version, additionalOptions="", baseOptions=sfdefaults.rtfi_options, buildServer=sfdefaults.pxe_server):
        """
        Start an in place RTFI of this node

        Args:
            repository:     the full release/repository name (e.g. fluorine, or fluorine-branchname)
            version:        the version string (e.g. 9.0.0.999)
            options:        the options to pass to RTFI
            buildServer:    the server the build is hsoted on
        """
        params = {}
        params["build"] = "ftp://{}/images/rtfi/solidfire-rtfi-{}-{}".format(buildServer, repository, version)
        params["options"] = "{},{}".format(baseOptions, additionalOptions)
        self.api.Call("StartRtfi", params, apiVersion=self.GetHighestVersion(), timeout=200)

    def GetLatestRTFIStatus(self):
        """
        Get the current status of RTFI from the node

        Returns:
            A dictionary of status information (dict)
        """
        return self.api.GetLatestRtfiStatus()

    def GetAllRTFIStatus(self):
        """
        Get the history of all status of RTFI from the node

        Returns:
            A list of dictionaries of status information (list of dict)
        """
        return self.api.GetAllRtfiStatus()

    def SaveRTFILog(self, localPath):
        """
        Save the RTFI log from the node

        Args:
            localPath:      the path to save the log to
        """
        self.api.GetRtfiLog(localPath)

    def GetPXEMacAddress(self):
        """
        Get the MAC address of the NIC used for PXE booting

        Returns:
            A string MAC address (str)
        """
        if self.vm:
            return self.vm.GetPXEMacAddress()

        else:
            ex = None
            with SSHConnection(self.ipmiIP, self.ipmiUsername, self.ipmiPassword) as ssh:
                # R630
                _, stdout, _ = ssh.RunCommand("racadm get NIC.VndrConfigPage.3.MacAddr", pipeFail=False)
                for line in stdout.split("\n"):
                    m = re.match(r"error_tag\s+:\s(.+)$", line.strip())
                    if m:
                        ex = SolidFireError("racadm command failed: {}".format(m.group(1)))
                        break
                    if line.startswith("ERROR"):
                        ex = SolidFireError("racadm command failed: {}".format(line))
                        break
                    m = re.match(r"MacAddr=(\S+)", line.strip())
                    if m:
                        return m.group(1)
                # R620
                _, stdout, _ = ssh.RunCommand("racadm get NIC.VndrConfigGroup.3.MacAddr", pipeFail=False)
                for line in stdout.split("\n"):
                    m = re.match(r"error_tag\s+:\s(.+)$", line.strip())
                    if m:
                        ex = SolidFireError("racadm command failed: {}".format(m.group(1)))
                        break
                    if line.startswith("ERROR"):
                        ex = SolidFireError("racadm command failed: {}".format(line))
                        break
                    m = re.match(r"MacAddr=(\S+)", line.strip())
                    if m:
                        return m.group(1)
            if ex:
                raise ex
            raise SolidFireError("Could not find MAC address")

    def SetPXEBoot(self):
        """
        Set the boot order of the node so that it will PXE boot before local disk
        """
        self.log.debug("Setting boot override to PXE boot")
        if self.vm:
            self.vm.SetPXEBoot()

        else:
            self.IPMICommand("chassis bootdev pxe")

    def Ping(self):
        """
        Ping this node's management IP

        Returns:
            Boolean true if the node can be pinged, false otherwise (bool)
        """
        return netutil.Ping(self.ipAddress)

    def TestAPI(self):
        """
        Test if the API is up and responding on this node
        """
        try:
            self.api.Call("GetAPI", timeout=15)
            return True
        except SolidFireError:
            return False

    def CleanLogs(self):
        """
        Empty current log files on the node
        """
        with SSHConnection(self.ipAddress, self.sshUsername, self.sshPassword) as ssh:
            ssh.RunCommand("sudo logrotate /etc/logrotate.d/solidfire")
