#!/usr/bin/env python
"""
SolidFire node object and related data structures
"""
import libsf
from libsf import mylog
import string
import multiprocessing
import time

class SFNode(object):
    """
    Common interactions with a SolidFire node
    """
    def __init__(self, ip, sshUsername=None, sshPassword=None, clusterMvip=None, clusterUsername=None, clusterPassword=None, ipmiIP=None, ipmiUsername=None, ipmiPassword=None):
        self.ipAddress = ip
        self.sshUsername = sshUsername
        self.sshPassword = sshPassword
        self.clusterMvip = clusterMvip
        self.clusterUsername = clusterUsername
        self.clusterPassword = clusterPassword
        self.ipmiIP = ipmiIP
        self.ipmiUsername = ipmiUsername
        self.ipmiPassword = ipmiPassword

    def GetCoreFileList(self, since = 0):
        """
        Get a list of core files on this node

        Args:
            since: only check for cores that were created after this time (integer unix timestamp)

        Returns:
            A list of core filenames (strings) or an empty list if there are none
        """

        timestamp = libsf.TimestampToStr(since, "%Y%m%d%H%M.%S")
        command = "touch -t " + timestamp + " /tmp/timestamp;find /sf -maxdepth 1 \\( -name \"core*\" ! -name \"core.zktreeutil*\" \\) -newer /tmp/timestamp"
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        result = stdout.readlines()
        result = map(string.strip, result)
        return result

    def ListDrives(self):
        """
        Get a list of the drives in this node
        """
        result = libsf.CallApiMethod(self.clusterMvip, self.clusterUsername, self.clusterPassword, "ListActiveNodes", {})

        for node in result["nodes"]:
            if node["mip"] == self.ipAddress:
                node_id = node["nodeID"]
        if not node_id:
            result = libsf.CallApiMethod(self.clusterMvip, self.clusterUsername, self.clusterPassword, "ListPendingNodes", {})
            for node in result["nodes"]:
                if node["mip"] == self.ipAddress:
                    node_id = node["nodeID"]
        if not node_id:
            raise libsf.SfUnknownObjectError("Could not find node " + self.ipAddress + " in cluster " + self.clusterMvip)

        drive_list = []
        result = libsf.CallApiMethod(self.clusterMvip, self.clusterUsername, self.clusterPassword, "ListDrives", {})
        for drive in result["drives"]:
            if drive["nodeID"] == node_id:
                drive_list.append(drive)

        return drive_list

    def Reboot(self, waitForUp=True):
        """
        Gracefully reboot this node
        """
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        libsf.ExecSshCommand(ssh, "shutdown now -r")
        ssh.close()

        self.WaitForDown()

        if waitForUp:
            self.WaitForUp()

    def PowerOn(self, waitForUp=True):
        """
        Power on this node
        """
        libsf.IpmiCommand(self.ipmiIP, self.ipmiUsername, self.ipmiPassword, "chassis power on")

        if waitForUp:
            self.WaitForUp()

    def PowerOff(self):
        """
        Power on this node
        """
        libsf.IpmiCommand(self.ipmiIP, self.ipmiUsername, self.ipmiPassword, "chassis power off")
        self.WaitForDown()

    def WaitForDown(self):
        """
        Wait for this node to be down and no longer responding on the network
        """
        mylog.info("Waiting for " + self.ipAddress + " to go down")
        while (libsf.Ping(self.ipAddress)):
            time.sleep(1)

    def WaitForUp(self, timeOut=600):
        """
        Wait for this node to be up on the network
        """
        start_time = time.time()
        mylog.info("Waiting for " + self.ipAddress + " to come up")
        time.sleep(120)
        while (not libsf.Ping(self.ipAddress)):
            time.sleep(1)
            current_time = time.time()
            if current_time - start_time >= timeOut:
                mylog.error("Method WaitForUp timed out after " + str(timeOut) + " seconds")
                return False

        # Wait a few extra seconds for services to be started up
        time.sleep(10)

    def SetClusterName(self, clusterName):
        """
        Set the cluster name for this node

        Args:
            clusterName: the name of the cluster
        """
        params = {}
        params["cluster"] = {}
        params["cluster"]["cluster"] = clusterName
        libsf.CallNodeApiMethod(self.ipAddress, self.clusterUsername, self.clusterPassword, "SetConfig", params)

    def SetHostname(self, hostname):
        """
        Set the hostname for this node

        Args:
            hostname: the new hostname for this node
        """
        params = {}
        params["cluster"] = {}
        params["cluster"]["name"] = hostname
        libsf.CallNodeApiMethod(self.ipAddress, self.clusterUsername, self.clusterPassword, "SetConfig", params)

    def SetTime(self, timeString):
        """
        Set the time on this node

        Args:
            timeString: the time to set, as a string format that 'date' will accept
        """
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        command = "date -s \"" + timeString + "\"; echo $?"
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        lines = stdout.readlines()
        if (int(lines.pop().strip()) != 0):
            raise libsf.SfError("Failed to set time: " + "\n".join(stderr.readlines()))
        command = "date +%s"
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        lines = stdout.readlines()
        return int(lines[0])
#nodeIP, username, password, onegIP, onegNetmask, onegGateway, tengIP, tengNetmask, dnsIP, dnsSearch
    def SetNetworkInfo(self, onegIP, onegNetmask, onegGateway, tengIP, tengNetmask, dnsIP, dnsSearch):
        """
        Set the network info on this node
        """

        # Must be done in a thread, because after changing the IP the old IP is no longer responsive and the API call hangs
        start_time = time.time()
        manager = multiprocessing.Manager()
        status = manager.dict()
        status["success"] = False
        status["message"] = None
        th = multiprocessing.Process(target=self._SetNetworkInfoThread, args=(onegIP, onegNetmask, onegGateway, dnsIP, dnsSearch, tengIP, tengNetmask, status))
        th.daemon = True
        th.start()
        while True:
            if not th.is_alive():
                break

            if time.time() - start_time > 30:
                mylog.debug("Terminating subprocess after timeout")
                th.terminate()
                status["success"] = True
                break

        if not status["success"]:
            raise libsf.SfError(status["message"])

        # Try to ping the new address to make sure it came up
        start_time = time.time()
        pingable = False
        while not pingable:
            pingable = libsf.Ping(onegIP)
            if time.time() - start_time > 60:
                break

        if not pingable:
            raise libsf.SfError("Could not ping node at new address")

        # Update my internal data
        self.ipAddress = onegIP

    def _SetNetworkInfoThread(self, onegIP, onegNetmask, onegGateway, dnsIP, dnsSearch, tengIP, tengNetmask, status):
        params = {}
        params["network"] = {}
        params["network"]["Bond1G"] = {}
        params["network"]["Bond1G"]["address"] = onegIP
        params["network"]["Bond1G"]["netmask"] = onegNetmask
        params["network"]["Bond1G"]["gateway"] = onegGateway
        params["network"]["Bond1G"]["dns-nameservers"] = dnsIP
        params["network"]["Bond1G"]["dns-search"] = dnsSearch
        params["network"]["Bond10G"] = {}
        params["network"]["Bond10G"]["address"] = tengIP
        params["network"]["Bond10G"]["netmask"] = tengNetmask
        try:
            libsf.CallNodeApiMethod(self.ipAddress, self.clusterUsername, self.clusterPassword, "SetConfig", params)
            status["success"] = True
        except libsf.SfApiError as e:
            status["success"] = False
            status["message"] = str(e)

    def GetHostname(self):
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        command = "hostname; echo $?"
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        lines = stdout.readlines()
        if (int(lines.pop().strip()) != 0):
            raise libsf.SfError("Failed to get hostname: " + "\n".join(stderr.readlines()))
        return lines[0].strip()

    def GetSfappVersion(self):
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        command = "/sf/bin/sfapp --Version; echo $?"
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        lines = stdout.readlines()
        if (int(lines.pop().strip()) != 0):
            raise libsf.SfError("Failed to get version: " + "\n".join(stderr.readlines()))
        return lines[0].strip()

    def KillMasterService(self):
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        command = "top -b -n 1 | grep master-1"
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)
        lines = stdout.readlines()
        index = lines[0].strip().index(" ")
        command = "kill " + str(lines[0].strip()[:index])
        stdin, stdout, stderr = libsf.ExecSshCommand(ssh, command)

    def AddNetworkRoute(self, network, subnetMask, gateway):
        """
        Add a network route to this node
        """

        # Per-node API is broken for this in C right now
        #params = {}
        #params["network"] = {}
        #params["network"]["Bond10G"] = {}
        #params["network"]["Bond10G"]["routes"] = []
        #route = {}
        #route["type"] = "net"
        #route["target"] = network
        #route["netmask"] = subnetMask
        #route["gateway"] = gateway
        #params["network"]["Bond10G"]["routes"].append(route)
        #libsf.CallNodeApiMethod(self.ipAddress, self.clusterUsername, self.clusterPassword, "SetConfig", params)

        # Temporary until C is fixed - this does not persist across reboots
        if not self.sshUsername:
            self.sshUsername = "root"
        ssh = libsf.ConnectSsh(self.ipAddress, self.sshUsername, self.sshPassword)
        command = "route add -net " + network + " netmask " + subnetMask + " gw " + gateway
        mylog.debug("Executing '" + command + "' on host " + self.ipAddress)
        stdin, stdout, stderr = ssh.exec_command(command)
        return_code = stdout.channel.recv_exit_status()
        stdout_data = stdout.readlines()
        stderr_data = stderr.readlines()
        if return_code != 0:
            raise libsf.SfError("Failed to add route: " + "\n".join(stderr_data))
