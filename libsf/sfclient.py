#!/usr/bin/env python2.7
"""
Client objects and data structures
"""

import os
import platform
import re
import sys
import time

from . import sfdefaults
from . import netutil
from . import shellutil
from . import util
from . import SSHConnection, ClientError, ClientCommandError,ClientAuthorizationError, ClientRefusedError, ClientConnectionError
from .logutil import GetLogger

class OSType(object):
    """Enumeration of known client types"""
    Linux, MacOS, Windows, ESX, SunOS = ("Linux", "MacOS", "Windows", "ESX", "SunOS")

def _prefix(logfn):
    """Add the client IP to a log message"""
    def wrapped(self, message):
        if isinstance(message, basestring):
            message = message.rstrip()
            if not message:
                message = "  <empty msg>"
        message = "  {}: {}".format(self.ipAddress, message)
        logfn(self, message)
    return wrapped

#pylint: disable=method-hidden,protected-access
class SFClient:
    """Common interactions with a client"""

    def __init__(self, clientIP, clientUser, clientPass, clientTypeHint=None):
        self.log = GetLogger()

        self.ipAddress = str(clientIP).lower()
        self.username = str(clientUser)
        self.password = str(clientPass)
        self.isLocalhost = False
        self.localOS = None
        self.remoteOS = None
        self.remoteOSVersion = ""
        self.sshSession = None
        self.hostname = None
        self.allIPAddresses = []
        self.chapCredentials = {}

        # Find the path to the current directory (libsf) where various file we need are stored
        self.libPath = os.path.dirname(os.path.realpath(__file__))

        if sys.platform.startswith("linux"):
            self.localOS = OSType.Linux
        elif sys.platform.startswith("darwin"):
            self.localOS = OSType.MacOS
        elif sys.platform.startswith("win"):
            self.localOS = OSType.Windows
        else:
            raise ClientError("Unsupported local OS " + sys.platform)

        self._unpicklable = ["log", "sshSession"]

        self._Connect(clientTypeHint)

    def __getstate__(self):
        attrs = {}
        for key, value in self.__dict__.iteritems():
            if key not in self._unpicklable:
                attrs[key] = value
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.log = GetLogger()
        self.sshSession = None

        for key in self._unpicklable:
            assert hasattr(self, key)

    # Local logging functions that include the client IP address
    #pylint: disable=missing-docstring
    @_prefix
    def _debug(self, message):
        self.log.debug(message)
    @_prefix
    def _info(self, message):
        self.log.info(message)
    @_prefix
    def _warn(self, message):
        self.log.warning(message)
    @_prefix
    def _error(self, message):
        self.log.error(message)
    @_prefix
    def _passed(self, message):
        self.log.passed(message)
    @_prefix
    def _step(self, message):
        self.log.step(message)
    #pylint: enable=missing-docstring

    def _Connect(self, clientTypeHint=None):
        """
        Connect to the client

        Args:
            clientIP:           the IP address of the client
            clientUser:         the username for the client
            clientPass:         the password for the client
            clientTypeHint:     assume the client is this type instead of trying to discover
        """

        if self.ipAddress == "localhost":
            if "win" in platform.system().lower():
                raise ClientError("Sorry, running on Windows is not supported")
            _, stdout, _ = shellutil.Shell("uname -a")
            uname_str = stdout.strip().lower()

            if "win" in uname_str:
                raise ClientError("Sorry, running on Windows is not supported")
            elif "linux" in uname_str:
                self.remoteOS = OSType.Linux
                if "ubuntu" in uname_str or "solidfire-element" in uname_str:
                    self.remoteOSVersion = "ubuntu"
                elif "el" in uname_str:
                    self.remoteOSVersion = "redhat"
                elif "fc" in uname_str:
                    self.remoteOSVersion = "redhat"
            elif "vmkernel" in uname_str:
                self.remoteOS = OSType.ESX
                m = re.search(r" (\d\.\d)\.\d+ ", uname_str)
                if m:
                    self.remoteOSVersion = m.group(1)
            elif "sunos" in uname_str:
                self.remoteOS = OSType.SunOS
            else:
                self._warn("Could not determine type of client; assuming Linux (uname -> {})".format(uname_str))
                self.remoteOS = OSType.Linux

            self.hostname = platform.node()
            return

        # First try with winexe to see if it is a Windows client
        if clientTypeHint == None or clientTypeHint == OSType.Windows:
            try:
                self._debug("Attempting connect to {} with winexe as {}:{}".format(self.ipAddress, self.username, self.password))
                retcode, stdout, stderr = self._execute_winexe_command(self.ipAddress, "hostname", timeout=3)
                if retcode != 0:
                    raise ClientCommandError("Could not get hostname: {}".format(stderr))

                # If we get here, we successfully connected - must be a windows machine
                self.remoteOS = OSType.Windows
                self.hostname = stdout.strip()

                # Make sure winexe will survive a reboot
                self._execute_winexe_command(self.ipAddress, "sc config winexesvc start= auto")

                # Make sure the latest version of diskapp.exe is on the client
                self._debug("Updating diskapp")
                if self.localOS == OSType.Windows:
                    command = r"robocopy {}\..\windiskhelper \\{}\c$\Windows\System32 diskapp.* /z /r:1 /w:3 /tbd".format(self.libPath, self.ipAddress)
                    retcode, stdout, stderr = shellutil.Shell(command)
                    if retcode != 0:
                        raise ClientError("Failed to update diskapp on client: {}".format(stderr))
                else:
                    command = "smbclient //{}/c$ {} -U {} <<EOC\ncd Windows\\System32\nlcd {}/../windiskhelper\nput diskapp.exe\nexit\nEOC".format(self.ipAddress, self.password, self.username, self.libPath)
                    retcode, stdout, stderr = shellutil.Shell(command)
                    if retcode != 0:
                        raise ClientError("Failed to update diskapp on client: {}".format(stderr))
                    for line in stdout.split("\n"):
                        if line.startswith("NT_"):
                            raise ClientError("Failed to update diskapp on client: {}".format(line))
            except ClientRefusedError:
                # This means it is not Windows
                pass

        if clientTypeHint != OSType.Windows:
            # If winexe fails, try with SSH
            if self.hostname == None:
                self._debug("Attempting connect to {} with SSH as {}:{}".format(self.ipAddress, self.username, self.password))
                retcode, stdout, stderr = self._execute_ssh_command(self.ipAddress, "hostname")
                if retcode != 0:
                    if "Ambiguous API call" in stdout:
                        raise ClientConnectionError("Server appears to be a vSphere appliance")
                    raise ClientCommandError("Could not get hostname: {}".format(stderr))
                self.hostname = stdout.strip()

                # Check what we are connected to
                retcode, stdout, stderr = self._execute_ssh_command(self.ipAddress, "uname -a")
                if retcode != 0:
                    raise ClientCommandError("Could not get uname: {}".format(stderr))
                uname_str = stdout.lower()
                if "cygwin" in uname_str:
                    raise ClientError("Sorry, cygwin is not supported. This script requires your Windows client accept connections from winexe")
                elif "linux" in uname_str:
                    self.remoteOS = OSType.Linux
                    if "ubuntu" in uname_str:
                        self.remoteOSVersion = "ubuntu"
                    elif "el" in uname_str:
                        self.remoteOSVersion = "redhat"
                    elif "fc" in uname_str:
                        self.remoteOSVersion = "redhat"
                    elif "solidfire" in uname_str:
                        self.remoteOSVersion = "solidfire"
                    else:
                        self.remoteOSVersion = "linux"
                elif "sunos" in uname_str:
                    self.remoteOS = OSType.SunOS
                else:
                    self._warn("Could not determine type of remote client; assuming Linux (uname -> " + uname_str + ")")
                    self.remoteOS = OSType.Linux

        self._debug("Client OS is {} {}".format(self.remoteOS, self.remoteOSVersion))

        # Save a list of all the IPs on this client
        self.allIPAddresses = self.GetIPv4Addresses()

    def _execute_ssh_command(self, clientIP, command):
        """Execute a command on the client using SSH"""
        if not self.sshSession or not self.sshSession.IsAlive():
            self._debug("Connecting SSH")
            self.sshSession = SSHConnection(clientIP, self.username, self.password)
            self.sshSession.Connect()

        retcode, stdout, stderr = self.sshSession.RunCommand(command, exceptOnError=False)
        return retcode, stdout, stderr

    def _close_command_session(self):
        if self.remoteOS == OSType.Linux:
            self.sshSession.Close()
            self.sshSession = None

    def _execute_winexe_command(self, clientIP, command, timeout=30):
        """Execute a command on the client using winexe or psexec"""
        return_code = -9
        stdout = ""
        stderr = ""

        # Run psexec
        if self.localOS == OSType.Windows:
            winexe = os.path.join(self.libPath, r"..\winexe\bin\psexec.exe") + r" \\{} -n 3 -u {} -p {} {}".format(clientIP, self.username, self.password, command)
            return_code, stdout, raw_stderr = shellutil.Shell(winexe, timeout)
            if return_code == None:
                raise ClientRefusedError("Could not connect to client - not responding or not Windows")

            # Parse psexec output from stderr.  stderr will contain a header and footer from psexec and the stderr of the remote
            # command in between
            stderr = ""
            psexec_output = ""
            header = True
            footer = False
            for line in raw_stderr.split("\n"):
                if "Sysinternals - www.sysinternals.com" in line:
                    header = False
                    psexec_output += line + "\n"
                    continue
                if header:
                    psexec_output += line + "\n"
                    continue

                if line.startswith("Connecting to " + clientIP):
                    footer = True
                    psexec_output += line + "\n"
                    continue
                if footer:
                    psexec_output += line + "\n"
                    continue

                stderr += line + "\n"

            if "PsExec could not start" in psexec_output:
                psexec_output += stderr

            if "The user name or password is incorrect" in psexec_output:
                raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
            if "Access is denied" in psexec_output:
                raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
            if "Timeout" in psexec_output:
                raise ClientRefusedError("Could not connect to client - not responding or not Windows")

        # Run winexec
        else:
            if (self.localOS == OSType.Linux):
                winexe = os.path.join(self.libPath, "../winexe/bin/winexe.linux") + " --system -U {}%{} //{} \"{}\"".format(self.username, self.password, clientIP, command)
            elif (self.localOS == OSType.MacOS):
                winexe = os.path.join(self.libPath, "../winexe/bin/winexe.macos") + " --system -U {}%{} //{} \"{}\"".format(self.username, self.password, clientIP, command)
            retry = 4
            while True:
                return_code, stdout, stderr = shellutil.Shell(winexe, timeout)
                # The returncode and stdout/stderr may be the return of the remote command or the return of winexe itself
                # In either case the return code may be non-zero
                # If winexe succeeded but the remote command failed, we want to pass the non-zero return and stdout/stderr back to the caller
                # If winexe itself failed to connect and run the command we want to raise an exception

                # self.log.debug2("return_code={} stdout={}".format(return_code, stdout))

                # Look for known error signatures from winexe, and assume everything else is from the remote command
                if "NT_STATUS_RESOURCE_NAME_NOT_FOUND" in stdout:
                    # usually a transient network error
                    self._debug("Connect to {} failed NT_STATUS_RESOURCE_NAME_NOT_FOUND - retrying".format(clientIP))
                    retry -= 1
                    if retry > 0:
                        time.sleep(sfdefaults.TIME_SECOND)
                        continue
                    else:
                        raise ClientError("Could not connect to Windows client - NT_STATUS_RESOURCE_NAME_NOT_FOUND")
                elif "NT_STATUS_ACCESS_DENIED" in stdout:
                    raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
                elif "NT_STATUS_LOGIN_FAILURE" in stdout:
                    raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
                elif "Failed to install service" in stdout:
                    raise ClientError("Could not install winexe service on Windows client.  Try executing 'sc delete winexesvc' on the client and then try this script again")
                elif "NT_STATUS_CONNECTION_REFUSED" in stdout:
                    raise ClientRefusedError("Could not connect to client - port blocked or not Windows")
                elif "NT_STATUS_HOST_UNREACHABLE" in stdout:
                    raise ClientRefusedError("Could not connect to client - port blocked or not Windows")
                elif "NT_STATUS_IO_TIMEOUT" in stdout:
                    raise ClientRefusedError("Could not connect to client - not responding or not Windows")
                elif "NT_STATUS_NETWORK_UNREACHABLE" in stdout:
                    raise ClientConnectionError("Could not connect to client - network unreachable")
                elif stdout.startswith("ERROR:") and "NT_" in stdout:
                    raise ClientError("Could not connect to Windows client - {}".format(stdout))

                # shell command timed out
                if return_code == -9:
                    raise ClientRefusedError("Could not connect to Windows client - timeout")

                if return_code == 127:
                    self._debug("winexe not found")
                    raise ClientRefusedError("Could not connect to Windows client - winexe not found")

        return return_code, stdout, stderr

    def PutFile(self, localPath, remotePath):
        """
        Upload a file to the client

        Args:
            localPath:      the source path to the file locally
            remotePath:     the path to put the file on the client
        """
        if self.remoteOS == OSType.Linux:
            self.sshSession.PutFile(localPath, remotePath)

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def ExecuteCommand(self, command, ipAddress=None, throwOnError=True):
        """
        Execute a command on the client

        Args:
            command:    the command to execute
            ipAddress:  the IP address to use to connect to the client

        Returns:
            A tuple of (return code, stdout, stderr)
        """
        command = str(command)
        if not command:
            return -1, '', ''
        if not ipAddress:
            ipAddress = self.ipAddress

        self._debug("Executing {}".format(command))

        if self.isLocalhost:
            retcode, stdout, stderr = shellutil.Shell(command)
        elif self.remoteOS == OSType.Windows:
            retcode, stdout, stderr =  self._execute_winexe_command(ipAddress, command)
        else:
            retcode, stdout, stderr =  self._execute_ssh_command(ipAddress, command)

        if throwOnError and retcode != 0:
            if self.remoteOS == OSType.Windows and "diskapp" in command:
                raise ClientCommandError("Client command failed: {}".format(self._parse_diskapp_error(stdout)))
            else:
                raise ClientCommandError("Client command failed: {} {}".format(stdout, stderr))
        return retcode, stdout, stderr

    def _shell_quote(self, inputString):
        """quote a string for shell execution"""
        output = inputString
        if self.remoteOS == OSType.Windows:
            output = output.replace("\"", "\\\"\\\"\\\"\\\"")
        else:
            output = output.replace("\"", "\\\"")
        output = output.replace(";", "\\;")
        return output

    def GetIPv4Addresses(self):
        """
        Get a list of IP addresses from active NICs on the client

        Returns:
            A list of strings containing IP addresses
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("wmic nicconfig where ipenabled=true get ipaddress /format:csv")
            # This wmic call returns something like this:
            #Node,IPAddress
            #WIN-TEMPLATE,{10.10.59.152;fe80::81dc:2f02:864a:4b96}
            #WIN-TEMPLATE,{192.168.128.24;fe80::d188:3b8a:c96d:b86b}
            iplist = []
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0:
                    continue
                if not line.startswith(self.hostname):
                    continue
                pieces = line.split(',')
                pieces = pieces[1].strip('{}').split(';')
                for ip in pieces:
                    if not ":" in ip:
                        iplist.append(ip)
            return iplist

        elif self.remoteOS == OSType.Linux:
            _, stdout, _ = self.ExecuteCommand("ifconfig | grep 'inet '")
            iplist = []
            for line in stdout.split("\n"):
                m = re.search(r"inet addr:(\S+)", line)
                if m and m.group(1) != "127.0.0.1":
                    iplist.append(m.group(1))
            return iplist

        elif self.remoteOS == OSType.SunOS:
            _, stdout, _ = self.ExecuteCommand("ifconfig -a | grep 'inet '")
            iplist = []
            for line in stdout.split("\n"):
                m = re.search(r"inet (\S+)", line)
                if m and m.group(1) != "127.0.0.1":
                    iplist.append(m.group(1))
            return iplist

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetNetworkInfo(self):
        """
        Get info about the NICs and IP addresses on the client

        Returns:
            A dictionary of nic name => IP address
        """
        if self.remoteOS == OSType.Linux:
            interfaces = {}
            _, stdout, _ = self.ExecuteCommand("ifconfig | egrep 'HWaddr|inet addr'")
            for line in stdout.split("\n"):
                m = re.search(r"^(\S+)\s+", line)
                if m:
                    nic = m.group(1)
                    continue
                m = re.search(r"inet addr:(\S+)", line)
                if m and not m.group(1).startswith("127") and not nic == "lo":
                    interfaces[nic] = m.group(1)
            return interfaces

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def ChangeIPAddress(self, newIP, newNetmask, newGateway=None, interfaceName=None, interfaceMAC=None, updateHosts=False):
        """
        Change the IP address of a NIC on the client

        Args:
            newIP:          the new IP address
            newNetmask:     the new netmask
            newGateway:     the new gateway
            interfaceName:  the name of the interface to change
            interfaceMAC:   the MAC of the interface to change
            updateHosts:    whether or not to update the hosts file
        """
        if not interfaceName and not interfaceMAC:
            raise ClientError("Please specify interface name or interface MAC")
        interface_mac = interfaceMAC
        if interface_mac:
            interface_mac = interface_mac.replace(":", "").replace("-", "").lower()

        if self.remoteOS == OSType.Windows:
            interfaces = {}
            # Get a list of interfaces and MAC addresses
            _, stdout, _ = self.ExecuteCommand("getmac.exe /v /nh /fo csv")
            # "Local Area Connection 12","vmxnet3 Ethernet Adapter #18","00-50-56-A4-38-2F","\Device\Tcpip_{1058FAE5-466C-4229-A75A-91FA71ABAC8E}"
            # "Local Area Connection 13","vmxnet3 Ethernet Adapter #19","00-50-56-A4-38-2E","\Device\Tcpip_{DC53B222-EE87-4492-81A1-D1FC1845F555}"
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0:
                    continue
                pieces = line.split(",")
                name = pieces[0].strip('"')
                mac = pieces[2].strip('"').replace("-","").lower()
                interfaces[name] = dict()
                interfaces[name]["name"] = name
                interfaces[name]["mac"] = mac

            # Get a list of interfaces and indexes
            _, stdout, _ = self.ExecuteCommand("netsh interface ip show interface")
            # Idx     Met         MTU          State                Name
            # ---  ----------  ----------  ------------  -------------------------
            #   1          50  4294967295  connected     Loopback Pseudo-Interface
            #  33           5        1500  connected     Local Area Connection 12
            #  34           5        1500  connected     Local Area Connection 13
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0:
                    continue
                m = re.search(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.+)", line)
                if m:
                    index = int(m.group(1))
                    #metric = int(m.group(2))
                    #mtu = int(m.group(3))
                    #state = m.group(4)
                    name = m.group(5)
                    if "loopback" in name.lower():
                        continue
                    if name not in interfaces.keys():
                        continue
                    interfaces[name]["index"] = index

            # Get a list of interfaces and IP addresses
            _, stdout, _ = self.ExecuteCommand("netsh interface ip show addresses")
            # Configuration for interface "Local Area Connection 13"
            #     DHCP enabled:                         Yes
            #     IP Address:                           192.168.128.24
            #     Subnet Prefix:                        192.168.128.0/19 (mask 255.255.224.0)
            #     Default Gateway:                      192.168.159.254
            #     Gateway Metric:                       0
            #     InterfaceMetric:                      5
            #
            # Configuration for interface "Local Area Connection 12"
            #     DHCP enabled:                         Yes
            #     IP Address:                           10.10.59.152
            #     Subnet Prefix:                        10.10.0.0/18 (mask 255.255.192.0)
            #     InterfaceMetric:                      5
            #
            # Configuration for interface "Loopback Pseudo-Interface 1"
            #     DHCP enabled:                         No
            #     IP Address:                           127.0.0.1
            #     Subnet Prefix:                        127.0.0.0/8 (mask 255.0.0.0)
            #     InterfaceMetric:                      50
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0:
                    continue
                m = re.search(r"Configuration for interface \"(.+)\"", line)
                if m:
                    name = m.group(1)
                    continue
                m = re.search(r"\s*IP Address:\s+(.+)", line)
                if m:
                    ip = m.group(1)
                    if ip.startswith("127."):
                        continue
                    interfaces[name]["ip"] = ip
                    continue

            for name in interfaces.keys():
                output = []
                for k,v in interfaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if interfaceMAC:
                for name in interfaces.keys():
                    if interfaces[name]["mac"] == interface_mac:
                        interface_name = name
                        break
                if not interface_name:
                    raise ClientError("Could not find interface with MAC '{}'".format(interfaceMAC))
            if interfaceName:
                if interfaceName not in interfaces.keys():
                    raise ClientError("Could not find interface '" + interfaceName + "'")
                interface_name = interfaceName
            interface_index = interfaces[interface_name]["index"]
            old_ip = interfaces[interface_name]["ip"]
            self._info("Changing IP on " + interface_name + " from " + old_ip + " to " + newIP)

            command = "netsh interface ip set address name={} source=static addr={} mask={}".format(interface_index, newIP, newNetmask)
            if newGateway:
                command += " gateway={} gwmetric=0".format(newGateway)
            else:
                command += " gateway=none"
            _, stdout, _ = self.ExecuteCommand(command)

            if self.ipAddress == old_ip:
                self.ipAddress = newIP
                start_time = time.time()
                found = False
                while (not found and time.time() - start_time < 2 * 60):
                    found = self.Ping(newIP)
                if not found:
                    raise ClientError("Can't contact {} on the network - something went wrong".format(self.hostname))
            self.allIPAddresses = self.GetIPv4Addresses()
            self._passed("Sucessfully changed IP address")

        elif self.remoteOS == OSType.Linux:
            self._debug("Searching for network interfaces")
            self.ExecuteCommand("ifconfig -a")
            ifaces = {}
            for line in stdout.split("\n"):
                m = re.search(r"^(\S+)\s+.+HWaddr (\S+)", line)
                if m:
                    iface_name = m.group(1)
                    mac = m.group(2)
                    ifaces[iface_name] = dict()
                    ifaces[iface_name]["name"] = iface_name
                    ifaces[iface_name]["mac"] = mac.lower().replace(":", "")
                    continue
                m = re.search(r"inet addr:(\S+)", line)
                if m:
                    ip = m.group(1)
                    if ip == "127.0.0.1":
                        continue
                    ifaces[iface_name]["ip"] = ip

            for name in ifaces.keys():
                output = []
                for k,v in ifaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if interfaceMAC:
                for iface in ifaces.keys():
                    if ifaces[iface]["mac"] == interface_mac:
                        interface_name = iface
                if not interface_name:
                    raise ClientError("Could not find interface with MAC '{}'".format(interfaceMAC))
            if interfaceName:
                if interfaceName not in ifaces.keys():
                    raise ClientError("Could not find interface '{}'".format(interfaceName))
                interface_name = interfaceName

            if self.remoteOSVersion == "ubuntu":
                old_ip = ifaces[interface_name]["ip"]
                self._info("Changing IP on {} from {} to {}".format(interface_name, old_ip, newIP))

                # Back up old interfaces file
                self._debug("Backing up old interfaces file to interfaces.bak")
                _, stdout, _ = self.ExecuteCommand("cp /etc/network/interfaces /etc/network/interfaces.bak")

                # Change the configuration in the interfaces file
                self.PutFile(os.path.join(self.libPath, "changeinterface.awk"), "changeinterface.awk")
                self._debug("Changing IP address in /etc/network/interfaces")
                command = "awk -f changeinterface.awk /etc/network/interfaces device={} address={} netmask={}".format(interface_name, newIP, newNetmask)
                if newGateway:
                    command += " gateway={}".format(newGateway)
                command += " > interfaces"
                self.ExecuteCommand(command)
                self.ExecuteCommand("cp interfaces /etc/network/interfaces")
                self.ExecuteCommand("rm interfaces changeinterface.awk")

                # Restart networking
                self.ExecuteCommand("echo \"sleep 5\" > restart_net.sh")
                self.ExecuteCommand("echo \"/etc/init.d/networking restart\" >> restart_net.sh")
                self.ExecuteCommand("echo \"rm restart_net.sh\" >> restart_net.sh")
                self.ExecuteCommand("sync")
                self.ExecuteCommand("nohup bash restart_net.sh 2>&1 >/tmp/netrestart &")
                self._debug("Disconnecting SSH")
                self._close_command_session()
                time.sleep(sfdefaults.TIME_SECOND * 30)

                if self.ipAddress == old_ip:
                    self.ipAddress = newIP
                    start_time = time.time()
                    found = False
                    while (not found and time.time() - start_time < 2 * 60):
                        found = self.Ping(newIP)
                    if not found:
                        raise ClientError("Can't contact {} on the network - something went wrong".format(self.hostname))
                self.allIPAddresses = self.GetIPv4Addresses()
                self._passed("Sucessfully changed IP address")

            elif self.remoteOSVersion == "redhat":
                old_ip = ifaces[interface_name]["ip"]
                self._info("Changing IP on {} from {} to {}".format(interface_name, old_ip, newIP))

                interface_conf_file = "/etc/sysconfig/network-scripts/ifcfg-{}".format(interface_name)

                # Back up old config file
                self._debug("Backing up old ifcfg file")
                self.ExecuteCommand("cp {} {}.bak".format(interface_conf_file, interface_conf_file))

                # Copy the file locally to work on.  If something goes wrong, we can raise an exception and bail without leaving a partially configured file around
                self.ExecuteCommand("cp {} ifcfg".format(interface_conf_file))

                # See if the interface is already statically configured.  If so, replace the existing config
                # grep return codes: 0 means it was successful and found a match, 1 means it was successful but found no matches, anything else means an error
                retcode, stdout, stderr = self.ExecuteCommand("grep BOOTPROTO=none {}".format(interface_conf_file), throwOnError=False)
                if retcode == 0:
                    command = "sed -i -e 's/IPADDR=.*/IPADDR={}/' -e s/NETMASK=.*/NETMASK={}/".format(newIP, newNetmask)
                    if newGateway:
                        command +=  " -e s/GATEWAY=.*/GATEWAY={}/".format(newGateway)
                    command += " -e s/PREFIX=.*/d ifcfg"
                    _, stdout, _ = self.ExecuteCommand(command)
                    retcode, stdout, stderr = self.ExecuteCommand("grep NETMASK ifcfg", throwOnError=False)
                    if retcode == 1:
                        self.ExecuteCommand("echo NETMASK=" + newNetmask + " >> ifcfg")
                    elif retcode != 0:
                        raise ClientError("Could not grep interface file: " + stderr)
                elif retcode == 1:
                    # If the interface was DHCP, change it to static
                    self.ExecuteCommand("sed -i -e 's/BOOTPROTO=.*/BOOTPROTO=none/' ifcfg")
                    self.ExecuteCommand("echo IPADDR={} >> ifcfg".format(newIP))
                    self.ExecuteCommand("echo NETMASK={} >> ifcfg".format(newNetmask))
                    if newGateway:
                        self.ExecuteCommand("echo GATEWAY={} >> ifcfg".format(newGateway))
                else:
                    raise ClientError("Could not grep interface file: {}".format(stderr))

                # Move the local file over the real file
                self.ExecuteCommand("mv ifcfg {}".format(interface_conf_file))

                # Restart networking
                self.ExecuteCommand("echo \"sleep 5\" > restart_net.sh")
                self.ExecuteCommand("echo \"/etc/init.d/network restart\" >> restart_net.sh")
                self.ExecuteCommand("echo \"rm -f restart_net.sh\" >> restart_net.sh")
                self.ExecuteCommand("sync")
                self.ExecuteCommand("nohup bash restart_net.sh 2>&1 >/tmp/netrestart &")
                self._debug("Disconnecting SSH")
                self._close_command_session()
                time.sleep(sfdefaults.TIME_SECOND * 30)

                if self.ipAddress == old_ip:
                    self.ipAddress = newIP
                    start_time = time.time()
                    found = False
                    while (not found and time.time() - start_time < 2 * 60):
                        found = self.Ping(newIP)
                    if not found:
                        raise ClientError("Can't contact {} on the network - something went wrong".format(self.hostname))
                self.allIPAddresses = self.GetIPv4Addresses()
                self._passed("Sucessfully changed IP address")

            else:
                raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOSVersion))


            if updateHosts:
                self.ExecuteCommand("echo \"127.0.0.1           localhost\" > /etc/hosts")
                self.ExecuteCommand("echo \"{}       {}\" >> /etc/hosts".format(newIP, self.hostname))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def UpdateHostname(self, newHostname):
        """
        Change the hostname on the client

        Args:
            newHostname:    the new hostname
        """
        self._info("Checking hostname")
        if newHostname.lower() == self.hostname.lower():
            self._debug("The hostname is already updated")
            return False

        if self.remoteOS == OSType.Windows:
            self._info("Setting hostname to {}".format(newHostname))
            self.ExecuteCommand("cmd.exe /c wmic computersystem where name='%COMPUTERNAME%' call rename name='{}'".format(newHostname))

        elif self.remoteOS == OSType.SunOS:
            self._info("Setting hostname to " + newHostname)
            oldhostname = self.hostname
            self.ExecuteCommand("svccfg -s node setprop config/nodename = \"{}\"".format(newHostname))
            self.ExecuteCommand("svccfg -s node setprop config/loopback = \"{}\"".format(newHostname))
            self.ExecuteCommand("svccfg -s system/identity:node refresh")
            self.ExecuteCommand("svcadm restart svc:/system/identity:node")

            # make sure hosts file is correct
            self.ExecuteCommand("echo -e \"::1 {} localhost\\n127.0.0.1 {} localhost loghost\" > /etc/inet/hosts".format(newHostname, newHostname))

        elif self.remoteOS == OSType.Linux:
            self._info("Setting hostname to {}".format(newHostname))
            oldhostname = self.hostname
            self.ExecuteCommand("hostname {}".format(newHostname))
            if self.remoteOSVersion == "redhat":
                # Change /etc/sysconfig/network
                self.ExecuteCommand("chattr -i /etc/sysconfig/network")
                self.ExecuteCommand("sed 's/HOSTNAME=.*/HOSTNAME={}/' /etc/sysconfig/network".format(newHostname))

                # Update current hostname
                self.ExecuteCommand("hostname -v {}".format(newHostname))
            else:
                # Change /etc/hostname
                self.ExecuteCommand("chattr -i /etc/hostname")
                self.ExecuteCommand("echo {} > /etc/hostname".format(newHostname))
                # Update current hostname
                self.ExecuteCommand("hostname -v -b {}".format(newHostname))

            # Change /etc/hosts
            self.ExecuteCommand("chattr -i /etc/hosts")
            self._debug("Updating /etc/hosts")
            self.ExecuteCommand("sed -i 's/{}/{}/g' /etc/hosts".format(oldhostname, newHostname))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

        self.hostname = newHostname
        return True

    def GetInitiatorIDs(self, connectionType='iscsi'):
        """
        Get the list of initiator IDs for the specified connection type (IQN or WWNs)

        Args:
            connectionType:     the type of volume connection

        Returns:
            A list of the appropriate identifiers (list of str)
        """
        if connectionType == 'iscsi':
            return [self.GetInitiatorName()]
        elif connectionType == 'fc':
            return self.GetWWNs()
        else:
            raise ClientError("Unknown connection type '{}'".format(connectionType))

    def GetInitiatorName(self):
        """
        Get the iSCSI initiator name of the client

        Returns:
            A string initiator name
        """
        if self.remoteOS == OSType.Linux:
            _, stdout, _ = self.ExecuteCommand("( [[ -e /etc/open-iscsi/initiatorname.iscsi ]] && cat /etc/open-iscsi/initiatorname.iscsi || cat /etc/iscsi/initiatorname.iscsi ) | grep -v '#' | cut -d'=' -f2")
            iqn = stdout.strip()
            if not iqn:
                raise ClientError("Empty IQN")
            return iqn

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetWWNs(self):
        """
        Get the Fibre Channel WWNs of the client

        Returns:
            A list of string WWNs
        """
        if self.remoteOS == OSType.Linux:
            _, stdout, _ = self.ExecuteCommand("cat /sys/class/fc_host/*/port_name")
            wwns = []
            for line in stdout.strip().split("\n"):
                #line = line.strip()
                #if not line:
                #    continue
                wwns.append(line[line.index("x")+1:].lower())
            return wwns

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def UpdateInitiatorName(self):
        """
        Set the iSCSI initator name on the client. The new initiator name will be created from the client hostname
        """
        self._info("Checking iSCSI Initiator name")
        if self.remoteOS == OSType.Windows:
            # iqn.1991-05.com.microsoft:hostname
            self._debug("Reading current initiator name")
            _, stdout, _ = self.ExecuteCommand("diskapp.exe --show_initiatorname")
            initiator_name = "iqn." + self.hostname
            for line in stdout.split("\n"):
                m = re.search(r"INFO\s+(iqn.+)", line)
                if m:
                    pieces = m.group(1).split(":")
                    oldname = pieces.pop()
                    if (oldname == self.hostname):
                        self._debug("Initiator name is already correct")
                        return False
                    initiator_name = ":".join(pieces) + ":" + self.hostname
                    break
            self._info("Setting initiator name to '{}'".format(initiator_name))
            self.ExecuteCommand("diskapp.exe --set_initiatorname --name=" + initiator_name)
            return True

        elif self.remoteOS == OSType.Linux:
            # iqn.1993-08.org.debian:01:hostname - from Ubuntu 10.04
            # iqn.1994-05.com.redhat:hostname - from RHEL 6.3
            self._debug("Reading current initiator name")
            initiator_name = self.GetInitiatorName()
            if initiator_name:
                pieces = initiator_name.split(":")
                oldname = pieces.pop()
                if oldname == self.hostname:
                    self._debug("Initiator name is already correct")
                    return False
                initiator_name = ":".join(pieces) + ":" + self.hostname
            else:
                if self.remoteOSVersion == "ubuntu":
                    initiator_name = "iqn.1993-08.org.debian:01:{}".format(self.hostname)
                else:
                    initiator_name = "iqn.1994-05.com.redhat:{}".format(self.hostname)
            self._info("Setting initiator name to '{}'".format(initiator_name))
            self.ExecuteCommand("echo InitiatorName={} > /etc/iscsi/initiatorname.iscsi".format(initiator_name))
            return True

        elif self.remoteOS == OSType.SunOS:
            # iqn.1986-03.com.sun:01:hostname
            self._debug("Reading current initiator name")
            _, stdout, _ = self.ExecuteCommand("iscsiadm list initiator-node")
            initiator_name = "iqn.1986-03.com.sun:01:" + self.hostname
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r"Initiator node name: (\S+)", line)
                if m:
                    pieces = m.group(1).split(":")
                    oldname = pieces.pop()
                    if oldname == self.hostname:
                        self._debug("Initiator name is already correct")
                        return False
                    initiator_name = ":".join(pieces) + ":" + self.hostname
                    break
            self._info("Setting initiator name to '{}'".format(initiator_name))
            self.ExecuteCommand("iscsiadm modify initiator-node --node-name={}".format(initiator_name))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def Ping(self, ipAddress=None):
        """
        Ping the client

        Args:
            ipAddress:  the IP address on the client to ping

        Returns:
            True if the client is ping-able, False otherwise
        """
        if ipAddress == None:
            ipAddress = self.ipAddress

        return netutil.Ping(ipAddress)

    def EnableInterfaces(self, fromIPAddress):
        """
        Enable all of the NICs on the client

        Args:
            fromIPAddress:  the IP address to use to connect to the client
        """
        if self.remoteOS == OSType.Linux:
            # Get a list of all interfaces
            _, stdout, _ = self.ExecuteCommand("ifconfig -a | grep eth", fromIPAddress)
            all_ifaces = []
            for line in stdout.split("\n"):
                m = re.search(r"^(eth\d+)\s+", line)
                if (m):
                    iface = m.group(1)
                    all_ifaces.append(iface)
            # Get a list of 'up' interfaces
            _, stdout, _ = self.ExecuteCommand("ifconfig | grep eth", fromIPAddress)
            up_ifaces = []
            for line in stdout.split("\n"):
                m = re.search(r"^(eth\d+)\s+", line)
                if (m):
                    iface = m.group(1)
                    up_ifaces.append(iface)
            # Bring up the interfaces that aren't already
            for iface in all_ifaces:
                if iface not in up_ifaces:
                    self._info("Enabling " + iface)
                    self.ExecuteCommand("ifconfig {} up".format(iface), fromIPAddress)

        else:
            self._warn("{} client - enable network interfaces not implemented".format(self.remoteOS))

    def RebootSoft(self):
        """
        Reboot the client
        """
        self._info("Sending reboot command")

        if self.remoteOS == OSType.Windows:
            self.ExecuteCommand("sc config winexesvc start= auto") # make sure winexe service will restart after boot
            self.ExecuteCommand("shutdown /r /f /t 10") # wait 10 sec so winexe can disconnect cleanly
            self._close_command_session()

        elif self.remoteOS == OSType.SunOS:
            self.ExecuteCommand("reboot")
            self._close_command_session()

        else:
            self.ExecuteCommand("shutdown -r now")
            self._close_command_session()

        self._info("Waiting to go down")
        while self.Ping():
            pass

    def WaitTillUp(self):
        """
        Wait for the client to be up and usable
        """
        self._info("Waiting to come up")
        start = time.time()
        responding_ip = self.ipAddress
        # Wait until the client is responding to ping
        while not self.Ping():
            if time.time() - start > 4 * 60:
                # if the client hasn't come back yet, try another IP address
                response = False
                for ip in self.allIPAddresses:
                    responding_ip = ip
                    response = self.Ping(ip)
                if response:
                    break
            time.sleep(sfdefaults.TIME_SECOND * 5)
        # Wait until the client is responding to management requests
        while True:
            try:
                self.ExecuteCommand("hostname", responding_ip)
                break
            except ClientError:
                time.sleep(sfdefaults.TIME_SECOND * 5)

        # Make sure all interfaces came back up on Linux
        if self.remoteOS == OSType.Linux:
            self.EnableInterfaces(responding_ip)
        self._info("Up and responding")

    def CleanIscsi(self, defaultConfigFile=True):
        """
        Clean the iSCSI initiator on the client

        Args:
            defaultConfigFile:  restore the default iscsid config file
        """
        if self.remoteOS == OSType.Windows:
            self.ExecuteCommand("diskapp.exe --clean")
            self._passed("Cleaned iSCSI")

        elif self.remoteOS == OSType.Linux:
            self._debug("Logging out of all targets")
            self.ExecuteCommand("iscsiadm -m node -U all", throwOnError=False)
            self.ExecuteCommand("iscsiadm -m session -o delete", throwOnError=False)
            self.ExecuteCommand("systemctl stop iscsid")
            time.sleep(sfdefaults.TIME_SECOND * 3)
            self.ExecuteCommand("killall -9 iscsid", throwOnError=False)

            if defaultConfigFile:
                self._debug("Restoring default iscsid.conf")
                self.PutFile(os.path.join(self.libPath, 'iscsid.conf.default.{}'.format(self.remoteOSVersion)), '/etc/iscsi/iscsid.conf')

            self._debug("Removing persistent configuration")
            self.ExecuteCommand("rm -rf /etc/iscsi/ifaces /etc/iscsi/nodes /etc/iscsi/send_targets")
            self.ExecuteCommand("rm -rf /var/lib/iscsi")
            self.ExecuteCommand("touch /etc/iscsi/iscsi.initramfs")
            self.ExecuteCommand("systemctl start iscsid")
            time.sleep(sfdefaults.TIME_SECOND * 5)
            self._passed("Cleaned iSCSI")

        elif self.remoteOS == OSType.SunOS:
            self.ExecuteCommand("iscsiadm modify discovery --sendtargets disable")
            self.ExecuteCommand("iscsiadm modify initiator-node --authentication=NONE")
            _, stdout, _ = self.ExecuteCommand("iscsiadm list discovery-address")
            discovery_ips = []
            for line in stdout.split("\n"):
                m = re.search(r"Discovery Address: (\S+):", line)
                if m:
                    discovery_ips.append(m.group(1))
            for ip in discovery_ips:
                self.ExecuteCommand("iscsiadm remove discovery-address " + ip)
            self.ExecuteCommand("devfsadm -C")
            self._passed("Cleaned iSCSI")

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def SetupCHAP(self, portalAddress, chapUser, chapSecret):
        """
        Setup iSCSI CHAP credentials on the client

        Args:
            portalAddress:  the discovery portal IP address
            chapUser:       the CHAP username
            chapSecret:     the CHAP secret
        """
        self.chapCredentials[portalAddress] = [chapUser, chapSecret]
        self._info("Setting up CHAP credentials for portal " + portalAddress)

        if self.remoteOS == OSType.Windows:
            # Make sure the CHAP secret is only alphameric
            # This restriction will exist until I can reliably figure out the multiple levels of shell quoting required
            if not chapSecret.isalnum():
                raise ClientError("Sorry, CHAP secret must be alphanumeric")
            self._debug("Adding portal {} to initiator".format(portalAddress))
            cmd = "diskapp.exe --add_portal --portal_address={} --chap_user={} --chap_secret=\"{}\"".format(portalAddress, chapUser, self._shell_quote(chapSecret))
            self.ExecuteCommand(cmd)
            self._passed("Added portal and CHAP credentials")

        elif self.remoteOS == OSType.Linux:
            self._debug("Updating iscsid.conf")

            # Turn on CHAP
            cmd = r"sed 's/#*\s*node\.session\.auth\.authmethod\s*=.*/node\.session\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/#*\s*discovery\.sendtargets\.auth\.authmethod\s*=.*/discovery\.sendtargets\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)

            # Set username/password for one-way CHAP
            cmd = r"sed 's/#*\s*discovery\.sendtargets\.auth\.username\s*=.*/discovery\.sendtargets\.auth\.username = {}/g' -i /etc/iscsi/iscsid.conf".format(chapUser)
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/#*\s*discovery\.sendtargets\.auth\.password\s*=.*/discovery\.sendtargets\.auth\.password = {}/g' -i /etc/iscsi/iscsid.conf".format(chapSecret)
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/#*\s*node\.session\.auth\.username\s*=.*/node\.session\.auth\.username = {}/g' -i /etc/iscsi/iscsid.conf".format(chapUser)
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/#*\s*node\.session\.auth\.password\s*=.*/node\.session\.auth\.password = {}/g' -i /etc/iscsi/iscsid.conf".format(chapSecret)
            self.ExecuteCommand(cmd)

            # Disable 2-way CHAP
            cmd = r"sed 's/^#*\s*discovery\.sendtargets\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/^#*\s*discovery\.sendtargets\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/^#*\s*node\.session\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)
            cmd = r"sed 's/^#*\s*node\.session\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf"
            self.ExecuteCommand(cmd)

            self._passed("Set CHAP credentials in iscsid.conf")

        elif self.remoteOS == OSType.SunOS:
            self._debug("Setting CHAP credentials")
            self.ExecuteCommand("iscsiadm modify initiator-node --CHAP-name={}".format(chapUser))

            # Use an expect script to set CHAP password because the command won't accept redirection
            self.PutFile(os.path.join(self.libPath, "solaris-chapsecret.exp"), "solaris-chapsecret.exp")
            self.ExecuteCommand("expect solaris-chapsecret.exp \"{}\"".format(chapSecret))

            self.ExecuteCommand("iscsiadm modify initiator-node --authentication=CHAP")

            # Don't bother enabling until we add a portal
            #self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            self._passed("Successfully setup CHAP")

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def RefreshTargets(self, portalAddress, expectedTargetCount=0, ifaceName=None):
        """
        Refresh the list of discovered iSCSI targets

        Args:
            portalAddress:          the discovery portal IP address
            expectedTargetCount:    the expected number of targets to discover
            ifaceName:              the name of the iSCSI iface to use
        """
        if self.remoteOS == OSType.Windows:
            if not self.chapCredentials.has_key(portalAddress):
                raise ClientError("Please setup CHAP for this portal before trying to discover or login")
            self.ExecuteCommand("diskapp.exe --refresh_targets")
            if expectedTargetCount <= 0:
                self._passed("Refreshed all portals")
                return
            targets = self.GetAllTargets()
            if len(targets) < expectedTargetCount:
                raise ClientError("Expected {} targets but discovered {}".format(expectedTargetCount, len(targets)))
            self._passed("Refreshed all portals")

        elif self.remoteOS == OSType.Linux:
            if ifaceName:
                cmd = "iscsiadm -m discovery -t sendtargets -p {} -I {}".format(portalAddress, ifaceName)
                self._debug("Refreshing target list on {} via iface {}".format(portalAddress, ifaceName))
            else:
                cmd = "iscsiadm -m discovery -t sendtargets -p {}".format(portalAddress)
                self._debug("Refreshing target list on {}".format(portalAddress))
            self.ExecuteCommand(cmd)
            targets = self.GetAllTargets()
            if len(targets) < expectedTargetCount:
                raise ClientError("Expected {} targets but discovered {}".format(expectedTargetCount, len(targets)))
            if len(targets) <= 0:
                self._warn("There were no iSCSI targets discovered")
            self._passed("Refreshed portal {}".format(portalAddress))

        elif self.remoteOS == OSType.SunOS:
            # Refresh targets and LoginTargets do the same thing for Solaris
            self._debug("Refreshing target list on {}".format(portalAddress))
            self.ExecuteCommand("iscsiadm add discovery-address {}".format(portalAddress))
            self.ExecuteCommand("iscsiadm modify discovery --sendtargets=disable")
            self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            self.ExecuteCommand("devfsadm -C")
            self._passed("Refreshed all portals")

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def _parse_diskapp_error(self, stdout):
        """parse out the error message from diskapp"""
        for line in stdout.split("\n"):
            m = re.search(r": ERROR\s+(.+)", line)
            if m:
                return m.group(1)
        # If we couldn't find a recognizable error, just return the last line
        return stdout.split("\n")[1]

    def LoginTargets(self, portalAddress=None, loginOrder="serial", targetList=None):
        """
        Login to discovered iSCSI targets

        Args:
            portalAddress:  the IP address
            loginOrder:     order to login to targets (serial, parallel)
            targetList:     the list of targets to login to
        """
        if self.remoteOS == OSType.Windows:
            if targetList != None and len(targetList) > 0:
                raise ClientError("target_list is not implemented for Windows")
            if not self.chapCredentials.has_key(portalAddress):
                raise ClientError("Please setup CHAP for this portal before trying to discover or login")
            chap_user = self.chapCredentials[portalAddress][0]
            chap_secret = self.chapCredentials[portalAddress][1]
            self._info("Logging in to all targets")
            cmd = "diskapp.exe --login_targets --portal_address={} --chap_user=\"{}\" --chap_secret=\"{}\"".format(portalAddress, self._shell_quote(chap_user), self._shell_quote(chap_secret))
            retcode, stdout, _ = self._execute_winexe_command(self.ipAddress, cmd, 300)
            if retcode == 0:
                self._passed("Logged in to all volumes")
            else:
                raise ClientCommandError(self._parse_diskapp_error(stdout))

        elif self.remoteOS == OSType.Linux:
            login_count = 0
            error_count = 0
            if loginOrder == "parallel":
                if targetList != None and len(targetList) > 0:
                    raise ClientError("Parallel login with a target_list is not currently implemented")
                self._info("Logging in to all targets in parallel")
                _, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -L all", throwOnError=False)
                for line in stdout.split("\n"):
                    m = re.search(r"^Logging in to", line)
                    if (m):
                        continue
                    m = re.search(r"^Login to .+ target: (.+), portal.+]: (.+)", line)
                    if (m):
                        iqn = m.group(1)
                        status = m.group(2)
                        if (status != "successful"):
                            self._error("Failed to log in to '{}'".format(iqn))
                        else:
                            login_count += 1
                if (login_count <= 0):
                    iqn = ""
                    for line in stderr.split("\n"):
                        line = line.strip()
                        m = re.search(r"Could not login to.+target: (.+), portal", line)
                        if (m):
                            iqn = m.group(1)
                        m = re.search("already exists", line)
                        if (m):
                            self._warn("Session already exists for '{}'".format(iqn))
                            login_count += 1
                            continue
                        m = re.search("reported error", line)
                        if (m):
                            self._error("Failed to log in to '{}' -- {}".format(iqn, line))
                            error_count += 1
            elif loginOrder == "serial":
                self._info("Logging in to targets serially")
                targets = self.GetAllTargets()
                if not targets:
                    self._warn("There are no targets to log in to")
                    return
                if targetList != None and len(targetList) > 0:
                    targets = targetList
                self._debug("Found {} targets to log in to".format(len(targets)))
                login_count = 0
                error_count = 0
                for target in targets:
                    self._info("Logging in to {}".format(target))
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -l -T {}".format(target), throwOnError=False)
                    if retcode != 0:
                        if "already exists" in stderr:
                            self._warn("Session already exists for {}".format(target))
                        else:
                            self._error("Failed to log in to {}".format(target))
                            error_count += 1
                    else:
                        login_count += 1

            # Set up automatic login
            all_targets = self.GetAllTargets()
            for target in all_targets:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -o update -n node.startup -v automatic -T {}".format(target), throwOnError=False)
                if retcode != 0:
                    self._error("Failed to set automatic login on {}".format(target))
                    error_count += 1

            # Wait for SCSI devices for all sessions
            if login_count > 0:
                start_time = time.time()
                while True:
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P3 | egrep 'Target:|scsi disk' | wc -l", throwOnError=False)
                    stdout = stdout.strip()
                    # This should generate two lines of output for every target
                    # Target: iqn.2010-01.com.solidfire:8m8z.kvm-templates.38537
                    #    Attached scsi disk sdc          State: running
                    # Instead of parsing the output, we'll assume that an even number of lines means there is a device for every session
                    if int(stdout) % 2 == 0:
                        break
                    if time.time() - start_time > 120: # Wait up to 2 minutes
                        raise ClientError("Timeout waiting for all iSCSI sessions to have SCSI devices")
                    time.sleep(sfdefaults.TIME_SECOND)

            if (login_count > 0):
                self._passed("Successfully logged in to {} volumes".format(login_count))
            if (error_count > 0):
                self._error("Failed to login to {} volumes".format(error_count))
                raise ClientError("Failed to log in to all volumes")

        elif self.remoteOS == OSType.SunOS:
            if targetList != None and len(targetList) > 0:
                raise ClientError("target_list is not implemented for SunOS")
            # Refresh targets and LoginTargets do the same thing for Solaris
            self._debug("Logging in to targets")
            self.ExecuteCommand("iscsiadm add discovery-address {}".format(portalAddress))
            self.ExecuteCommand("iscsiadm modify discovery --sendtargets=disable")
            self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            self.ExecuteCommand("devfsadm -C")
            self._passed("Logged in to all volumes")

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def CreateIscsiIface(self, ifaceName, ipAddress=None, nicName=None, initiatorName=None):
        """
        Create an iSCSI iface to use to connect to volumes

        Args:
            ifaceName:      the name of the iface to create
            ipAddress:      the IP address of the NIC to use
            nicName:        the name of the NIC to use
            initiatorName:  the iSCSI initator name to use
        """
        if self.remoteOS == OSType.Linux:
            self.ExecuteCommand("iscsiadm -m iface -I {0} -o new".format(ifaceName))

            if ipAddress:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m iface -I {} -o update -n iface.ipaddress -v {}".format(ifaceName, ipAddress), throwOnError=False)
                if retcode != 19 and retcode != 0:
                    raise ClientError("Could not update iface {} IP address: [{}] {} {}".format(ifaceName, retcode, stderr, stdout))

            if nicName:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m iface -I {} -o update -n iface.net_ifacename -v {}".format(ifaceName, nicName), throwOnError=False)
                if retcode != 19 and retcode != 0:
                    raise ClientError("Could not update iface {} net interface name: [{}] {} {}".format(ifaceName, retcode, stderr, stdout))

            if initiatorName:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m iface -I {} -o update -n iface.initiatorname -v {}".format(ifaceName, initiatorName), throwOnError=False)
                if retcode != 19 and retcode != 0:
                    raise ClientError("Could not update iface {} initiator name: [{}] {} {}".format(ifaceName, retcode, stderr, stdout))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def LogoutTargets(self, targetList=None):
        """
        Logout of iSCSI targets

        Args:
            targetList: the list of targets to log out of
        """
        self._info("Logging out of all iSCSI volumes")

        if self.remoteOS == OSType.Windows:
            if targetList != None and len(targetList) > 0:
                raise ClientError("target_list is not implemented for Windows")
            retcode, stdout, _ = self.ExecuteCommand("diskapp.exe --logout_targets --force_unmount --persistent")
            if retcode == 0:
                self._passed("Logged out of all volumes")
            else:
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.remoteOS == OSType.Linux:
            # Log out of a list of targets
            if targetList != None and len(targetList) > 0:
                error = False
                for target in targetList:
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -u -T", throwOnError=False)
                    if retcode != 0 and retcode != 21:
                        self._error("Failed to log out of {}: {}".format(target, stderr))
                        error = True
                if error:
                    raise ClientError("Failed to log out of all targets")
                self._passed("Logged out of requested volumes")
                return

            # Log out of all volumes
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -U all", throwOnError=False)
            if retcode == 0 or retcode == 21: # 21 means there were no sessions to log out of
                self._passed("Logged out of all volumes")
                return
            else:
                logout_count = 0
                for line in stdout.split("\n"):
                    m = re.search(r"^Logging out of", line)
                    if (m):
                        continue
                    m = re.search(r"^Logout of .+ target: (.+), portal.+]: (.+)", line)
                    if (m):
                        iqn = m.group(1)
                        status = m.group(2)
                        if (status != "successful"):
                            self._error("Failed to log out of '{}'".format(iqn))
                        else:
                            logout_count += 1
                if logout_count <= 0:
                    for line in stderr.split("\n"):
                        line = line.strip()
                        iqn = ""
                        m = re.search(r"Could not logout to.+target: (.+), portal", line)
                        if (m):
                            iqn = m.group(1)
                        m = re.search(r"reported error", line)
                        if (m):
                            self._error("Failed to log out of '{}' -- {}".format(iqn, line))
                raise ClientError("Failed to log out of all volumes")

        elif self.remoteOS == OSType.SunOS:
            if targetList != None and len(targetList) > 0:
                raise ClientError("target_list is not implemented for SunOS")
            _, stdout, _ = self.ExecuteCommand("iscsiadm list discovery-address")
            discovery_ips = []
            for line in stdout.split("\n"):
                m = re.search(r"Discovery Address: (\S+):", line)
                if m:
                    discovery_ips.append(m.group(1))
            for ip in discovery_ips:
                self.ExecuteCommand("iscsiadm remove discovery-address {}".format(ip))
            self.ExecuteCommand("devfsadm -C")
            self._passed("Logged out of all volumes")

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetAllTargets(self):
        """
        Get a list of all discovered iSCSI targets

        Returns:
            A list of string IQNs
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("diskapp --list_targets")
            targets = []
            for line in stdout.split("\n"):
                m = re.search(r"(iqn\.\S+)", line)
                if m:
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.remoteOS == OSType.Linux:
            _, stdout, _ = self.ExecuteCommand("iscsiadm -m node -P 1 | grep 'Target:'")
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                m = re.search(r"Target:\s+(.+)", line)
                if (m):
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.remoteOS == OSType.SunOS:
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target")
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r"Target:\s+(.+)", line)
                if (m):
                    targets.append(m.group(1))
            targets.sort()
            return targets

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetLoggedInTargets(self):
        """
        Get a list of all logged in iSCSI targets

        Returns:
            A list of string IQNs
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("diskapp.exe --list_targets")
            targets = []
            for line in stdout.split("\n"):
                m = re.search(r"(iqn\..+)\s+\(LOGGED IN\)", line)
                if m:
                    #self._debug(m.group(1))
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.remoteOS == OSType.Linux:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P 0", throwOnError=False)
            if retcode != 0 and retcode != 21:
                raise ClientCommandError("Client command failed: {} {}".format(stdout, stderr))
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                m = re.search(r"(iqn\.\S+)", line)
                if m:
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.remoteOS == OSType.SunOS:
            return self.GetAllTargets()

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetVdbenchDevices(self):
        """
        Get a list of SCSI devices that can be used as vdbench targets

        Returns:
            A list of string vdbench devices
        """
        if self.remoteOS == OSType.Windows:
           # Give this a longer than default timeout because it can take a while when there a large number of disks
            _, stdout, _ = self._execute_winexe_command(self.ipAddress, "diskapp.exe --list_disks", 180)
            devices = []
            for line in stdout.split("\n"):
                m = re.search(r"INFO\s+(\S+) => (\S+),", line)
                if m:
                    devices.append(m.group(2))
            return sorted(devices)

        elif self.remoteOS == OSType.Linux:
            # Find the root device
            retcode, stdout, _ = self.ExecuteCommand(r"cat /proc/mounts | egrep '\s/\s' | cut -d' ' -f1 | egrep -o '[a-z/]+'")
            if retcode != 0:
                raise ClientError("Could not determine root drive")
            root_drive = stdout.strip()

            # First look for multipath devices (FC or multipath iSCSI)
            retcode, stdout, _ = self.ExecuteCommand("multipath -l | grep SolidFir | awk '{print $3}' | sort", throwOnError=False)
            if retcode == 0:
                dev_list = []
                for line in stdout.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    dev_list.append("/dev/" + line)
                if len(dev_list) > 0:
                    return sorted(dev_list, key=lambda x: int(re.findall(r'\d+$', x)[0]))

            # Look for regular iSCSI volumes
            _, stdout, _ = self.ExecuteCommand("iscsiadm -m session -P 3 | egrep 'Target:|State:|disk'")
            new_volume = None
            volumes = {}
            for line in stdout.split("\n"):
                m = re.search(r"Target:\s+(\S+)", line)
                if(m):
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search(r"iSCSI Session State:\s+(.+)", line)
                if(m):
                    new_volume["state"] = m.group(1)
                m = re.search(r"disk\s+(\S+)\s", line)
                if(m):
                    new_volume["device"] = "/dev/" + m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            devices = []
            devs_by_length = dict()
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                if volume["state"] != "LOGGED_IN":
                    self._warn("Skipping {} because session state is {}".format(volume["iqn"], volume["state"]))
                    continue
                length = str(len(volume["device"]))
                if length not in devs_by_length:
                    devs_by_length[length] = []
                devs_by_length[length].append(volume["device"])
                devices.append(volume["device"])
            sorted_devs = []
            for length in sorted(devs_by_length.keys(), key=int):
                devs_by_length[length].sort()
                sorted_devs += devs_by_length[length]

            # Make sure the root device is not in the list
            try:
                sorted_devs.remove(root_drive)
            except ValueError:
                pass
            return sorted_devs

        elif self.remoteOS == OSType.SunOS:
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target -S")
            devices = []
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r"OS Device Name:\s+(.+)", line)
                if (m):
                    devices.append(m.group(1))
            devices.sort()
            return devices

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetVolumeSummary(self):
        """
        Get a list of connected volumes and some info about them

        Returns:
            A dictionary of string device name => device info
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("diskapp.exe --list_disks")
            line_list = []
            for line in stdout.split("\n"):
                m = re.search(r"INFO\s+(.+)", line)
                if m:
                    line_list.append(m.group(1))
            self._info("Found {} iSCSI volumes".format(len(line_list)))
            for line in line_list:
                self._info("    " + line)

        elif self.remoteOS == OSType.Linux:
            # Sector size for all attached scsi block devices
            _, raw_devices, _ = self.ExecuteCommand("for dev in `ls -d /sys/block/sd*`; do echo \"$dev=`cat $dev/queue/hw_sector_size`\"; done")
            sectors = dict()
            for line in raw_devices.split("\n"):
                if not line.strip():
                    continue
                pieces = line.split('=')
                dev = pieces[0][11:] # remove /sys/block/ off the front
                size = pieces[1]
                sectors[dev] = size

            retcode, raw_iscsiadm, stderr = self.ExecuteCommand("iscsiadm -m session -P 3 | egrep 'Target:|Portal:|State:|SID:|disk'", throwOnError=False)
            if not (retcode == 0 or retcode == 21 or (retcode == 1 and "No active sessions" in stderr)):
                raise ClientCommandError("iscsiadm command failed: {} {}".format(raw_iscsiadm, stderr))
            new_volume = None
            volumes = dict()
            for line in raw_iscsiadm.split("\n"):
                m = re.search(r"Target:\s+(\S+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                    for key in ["portal", "sid", "state", "device", "sectors"]:
                        new_volume[key] = "unknown"
                m = re.search(r"Current Portal:\s+(.+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                m = re.search(r"SID:\s+(.+)", line)
                if m:
                    new_volume["sid"] = m.group(1)
                m = re.search(r"iSCSI Session State:\s+(.+)", line)
                if m:
                    new_volume["state"] = m.group(1)
                m = re.search(r"disk\s+(\S+)\s", line)
                if m:
                    new_volume["device"] = "/dev/" + m.group(1)
                    #_, stdout, _ = self.ExecuteCommand("cat /sys/block/" + m.group(1) + "/queue/hw_sector_size")
                    #new_volume["sectors"] = stdout.strip()
                    if m.group(1) in sectors.keys():
                        new_volume["sectors"] = sectors[m.group(1)]
                    else:
                        new_volume["sectors"] = 0
                    volumes[new_volume["device"]] = new_volume
            return volumes

        elif self.remoteOS == OSType.SunOS:
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target -v")
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r"Target:\s+(.+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search(r"IP address \(Peer\):\s+(\S+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target -S")
            current_target = None
            for line in stdout.split("\n"):
                m = re.search(r"Target:\s+(.+)", line)
                if m:
                    current_target = m.group(1)
                m = re.search(r"OS Device Name:\s+(.+)", line)
                if m:
                    volumes[current_target]["device"] = m.group(1)
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                self._info("    {} -> {}, Portal: {}".format(volume["iqn"], volume["device"], volume["portal"]))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def ListVolumes(self):
        """
        Print a list of volumes and some info about them
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("diskapp.exe --list_disks")
            line_list = []
            for line in stdout.split("\n"):
                m = re.search(r"INFO\s+(.+)", line)
                if m:
                    line_list.append(m.group(1))
            self._info("Found {} iSCSI volumes".format(len(line_list)))
            for line in line_list:
                self._info("    " + line)

        elif self.remoteOS == OSType.Linux:
            volumes = self.GetVolumeSummary()
            sort = "iqn" # or device, portal, state
            self._info("Found {} iSCSI volumes".format(len(volumes.keys())))
            for _, volume in sorted(volumes.iteritems(), key=lambda (k,v): v[sort]):
                outstr = "    {} -> {}, SID: {}, SectorSize: {}, Portal: {}".format(volume["iqn"], volume["device"], volume["sid"], volume["sectors"], volume["portal"])
                if "state" in volume:
                    outstr += ", Session: {}".format(volume["state"])
                self._info(outstr)

        elif self.remoteOS == OSType.SunOS:
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target -v")
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r"Target:\s+(.+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search(r"IP address \(Peer\):\s+(\S+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            _, stdout, _ = self.ExecuteCommand("iscsiadm list target -S")
            current_target = None
            for line in stdout.split("\n"):
                m = re.search(r"Target:\s+(.+)", line)
                if m:
                    current_target = m.group(1)
                m = re.search(r"OS Device Name:\s+(.+)", line)
                if m:
                    volumes[current_target]["device"] = m.group(1)
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                self._info("    {} -> {}, Portal: {}".format(volume["iqn"], volume["device"], volume["portal"]))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def SetupVolumes(self):
        """
        Mount, partition and format connected volumes
        """
        if self.remoteOS == OSType.Windows:
            retcode, stdout, _ = self._execute_winexe_command(self.ipAddress, "diskapp.exe --setup_disks --force_mountpoints --relabel", timeout=300)
            if retcode == 0:
                self._passed("Setup all disks")
            else:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.remoteOS == OSType.Linux:
            volumes = self.GetVolumeSummary()
            for vol_info in volumes.values():
                vname = vol_info["iqn"].split(".")[-2]
                self._info("Mounting {}".format(vname))
                self.ExecuteCommand("mkdir -p /mnt/{}".format(vname))
                self.ExecuteCommand(r"""parted {} --script mklabel msdos \
                    mkpart primary 2048s 100%""".format(vol_info["device"]))
                self.ExecuteCommand("mkfs.ext4 -F -E nodiscard -L {} {}1".format(vname, vol_info["device"]))
                self.ExecuteCommand("mount {}1 /mnt/{}".format(vol_info["device"], vname))

        else:
            raise ClientError("Sorry, not implemented yet for " + self.remoteOS)

    def KernelPanic(self):
        """
        Cause a kernel panic on the client
        """
        if self.remoteOS == OSType.Linux:
            self.ExecuteCommand("echo \"Kernel panicking in 5 seconds\" > panic.sh")
            self.ExecuteCommand("echo \"sleep 5\" >> panic.sh")
            self.ExecuteCommand("echo \"echo c > /proc/sysrq-trigger\" >> panic.sh")
            self.ExecuteCommand("sync")
            self.ExecuteCommand("nohup bash panic.sh &")
            self._debug("Disconnecting SSH")
            self._close_command_session()
            self._debug("Waiting to go down")
            while self.Ping(): pass

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetOSVersion(self):
        """
        Get the version of the OS on the client

        Returns:
            A string containing version info
        """
        if self.remoteOS == OSType.Windows:
            _, stdout, _ = self.ExecuteCommand("wmic os get caption,servicepackmajorversion /format:csv")
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0:
                    continue
                if not line.lower().startswith(self.hostname.lower()[:15]):
                    continue
                pieces = line.split(",")
                osver = pieces[1].strip()
                if int(pieces[2]) > 0:
                    osver += " SP" + pieces[2]
                return osver
            raise ClientError("Could not find OS info")

        elif self.remoteOS == OSType.Linux:
            if self.remoteOSVersion == "ubuntu" or self.remoteOSVersion == "solidfire":
                _, stdout, _ = self.ExecuteCommand("lsb_release -d")
                m = re.search(r"Description:\s+(.+)", stdout)
                if m:
                    return m.group(1).strip()
            elif self.remoteOSVersion == "redhat":
                _, stdout, _ = self.ExecuteCommand("cat /etc/redhat-release")
                return stdout.strip()

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def GetDHCPEnabled(self, interfaceName=None, interfaceMAC=None):
        """
        Get whether or not the client has DHCP enabled on an interface

        Args:
            interfaceName:  the name of the interface to query
            interfaceMAC:   the MAC address of the interface to query

        Returns:
            Boolean true if DHCP is enabled, false otherwise
        """
        if not interfaceName and not interfaceMAC:
            raise ClientError("Please specify interface name or interface MAC")
        interface_mac = interfaceMAC
        if interface_mac:
            interface_mac = interface_mac.replace(":", "")
            interface_mac = interface_mac.replace("-", "")
            interface_mac = interface_mac.lower()

        if self.remoteOS == OSType.Linux:
            self._debug("Searching for network interfaces")
            _, stdout, _ = self.ExecuteCommand("ifconfig -a")
            ifaces = {}
            for line in stdout.split("\n"):
                m = re.search(r"^(\S+)\s+.+HWaddr (\S+)", line)
                if m:
                    iface_name = m.group(1)
                    mac = m.group(2)
                    ifaces[iface_name] = dict()
                    ifaces[iface_name]["name"] = iface_name
                    ifaces[iface_name]["mac"] = mac.lower().replace(":", "")
                    continue
                m = re.search(r"inet addr:(\S+)", line)
                if m:
                    ip = m.group(1)
                    if ip == "127.0.0.1":
                        continue
                    ifaces[iface_name]["ip"] = ip

            for name in ifaces.keys():
                output = []
                for k,v in ifaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if interfaceMAC:
                for iface in ifaces.keys():
                    if ifaces[iface]["mac"] == interface_mac:
                        interface_name = iface
                if not interface_name:
                    raise ClientError("Could not find interface with MAC '{}'".format(interfaceMAC))
            if interfaceName:
                if interfaceName not in ifaces.keys():
                    raise ClientError("Could not find interface '{}'".format(interfaceName))
                interface_name = interfaceName

            if self.remoteOSVersion == "ubuntu":
                retcode, stdout, stderr = self.ExecuteCommand("egrep -c -i \"{}.+dhcp\" /etc/network/interfaces".format(interface_name), throwOnError=False)
                if retcode != 0 and retcode != 1:
                    raise ClientError("Could not search interfaces file: {}".format(stderr))
                if stdout.strip() == "1":
                    return True
                else:
                    return False

            elif self.remoteOSVersion == "redhat":
                retcode, stdout, stderr = self.ExecuteCommand("egrep -c -i dhcp /etc/sysconfig/network-scripts/ifcfg-{}".format(interface_name), throwOnError=False)
                if retcode != 0 and retcode != 1:
                    raise ClientError("Could not search interfaces file: {}".format(stderr))
                if stdout.strip() == "1":
                    return True
                else:
                    return False
            else:
                raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOSVersion))

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def IsHealthy(self):
        """
        Check various info on the client to see if it appears to be behaving normally

        Returns:
            Boolean true if the client is healthy, false otherwise
        """
        if self.remoteOS == OSType.Linux:
            #self._step("Checking health")

            # alphabetically first MAC
            _, stdout, _ = self.ExecuteCommand("ifconfig | grep HWaddr | awk '{print $5}' | sed 's/://g' | sort | head -1")
            unique_id = stdout.strip()

            # Get uptime
            _, stdout, _ = self.ExecuteCommand("cat /proc/uptime | awk '{print $1}'")
            uptime = stdout.strip()

            # Check memory usage
            _, stdout, _ = self.ExecuteCommand("cat /proc/meminfo")
            mem_total = 0
            mem_free = 0
            mem_buff = 0
            mem_cache = 0
            for line in stdout.split("\n"):
                m = re.search(r"MemTotal:\s+(\d+) kB", line)
                if m:
                    mem_total = float(m.group(1))
                    continue
                m = re.search(r"MemFree:\s+(\d+) kB", line)
                if m:
                    mem_free = float(m.group(1))
                    continue
                m = re.search(r"Buffers:\s+(\d+) kB", line)
                if m:
                    mem_buff = float(m.group(1))
                    continue
                m = re.search(r"Cached:\s+(\d+) kB", line)
                if m:
                    mem_cache = float(m.group(1))
                    continue
            mem_usage = 0
            if mem_total > 0:
                mem_usage = "%.1f" % (100 - ((mem_free + mem_buff + mem_cache) * 100) / mem_total)

            # Check CPU usage
            cpu_usage = "-1"
            try:
                _, stdout, _ = self.ExecuteCommand("top -b -d 1 -n 2 | grep Cpu | tail -1")
                m = re.search(r"(\d+\.\d+)%id", stdout)
                if (m):
                    cpu_usage = "%.1f" % (100.0 - float(m.group(1)))
            except ValueError:
                pass

            # Check if vdbench is running here
            _, stdout, _ = self.ExecuteCommand("ps -ef | grep -v grep | grep java | grep vdbench | wc -l")
            vdbench_count = 0
            try:
                vdbench_count = int(stdout.strip())
            except ValueError:
                pass

            # See if vdbenchd is in use
            _, stdout, _ = self.ExecuteCommand("if [ -f /opt/vdbench/last_vdbench_pid ]; then echo 'True'; else echo 'False'; fi")
            vdbenchd = bool(stdout.strip())

            # See if we have a vdbench last exit status
            vdbench_exit = -1
            _, stdout, _ = self.ExecuteCommand("cat /opt/vdbench/last_vdbench_exit")
            try:
                vdbench_exit = int(stdout.strip())
            except ValueError:pass

            self._step("Checking health")
            self._info("Hostname {} MAC {}".format(self.hostname, unique_id))
            self._info("Uptime {}".format(uptime))

            # Use vdbench status to determine health
            healthy = True
            if vdbench_count > 0:
                self._info("vdbench is running")
            elif not vdbenchd and vdbench_count <= 0:
                self._error("vdbench failed")
                healthy = False
            elif vdbenchd and vdbench_exit == 0:
                self._info("Last vdbench run finished without errors")
            else:
                self._error("vdbench failed")
                healthy = False

            if cpu_usage > 0:
                self._info("CPU usage {}%".format(cpu_usage))
            if mem_usage > 0:
                self._info("Mem usage {}".format(mem_usage))

            if healthy:
                self._passed("Client is healthy")
            else:
                self._error("Client is not healthy")

            return healthy

        else:
            raise ClientError("Sorry, this is not implemented for {}".format(self.remoteOS))

    def HostnameToAccountName(self):
        return self.hostname.split(".")[0]

    def GetHBAInfo(self):
        """
        Get a summary of FC HBAs present in this client

        Returns:
            A dictionary of HBA info (dict)
        """

        # TODO make this less terrible

        hbas = {}
        _, host_stdout, _ = self.ExecuteCommand("ls /sys/class/fc_host")
        for line in host_stdout.split("\n"):
            line = line.strip()
            if not line:
                continue
            host = line
            m = re.search(r"(\d+)", host)
            if m:
                host_num = m.group(1)
            hbas[host] = {}

            cmd = "[ -e /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/modeldesc ] && cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/modeldesc || cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/model_desc"
            _, stdout, _ = self.ExecuteCommand(cmd)
            hbas[host]['desc'] = stdout.strip()

            cmd = "cat /sys/class/fc_host/" + host + "/port_name"
            _, stdout, _ = self.ExecuteCommand(cmd)
            hbas[host]['wwn'] = util.HumanizeWWN(stdout.strip())

            cmd = "cat /sys/class/fc_host/" + host + "/speed"
            _, stdout, _ = self.ExecuteCommand(cmd)
            hbas[host]['speed'] = stdout.strip()

            cmd = "cat /sys/class/fc_host/" + host + "/device/scsi_host/" + host + "/link_state"
            _, stdout, _ = self.ExecuteCommand(cmd)
            link_state = stdout.strip()
            if "-" in link_state:
                link_state = link_state[:link_state.index("-")-1].strip()
            hbas[host]['link'] = link_state

            cmd = "for port in `ls -1d /sys/class/fc_remote_ports/rport-" + host_num + "*`; do a=$(cat $port/roles); if [[ $a == *Target* ]]; then cat $port/port_name; fi; done"
            _, stdout, _ = self.ExecuteCommand(cmd)
            hbas[host]['targets'] = []
            for line1 in stdout.split("\n"):
                line1 = line1.strip()
                if not line1:
                    continue
                hbas[host]['targets'].append(util.HumanizeWWN(line1.strip()))

        return hbas
