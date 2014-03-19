#!/usr/bin/python
import lib.libsf as libsf
from lib.libsf import mylog
import subprocess
import socket
import sys
import time
import re
import os
import string
import tempfile
import commands
import platform
try:
    import ssh
except ImportError:
    import paramiko as ssh

# Why can't we have enums in python?
class OsType:
    Linux, MacOS, Windows, ESX, SunOS = ("Linux", "MacOS", "Windows", "ESX", "SunOS")

# Generic exception to wrap around all errors thrown from the SfClient class
class ClientError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message
class ClientAuthorizationError(ClientError): pass
class ClientRefusedError(ClientError): pass

class SfClient:
    def __init__(self):
        self.Localhost = False
        self.LocalOs = None
        self.RemoteOs = None
        self.RemoteOsVersion = ""
        self.IpAddress = None
        self.Username = None
        self.Password = None
        self.SshSession = None
        self.Hostname = None
        self.AllIpAddresses = []
        self.ChapCredentials = dict()
        self.EsxIscsiHba = None

        if sys.platform.startswith("linux"):
            self.LocalOs = OsType.Linux
        elif sys.platform.startswith("darwin"):
            self.LocalOs = OsType.MacOS
        elif sys.platform.startswith("win"):
            self.LocalOs = OsType.Windows
        else:
            raise ClientError("Unsupported local OS " + sys.platform)

    def _log_prefix(self):
        return "  " + self.IpAddress + ": "
    def _debug(self, pMessage):
        mylog.debug(self._log_prefix() + pMessage)
    def _info(self, pMessage):
        mylog.info(self._log_prefix() + pMessage)
    def _warn(self, pMessage):
        mylog.warning(self._log_prefix() + pMessage)
    def _error(self, pMessage):
        mylog.error(self._log_prefix() + pMessage)
    def _passed(self, pMessage):
        mylog.passed(self._log_prefix() + pMessage)
    def _step(self, pMessage):
        mylog.step(self._log_prefix() + pMessage)

    def Connect(self, pClientIp, pUsername, pPassword):
        self.IpAddress = str(pClientIp).lower()
        self.Username = str(pUsername)
        self.Password = str(pPassword)

        if self.IpAddress == "localhost":
            if "win" in platform.system().lower():
                raise ClientError("Sorry, running on Windows is not supported")
            uname_str = commands.getoutput("uname -a")
            uname_str = uname_str.lower()
            if "win" in uname_str:
                raise ClientError("Sorry, running on Windows is not supported")
            elif "linux" in uname_str:
                self.RemoteOs = OsType.Linux
                if "ubuntu" in uname_str:
                    self.RemoteOsVersion = "ubuntu"
                elif "el" in uname_str:
                    self.RemoteOsVersion = "redhat"
                elif "fc" in uname_str:
                    self.RemoteOsVersion = "redhat"
            elif "vmkernel" in uname_str:
                self.RemoteOs = OsType.ESX
                m = re.search(" (\d\.\d)\.\d+ ", uname_str)
                if m:
                    self.RemoteOsVersion = m.group(1)
            elif "sunos" in uname_str:
                self.RemoteOs = OsType.SunOS
            else:
                self._warn("Could not determine type of client; assuming Linux (uname -> " + uname_str + ")")
                self.RemoteOs = OsType.Linux

            self.Hostname = platform.node()
            return

        # First try with winexe to see if it is a Windows client
        try:
            self._debug("Attempting connect to " + pClientIp + " with winexe as " + pUsername + ":" + pPassword)
            retcode, stdout, stderr = self._execute_winexe_command(pClientIp, pUsername, pPassword, "hostname", 10)
            if (retcode == 0):
                # Successfully connected - must be a windows machine
                self.RemoteOs = OsType.Windows
                self.Hostname = stdout.strip()

                # Make sure winexe will survive a reboot
                retcode, stdout, stderr = self._execute_winexe_command(pClientIp, pUsername, pPassword, "sc config winexesvc start= auto")

                # Make sure the latest version of diskapp.exe is on the client
                self._debug("Updating diskapp")
                if self.LocalOs == OsType.Windows:
<<<<<<< HEAD:libclient.py
                    command = r"robocopy windiskhelper/bin \\" + str(self.IpAddress) + r"\c$\Windows\System32 diskapp.* /z /r:1 /w:3 /tbd"
=======
                    lib_path = os.path.dirname(os.path.realpath(__file__))
                    command = "robocopy " + lib_path + r"\diskapp\bin \\" + str(self.IpAddress) + r"\c$\Windows\System32 diskapp.* /z /r:1 /w:3 /tbd"
>>>>>>> 3ba6dc2... Refactor scripts to be importable as modules; various bugfixes and cleanup:lib/libclient.py
                    retcode, stdout, stderr = libsf.RunCommand(command)
                    if retcode != 0:
                        raise ClientError("Failed to update diskapp on client: " + stderr)
                else:
<<<<<<< HEAD:libclient.py
                    command = "smbclient //" + self.IpAddress + "/c$ " + self.Password + " -U " + self.Username + " <<EOC\ncd Windows\\System32\nlcd diskapp/bin\nput diskapp.exe\nexit\nEOC"
=======
                    command = "smbclient //" + self.IpAddress + "/c$ " + self.Password + " -U " + self.Username + " <<EOC\ncd Windows\\System32\nlcd " + lib_path + "/windiskhelper\nput diskapp.exe\nexit\nEOC"
>>>>>>> 3ba6dc2... Refactor scripts to be importable as modules; various bugfixes and cleanup:lib/libclient.py
                    retcode, stdout, stderr = libsf.RunCommand(command)
                    if retcode != 0:
                        raise ClientError("Failed to update diskapp on client: " + stderr)
                    for line in stdout.split("\n"):
                        if line.startswith("NT_"):
                            raise ClientError("Failed to update diskapp on client: " + line)
        except ClientRefusedError:
            pass
        except ClientAuthorizationError:
            raise
        except ClientError as e:
            if "Windows" in e.message: raise

        # If winexe fails, try with SSH
        if self.Hostname == None:
            try:
                self._debug("Attempting connect to " + pClientIp + " with SSH as " + pUsername + ":" + pPassword)
                retcode, stdout, stderr = self._execute_ssh_command(pClientIp, pUsername, pPassword, "hostname")
                self.Hostname = stdout.strip()

                # Check what we are connected to
                retcode, stdout, stderr = self._execute_ssh_command(pClientIp, pUsername, pPassword, "uname -a")
                uname_str = stdout.lower()
                if retcode != 0: raise ClientError("Could not run uname on client")
                if "win" in uname_str:
                    raise ClientError("Sorry, cygwin is not supported. This script requires your Windows client accept connections from winexe")
                elif "vmkernel" in uname_str:
                    self.RemoteOs = OsType.ESX
                    m = re.search(" (\d\.\d)\.\d+ ", uname_str)
                    if m:
                        self.RemoteOsVersion = m.group(1)
                elif "linux" in uname_str:
                    self.RemoteOs = OsType.Linux
                    if "ubuntu" in uname_str:
                        self.RemoteOsVersion = "ubuntu"
                    elif "el" in uname_str:
                        self.RemoteOsVersion = "redhat"
                    elif "fc" in uname_str:
                        self.RemoteOsVersion = "redhat"
                elif "sunos" in uname_str:
                    self.RemoteOs = OsType.SunOS
                else:
                    self._warn("Could not determine type of remote client; assuming Linux (uname -> " + uname_str + ")")
                    self.RemoteOs = OsType.Linux

            except ClientAuthorizationError:
                raise
            except ClientError as e:
                if "Windows" in e.message: raise

        if self.Hostname == None:
            raise ClientError("Could not connect to client " + pClientIp)

        self._debug("Client OS is " + str(self.RemoteOs) + " " + str(self.RemoteOsVersion))

        # Save a list of all the IPs on this client
        self.AllIpAddresses = self.GetIpv4Addresses()

    def _execute_ssh_command(self, pClientIp, pUsername, pPassword, pCommand):
        if self.SshSession == None or self.SshSession.get_transport() == None or not self.SshSession.get_transport().is_active():
            self._debug("Connecting SSH")
            self.SshSession = ssh.SSHClient()
            self.SshSession.set_missing_host_key_policy(ssh.AutoAddPolicy())
            self.SshSession.load_system_host_keys();
            try:
                self.SshSession.connect(pClientIp, username=pUsername, password=pPassword)
            except ssh.AuthenticationException:
                raise ClientError("Invalid username/password for " + pClientIp)
            except ssh.SSHException as e:
                raise ClientError("SSH error connecting to " + pClientIp + ": " + e.message)
            except socket.error as e:
                raise ClientError("Could not connect to " + pClientIp + ": " + str(e))

        self._debug("Executing " + pCommand)
        stdin, stdout, stderr = self.SshSession.exec_command(pCommand)
        return_code = stdout.channel.recv_exit_status()
        stdout_data = stdout.readlines()
        stderr_data = stderr.readlines()

        stdout_data = "".join(stdout_data)
        stderr_data = "".join(stderr_data)
        return return_code, stdout_data, stderr_data

    def _execute_winexe_command(self, pClientIp, pUsername, pPassword, pCommand, pTimeout=30):
        lib_path = os.path.dirname(os.path.realpath(__file__))
        if (self.LocalOs == OsType.Linux):
            winexe = os.path.join(lib_path, "winexe/bin/winexe.linux") + " --system -U " + pUsername + "%" + pPassword + " //" + pClientIp + " \"" + pCommand + "\""
        elif (self.LocalOs == OsType.MacOS):
            winexe = os.path.join(lib_path, "winexe/bin/winexe.macos") + " --system -U " + pUsername + "%" + pPassword + " //" + pClientIp + " \"" + pCommand + "\""
        elif self.LocalOs == OsType.Windows:
            winexe = os.path.join(lib_path, r"winexe\bin\psexec.exe") + r" \\" + str(pClientIp) + " -n 3 -u " + pUsername + " -p " + pPassword + " " + pCommand
        self._debug("Executing " + winexe)

        # Run psexec
        if self.LocalOs == OsType.Windows:
            return_code, stdout, raw_stderr = libsf.RunCommand(winexe, pTimeout)
            if return_code == None:
                raise ClientRefusedError("Could not connect to client - not responding or not Windows")

            # Parse psexec output from stderr
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

                if line.startswith("Connecting to " + pClientIp):
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

            return return_code, stdout, stderr

        # Run winexec
        else:
            retry = 5
            while True:
                return_code, stdout, stderr = libsf.RunCommand(winexe, pTimeout)
                # The returncode and stdout/stderr may be the return of the remote command or the return of winexe itself
                # In either case the return code may be non-zero
                # If winexe succeeded but the remote command failed, we want to pass the non-zero return and stdout/stderr back to the caller
                # If winexe itself failed to connect and run the command we want to raise an exception

                # Look for known error signatures from winexe, and assume everything else is from the remote command
                if "NT_STATUS_RESOURCE_NAME_NOT_FOUND" in stdout:
                    # usually a transient network error
                    self._debug("Connect to " + pClientIp + " failed NT_STATUS_RESOURCE_NAME_NOT_FOUND - retrying")
                    retry -= 1
                    if retry > 0:
                        time.sleep(1)
                        continue
                    else:
                        raise ClientError("Could not connect to Windows client - NT_STATUS_RESOURCE_NAME_NOT_FOUND")
                elif "NT_STATUS_ACCESS_DENIED" in stdout:
                    raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
                elif "NT_STATUS_LOGIN_FAILURE" in stdout:
                    raise ClientAuthorizationError("Could not connect to Windows client - auth or permission error")
                elif "Failed to install service" in stdout:
                    raise ClientError("Could not install winexe service on Windows client.  Try executing 'sc delete winexesvc'on the client and then try this script again")
                elif "NT_STATUS_CONNECTION_REFUSED" in stdout:
                    raise ClientRefusedError("Could not connect to client - port blocked or not Windows")
                elif "NT_STATUS_HOST_UNREACHABLE" in stdout:
                    raise ClientRefusedError("Could not connect to client - port blocked or not Windows")
                elif "NT_STATUS_IO_TIMEOUT" in stdout:
                    raise ClientRefusedError("Could not connect to client - not responding or not Windows")
                elif stdout.startswith("ERROR:") and "NT_" in stdout:
                    raise ClientError("Could not connect to Windows client - " + stdout)

                return return_code, stdout, stderr

    def ExecuteCommand(self, pCommand, pIpAddress = None):
        pCommand = str(pCommand)
        if pCommand == None or len(pCommand) == 0:
            return -1, '', ''
        if pIpAddress == None:
            pIpAddress = self.IpAddress

        if self.Localhost:
            return_code, stdout_data = commands.getstatusoutput(pCommand)
            return return_code, stdout_data, ""

        if self.RemoteOs == OsType.Windows:
            return self._execute_winexe_command(pIpAddress, self.Username, self.Password, pCommand)
        else:
            return self._execute_ssh_command(pIpAddress, self.Username, self.Password, pCommand)

    def _shell_quote(self, pInputString):
        output = pInputString;
        if self.RemoteOs == OsType.Windows:
            output = output.replace("\"", "\\\"\\\"\\\"\\\"")
        else:
            output = output.replace("\"", "\\\"")
        output = output.replace(";", "\;")
        return output

    def _get_esx_iscsi_hba(self):
        if not self.EsxIscsiHba:
            self._debug("Getting the iSCSI HBA name")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter list | grep iscsi_vmk")
            if retcode != 0: raise ClientError(stderr)
            pieces = re.split("\s+", stdout)
            self.EsxIscsiHba = pieces[0]
        return self.EsxIscsiHba

    def GetIpv4Addresses(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("wmic nicconfig where ipenabled=true get ipaddress /format:csv")
            if retcode != 0: raise ClientError(stderr)
            # This wmic call returns something like this:
            #Node,IPAddress
            #WIN-TEMPLATE,{10.10.59.152;fe80::81dc:2f02:864a:4b96}
            #WIN-TEMPLATE,{192.168.128.24;fe80::d188:3b8a:c96d:b86b}
            iplist = []
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0: continue
                if not line.startswith(self.Hostname): continue
                pieces = line.split(',')
                pieces = pieces[1].strip('{}').split(';')
                for ip in pieces:
                    if not ":" in ip:
                        iplist.append(ip)
            return iplist

        elif self.RemoteOs == OsType.ESX:
            retcode, stdout, stderr = self.ExecuteCommand("esxcli network ip interface ipv4 get | grep vmk")
            if retcode != 0: raise ClientError(stderr)
            iplist = []
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                pieces = re.split("\s+", line)
                iplist.append(pieces[1])
            return iplist

        elif self.RemoteOs == OsType.Linux:
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig | grep 'inet '")
            if retcode != 0: raise ClientError(stderr)
            iplist = []
            for line in stdout.split("\n"):
                m = re.search("inet addr:(\S+)", line)
                if m and m.group(1) != "127.0.0.1":
                    iplist.append(m.group(1))
            return iplist

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig -a | grep 'inet '")
            if retcode != 0: raise ClientError(stderr)
            iplist = []
            for line in stdout.split("\n"):
                m = re.search("inet (\S+)", line)
                if m and m.group(1) != "127.0.0.1":
                    iplist.append(m.group(1))
            return iplist

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def ChangeIpAddress(self, NewIp, NewMask, NewGateway=None, InterfaceName=None, InterfaceMac=None, UpdateHosts=None):
        if not InterfaceName and not InterfaceMac:
            raise ClientError("Please specify interface name or interface MAC")
        interface_mac = InterfaceMac
        if interface_mac:
            interface_mac = interface_mac.replace(":", "")
            interface_mac = interface_mac.replace("-", "")
            interface_mac = interface_mac.lower()

        if self.RemoteOs == OsType.Windows:
            interfaces = dict()
            # Get a list of interfaces and MAC addresses
            retcode, stdout, stderr = self.ExecuteCommand("getmac.exe /v /nh /fo csv")
            if retcode != 0: raise ClientError("Could not get mac address list: " + stderr)
            # "Local Area Connection 12","vmxnet3 Ethernet Adapter #18","00-50-56-A4-38-2F","\Device\Tcpip_{1058FAE5-466C-4229-A75A-91FA71ABAC8E}"
            # "Local Area Connection 13","vmxnet3 Ethernet Adapter #19","00-50-56-A4-38-2E","\Device\Tcpip_{DC53B222-EE87-4492-81A1-D1FC1845F555}"
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0: continue
                pieces = line.split(",")
                name = pieces[0].strip('"')
                mac = pieces[2].strip('"').replace("-","").lower()
                interfaces[name] = dict()
                interfaces[name]["name"] = name
                interfaces[name]["mac"] = mac

            # Get a list of interfaces and indexes
            retcode, stdout, stderr = self.ExecuteCommand("netsh interface ip show interface")
            if retcode != 0: raise ClientError("Could not get interface list: " + stderr)
            # Idx     Met         MTU          State                Name
            # ---  ----------  ----------  ------------  -------------------------
            #   1          50  4294967295  connected     Loopback Pseudo-Interface
            #  33           5        1500  connected     Local Area Connection 12
            #  34           5        1500  connected     Local Area Connection 13
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0: continue
                m = re.search("^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.+)", line)
                if m:
                    index = int(m.group(1))
                    #metric = int(m.group(2))
                    #mtu = int(m.group(3))
                    #state = m.group(4)
                    name = m.group(5)
                    if "loopback" in name.lower(): continue
                    if name not in interfaces.keys(): continue
                    interfaces[name]["index"] = index

            # Get a list of interfaces and IP addresses
            retcode, stdout, stderr = self.ExecuteCommand("netsh interface ip show addresses")
            if retcode != 0: raise ClientError("Could not get ip address list: " + stderr)
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
                if len(line) <= 0: continue
                m = re.search("Configuration for interface \"(.+)\"", line)
                if m:
                    name = m.group(1)
                    continue
                m = re.search("\s*IP Address:\s+(.+)", line)
                if m:
                    ip = m.group(1)
                    if ip.startswith("127."): continue
                    interfaces[name]["ip"] = ip
                    continue

            for name in interfaces.keys():
                output = []
                for k,v in interfaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if InterfaceMac:
                for name in interfaces.keys():
                    if interfaces[name]["mac"] == interface_mac:
                        interface_name = name
                        break
                if not interface_name: raise ClientError("Could not find interface with MAC '" + InterfaceMac + "'")
            if InterfaceName:
                if InterfaceName not in interfaces.keys():
                    raise ClientError("Could not find interface '" + InterfaceName + "'")
                interface_name = InterfaceName
            interface_index = interfaces[interface_name]["index"]
            old_ip = interfaces[interface_name]["ip"]
            self._info("Changing IP on " + interface_name + " from " + old_ip + " to " + NewIp)

            command = "netsh interface ip set address name=" + str(interface_index) + " source=static addr=" + NewIp + " mask=" + NewMask
            if NewGateway:
                command += " gateway=" + NewGateway + " gwmetric=0"
            else:
                command += " gateway=none"
            retcode, stdout, stderr = self.ExecuteCommand(command)

            if self.IpAddress == old_ip:
                    self.IpAddress = NewIp
                    start_time = time.time()
                    found = False
                    while (not found and time.time() - start_time < 2 * 60):
                        found = self.Ping(NewIp)
                    if not found:
                        raise ClientError("Can't contact " + self.Hostname + " on the network - something went wrong")
            self.AllIpAddresses = self.GetIpv4Addresses()
            self._passed("Successfully changed IP address")

        elif self.RemoteOs == OsType.Linux:
            self._debug("Searching for network interfaces")
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig -a")
            if retcode != 0: raise ClientError("Could not get interface list: " + stderr)
            ifaces = dict()
            for line in stdout.split("\n"):
                m = re.search("^(\S+)\s+.+HWaddr (\S+)", line)
                if m:
                    iface_name = m.group(1)
                    mac = m.group(2)
                    ifaces[iface_name] = dict()
                    ifaces[iface_name]["name"] = iface_name
                    ifaces[iface_name]["mac"] = mac.lower().replace(":", "")
                    continue
                m = re.search("inet addr:(\S+)", line)
                if m:
                    ip = m.group(1)
                    if ip == "127.0.0.1": continue
                    ifaces[iface_name]["ip"] = ip

            for name in ifaces.keys():
                output = []
                for k,v in ifaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if InterfaceMac:
                for iface in ifaces.keys():
                    if ifaces[iface]["mac"] == interface_mac:
                        interface_name = iface
                if not interface_name: raise ClientError("Could not find interface with MAC '" + InterfaceMac + "'")
            if InterfaceName:
                if InterfaceName not in ifaces.keys():
                    raise ClientError("Could not find interface '" + InterfaceName + "'")
                interface_name = InterfaceName

            if self.RemoteOsVersion == "ubuntu":
                old_ip = ifaces[interface_name]["ip"]
                self._info("Changing IP on " + interface_name + " from " + old_ip + " to " + NewIp)

                # Back up old interfaces file
                self._debug("Backing up old interfaces file to interfaces.bak")
                retcode, stdout, stderr = self.ExecuteCommand("cp /etc/network/interfaces /etc/network/interfaces.bak")
                if retcode != 0: raise ClientError("Could not back up interfaces file: " + stderr)

                # Change the configuration in the interfaces file
                lib_path = os.path.dirname(os.path.realpath(__file__))
                sftp = self.SshSession.open_sftp()
                sftp.put(os.path.join(lib_path, "changeinterface.awk"), "changeinterface.awk")
                sftp.close()
                self._debug("Changing IP address in /etc/network/interfaces")
                command = "awk -f changeinterface.awk /etc/network/interfaces device=" + interface_name + " address=" + NewIp + " netmask=" + NewMask
                if NewGateway:
                    command += " gateway=" + NewGateway
                command += " > interfaces"
                awk_retcode, stdout, stderr = self.ExecuteCommand(command)
                cp_retcode, stdout, stderr = self.ExecuteCommand("cp interfaces /etc/network/interfaces")
                retcode, stdout, stderr = self.ExecuteCommand("rm interfaces changeinterface.awk")
                if awk_retcode != 0: raise ClientError("Could not update interfaces file: " + stderr)
                if cp_retcode != 0: raise ClientError("Could not copy interfaces file: " + stderr)

                # Restart networking
                retcode, stdout, stderr = self.ExecuteCommand("echo \"sleep 5\" > restart_net.sh")
                retcode, stdout, stderr = self.ExecuteCommand("echo \"/etc/init.d/networking restart\" >> restart_net.sh")
                retcode, stdout, stderr = self.ExecuteCommand("echo \"rm restart_net.sh\" >> restart_net.sh")
                self.ExecuteCommand("sync")
                self.ExecuteCommand("nohup bash restart_net.sh 2>&1 >/tmp/netrestart &")
                self._debug("Disconnecting SSH")
                self.SshSession.close()
                self.SshSession = None
                time.sleep(30)

                if self.IpAddress == old_ip:
                    self.IpAddress = NewIp
                    start_time = time.time()
                    found = False
                    while (not found and time.time() - start_time < 2 * 60):
                        found = self.Ping(NewIp)
                    if not found:
                        raise ClientError("Can't contact " + self.Hostname + " on the network - something went wrong")
                self.AllIpAddresses = self.GetIpv4Addresses()
                self._passed("Successfully changed IP address")

            elif self.RemoteOsVersion == "redhat":
                old_ip = ifaces[interface_name]["ip"]
                self._info("Changing IP on " + interface_name + " from " + old_ip + " to " + NewIp)

                interface_conf_file = "/etc/sysconfig/network-scripts/ifcfg-" + interface_name

                # Back up old config file
                self._debug("Backing up old ifcfg file")
                retcode, stdout, stderr = self.ExecuteCommand("cp " + interface_conf_file + " " + interface_conf_file + ".bak")
                if retcode != 0: raise ClientError("Could not back up interface file: " + stderr)

                # Copy the file locally to work on.  If something goes wrong, we can raise an exception and bail without leaving a partially configured file around
                retcode, stdout, stderr = self.ExecuteCommand("cp " + interface_conf_file + " ifcfg")

                # See if the interface is already statically configured.  If so, replace the existing config
                # grep return codes: 0 means it was successful and found a match, 1 means it was successful but found no matches, anything else means an error
                retcode, stdout, stderr = self.ExecuteCommand("grep BOOTPROTO=none " + interface_conf_file)
                if retcode == 0:
                    command = "sed -i -e 's/IPADDR=.*/IPADDR=" + NewIp + "/' -e s/NETMASK=.*/NETMASK=" + NewMask + "/"
                    if NewGateway:
                        command +=  " -e s/GATEWAY=.*/GATEWAY=" + NewGateway + "/"
                    command += " -e s/PREFIX=.*/d ifcfg"
                    retcode, stdout, stderr = self.ExecuteCommand(command)
                    if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                    retcode, stdout, stderr = self.ExecuteCommand("grep NETMASK ifcfg")
                    if retcode == 1:
                        retcode, stdout, stderr = self.ExecuteCommand("echo NETMASK=" + NewMask + " >> ifcfg")
                        if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                    elif retcode != 0: raise ClientError("Could not grep interface file: " + stderr)
                elif retcode == 1:
                    # If the interface was DHCP, change it to static
                    retcode, stdout, stderr = self.ExecuteCommand("sed -i -e 's/BOOTPROTO=.*/BOOTPROTO=none/' ifcfg")
                    if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                    retcode, stdout, stderr = self.ExecuteCommand("echo IPADDR=" + NewIp + " >> ifcfg")
                    if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                    retcode, stdout, stderr = self.ExecuteCommand("echo NETMASK=" + NewMask + " >> ifcfg")
                    if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                    if NewGateway:
                        retcode, stdout, stderr = self.ExecuteCommand("echo GATEWAY=" + NewGateway + " >> ifcfg")
                        if retcode != 0: raise ClientError("Could not change interface file: " + stderr)
                else:
                    raise ClientError("Could not grep interface file: " + stderr)

                # Move the local file over the real file
                retcode, stdout, stderr = self.ExecuteCommand("mv ifcfg " + interface_conf_file)
                if retcode != 0: raise ClientError("Could not copy interface file: " + stderr)

                # Restart networking
                retcode, stdout, stderr = self.ExecuteCommand("echo \"sleep 5\" > restart_net.sh")
                retcode, stdout, stderr = self.ExecuteCommand("echo \"/etc/init.d/network restart\" >> restart_net.sh")
                retcode, stdout, stderr = self.ExecuteCommand("echo \"rm -f restart_net.sh\" >> restart_net.sh")
                self.ExecuteCommand("sync")
                self.ExecuteCommand("nohup bash restart_net.sh 2>&1 >/tmp/netrestart &")
                self._debug("Disconnecting SSH")
                self.SshSession.close()
                self.SshSession = None
                time.sleep(30)

                if self.IpAddress == old_ip:
                    self.IpAddress = NewIp
                    start_time = time.time()
                    found = False
                    while (not found and time.time() - start_time < 2 * 60):
                        found = self.Ping(NewIp)
                    if not found:
                        raise ClientError("Can't contact " + self.Hostname + " on the network - something went wrong")
                self.AllIpAddresses = self.GetIpv4Addresses()
                self._passed("Successfully changed IP address")

            else:
                raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOsVersion))

            if UpdateHosts:
                retcode, stdout, stderr = self.ExecuteCommand("echo \"127.0.0.1           localhost\" > /etc/hosts")
                retcode, stdout, stderr = self.ExecuteCommand("echo \"" + NewIp + "       " + self.Hostname + "\" >> /etc/hosts")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def UpdateHostname(self, pNewHostname):
        self._info("Checking hostname")
        if pNewHostname.lower() == self.Hostname.lower():
            self._debug("The hostname is already updated")
            return False

        if self.RemoteOs == OsType.Windows:
            self._info("Setting hostname to " + pNewHostname)
            retcode, stdout, stderr = self.ExecuteCommand("cmd.exe /c wmic computersystem where name='%COMPUTERNAME%' call rename name='" + pNewHostname + "'")
            if (retcode != 0): raise ClientError(stderr)

        elif self.RemoteOs == OsType.SunOS:
            self._info("Setting hostname to " + pNewHostname)
            oldhostname = self.Hostname
            retcode, stdout, stderr = self.ExecuteCommand("svccfg -s node setprop config/nodename = \"" + pNewHostname + "\"")
            if retcode != 0: raise ClientError("Failed to set nodename: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("svccfg -s node setprop config/loopback = \"" + pNewHostname + "\"")
            if retcode != 0: raise ClientError("Failed to set loopback: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("svccfg -s system/identity:node refresh")
            if retcode != 0: raise ClientError("Failed to restart service: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("svcadm restart svc:/system/identity:node")
            if retcode != 0: raise ClientError("Failed to restart service: " + stderr)

            # make sure hosts file is correct
            retcode, stdout, stderr = self.ExecuteCommand("echo -e \"::1 " + pNewHostname + " localhost\\n127.0.0.1 " + pNewHostname + " localhost loghost\" > /etc/inet/hosts")
            if retcode != 0: raise ClientError("Failed to restart service: " + stderr)

        elif self.RemoteOs == OsType.Linux:
            self._info("Setting hostname to " + pNewHostname)
            oldhostname = self.Hostname
            self.ExecuteCommand("hostname " + pNewHostname)
            if self.RemoteOsVersion == "redhat":
                # Change /etc/sysconfig/network
                retcode, stdout, stderr = self.ExecuteCommand("chattr -i /etc/sysconfig/network")
                if retcode != 0:
                    raise ClientError("Failed to chattr /etc/sysconfig/network: " + stderr)
                retcode, stdout, stderr = self.ExecuteCommand("sed 's/HOSTNAME=.*/HOSTNAME=" + pNewHostname + "/' /etc/sysconfig/network")
                if retcode != 0:
                    raise ClientError("Failed to update /etc/hostname: " + stderr)

                # Update current hostname
                retcode, stdout, stderr = self.ExecuteCommand("hostname -v " + pNewHostname)
                if retcode != 0:
                    raise ClientError("Failed to change hostname: " + stderr)
            else:
                # Change /etc/hostname
                retcode, stdout, stderr = self.ExecuteCommand("chattr -i /etc/hostname")
                if retcode != 0:
                    raise ClientError("Failed to chattr /etc/hostname: " + stderr)
                self._debug("Changing hostname to '" + pNewHostname + "'")
                retcode, stdout, stderr = self.ExecuteCommand("echo " + pNewHostname + " > /etc/hostname")
                if retcode != 0:
                    raise ClientError("Failed to update /etc/hostname: " + stderr)
                # Update current hostname
                retcode, stdout, stderr = self.ExecuteCommand("hostname -v -b " + pNewHostname)
                if retcode != 0:
                    raise ClientError("Failed to change hostname: " + stderr)

            # Change /etc/hosts
            retcode, stdout, stderr = self.ExecuteCommand("chattr -i /etc/hosts")
            if retcode != 0:
                raise ClientError("Failed to chattr /etc/hosts: " + stderr)
            self._debug("Updating /etc/hosts")
            retcode, stdout, stderr = self.ExecuteCommand("sed -i 's/" + oldhostname + "/" + pNewHostname + "/g' /etc/hosts")
            if retcode != 0:
                raise ClientError("Failed to edit /etc/hosts: " + stderr)

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

        self.Hostname = pNewHostname
        return True

    def GetInitiatorName(self):
        if self.RemoteOs == OsType.Linux:
            retcode, stdout, stderr = self.ExecuteCommand("cat /etc/iscsi/initiatorname.iscsi | cut -d'=' -f2")
            if retcode != 0:
                return None
            return stdout.strip()

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def UpdateInitiatorName(self):
        self._info("Checking iSCSI Initiator name")
        if self.RemoteOs == OsType.Windows:
            # iqn.1991-05.com.microsoft:hostname
            self._debug("Reading current initiator name")
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --show_initiatorname")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            initiator_name = "iqn." + self.Hostname
            for line in stdout.split("\n"):
                m = re.search("INFO\s+(iqn.+)", line)
                if m:
                    pieces = m.group(1).split(":")
                    oldname = pieces.pop()
                    if (oldname == self.Hostname):
                        self._debug("Initiator name is already correct")
                        return False
                    initiator_name = ":".join(pieces) + ":" + self.Hostname
                    break
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --set_initiatorname --name=" + initiator_name)
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            return True

        elif self.RemoteOs == OsType.ESX:
            # iqn.1998-01.com.vmware:hostname
            adapter_name = self._get_esx_iscsi_hba()
            self._debug("Reading current initiator name")
            initiator_name = "iqn.1998-01.com.vmware:" + self.Hostname
            oldname = None
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter get --adapter=" + adapter_name + " | grep Name")
            for line in stdout.split("\n"):
                m = re.search("Name:\s+(iqn\S+)", line)
                if m:
                    oldname = m.group(1)
                    break
            if oldname == iqn:
                self._debug("Initiator name is already correct")
                return False
            self._info("Setting initiator name to '" + initiator_name +"'")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter set --adapter=" + adapter_name + " --name=" + initiator_name)
            if retcode != 0: raise ClientError(stderr)
            return True

        elif self.RemoteOs == OsType.Linux:
            # iqn.1993-08.org.debian:01:hostname - from Ubuntu 10.04
            # iqn.1994-05.com.redhat:hostname - from RHEL 6.3
            self._debug("Reading current initiator name")
            initiator_name = self.GetInitiatorName()
            if initiator_name:
                pieces = initiator_name.split(":")
                oldname = pieces.pop()
                if oldname == self.Hostname:
                    self._debug("Initiator name is already correct")
                    return False
                initiator_name = ":".join(pieces) + ":" + self.Hostname
            else:
                if self.RemoteOsVersion == "ubuntu":
                    initiator_name = "iqn.1993-08.org.debian:01:" + self.Hostname
                else:
                    initiator_name = "iqn.1994-05.com.redhat:" + self.Hostname
            self._info("Setting initiator name to '" + initiator_name + "'")
            retcode, stdout, stderr = self.ExecuteCommand("echo InitiatorName=" + initiator_name + " > /etc/iscsi/initiatorname.iscsi")
            if retcode != 0: raise ClientError(stderr)
            return True

        elif self.RemoteOs == OsType.SunOS:
            # iqn.1986-03.com.sun:01:hostname
            self._debug("Reading current initiator name")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list initiator-node")
            if retcode != 0: raise ClientError("Could not read initiator name: " + stderr)
            initiator_name = "iqn.1986-03.com.sun:01:" + self.Hostname
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.search("Initiator node name: (\S+)", line)
                if m:
                    pieces = m.group(1).split(":")
                    oldname = pieces.pop()
                    if oldname == self.Hostname:
                        self._debug("Initiator name is already correct")
                        return False
                    initiator_name = ":".join(pieces) + ":" + self.Hostname
                    break
            self._info("Setting initiator name to '" + initiator_name + "'")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify initiator-node --node-name=" + initiator_name)
            if retcode != 0: raise ClientError("Could not set initiator name: " + stderr)

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def Ping(self, pIpAddress = None):
        if pIpAddress == None:
            pIpAddress = self.IpAddress

        if self.LocalOs == OsType.Windows:
            command = "ping -n 2 %s"
        elif self.LocalOs == OsType.MacOS:
            command = "ping -n -i 1 -c 3 -W 0.5 -q %s"
        else:
            command = "ping -n -i 0.2 -c 3 -W 1 -q %s"
        ret = subprocess.call(command % pIpAddress, shell=True, stdout=open(os.devnull, 'w'), stderr=subprocess.STDOUT)
        if ret == 0:
            return True
        else:
            return False

    def EnableInterfaces(self, pFromIpAddress):
        if self.RemoteOs == OsType.Linux:
            # Get a list of all interfaces
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig -a | grep eth", pFromIpAddress)
            all_ifaces = []
            for line in stdout.split("\n"):
                m = re.search("^(eth\d+)\s+", line)
                if (m):
                    iface = m.group(1)
                    all_ifaces.append(iface)
            # Get a list of 'up' interfaces
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig | grep eth", pFromIpAddress)
            up_ifaces = []
            for line in stdout.split("\n"):
                m = re.search("^(eth\d+)\s+", line)
                if (m):
                    iface = m.group(1)
                    up_ifaces.append(iface)
            # Bring up the interfaces that aren't already
            for iface in all_ifaces:
                if iface not in up_ifaces:
                    self._info("Enabling " + iface)
                    retcode, stdout, stderr = self.ExecuteCommand("ifconfig  " + iface + " up", pFromIpAddress)
                    if retcode != 0: raise ClientError(stderr)

        else:
            self._warn(str(self.RemoteOs) + " client - enable network interfaces not implemented")

    def RebootSoft(self):
        self._info("Sending reboot command")
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("sc config winexesvc start= auto") # make sure winexe service will restart after boot
            retcode, stdout, stderr = self.ExecuteCommand("shutdown /r /f /t 10") # wait 10 sec so winexe can disconnect cleanly
        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("reboot")
            if retcode != 0: raise ClientError("Reboot command failed: " + stderr)
            self.SshSession.close()
            self.SshSession = None
        else:
            retcode, stdout, stderr = self.ExecuteCommand("shutdown -r now")
            if retcode != 0: raise ClientError("Shutdown command failed: " + stderr)
            self.SshSession.close()
            self.SshSession = None
        self._info("Waiting to go down")
        while self.Ping(): pass

    def WaitTillUp(self):
        self._info("Waiting to come up")
        start = time.time()
        responding_ip = self.IpAddress
        # Wait until the client is responding to ping
        while not self.Ping():
            if time.time() - start > 4 * 60:
                # if the client hasn't come back yet, try another IP address
                response = False
                for ip in self.AllIpAddresses:
                    responding_ip = ip;
                    response = self.Ping(ip)
                if response: break
            time.sleep(5)
        # Wait until the client is responding to management requests
        while True:
            try:
                self.ExecuteCommand("hostname", responding_ip)
                break
            except ClientError: time.sleep(5)

        # Make sure all interfaces came back up on Linux
        if self.RemoteOs == OsType.Linux: self.EnableInterfaces(responding_ip)
        self._info("Up and responding")

    def CleanIscsi(self, default_iscsid=True):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --clean")
            if retcode == 0:
                self._passed("Cleaned iSCSI")
            else:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.RemoteOs == OsType.ESX:
            adapter_name = self._get_esx_iscsi_hba()
            self._debug("Unmounting datastores")
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-mpath -l")
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                m = re.search("^iqn", line)
                if m:
                    pieces = line.split(",")
                    new_volume = dict()
                    new_volume["iqn"] = pieces[1]
                m = re.search("Runtime Name: (\S+)", line)
                if m and new_volume:
                    new_volume["runtime"] = m.group(1)
                m = re.search("Device: (\S+)", line)
                if m and new_volume:
                    new_volume["device"] = m.group(1)
                    volumes[m.group(1)] = new_volume
                    new_volume = None
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-scsidevs -m")
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                pieces = re.split("\s+", line)
                device = pieces[0]
                device = device.split(":")[0]
                devfs = pieces[1]
                uuid = pieces[2]
                datastore = pieces[4]
                if device in volumes:
                    volumes[device]["devfs"] = devfs
                    volumes[device]["datastore"] = datastore
                    volumes[device]["uuid"] = uuid
            for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v["iqn"]):
                self._debug(" Unmounting " + volume["datastore"])
                retcode, stdout, stderr = self.ExecuteCommand("esxcli storage filesystem unmount --volume-uuid=" + volume["uuid"])
            self._debug("Removing target portals")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter discovery sendtarget list | grep " + adapter_name)
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                pieces = re.split("\s+", line)
                portal_ip = pieces[1]
                retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter discovery sendtarget remove --address=" + portal_ip + " --adapter=" + adapter_name)
                if retcode != 0: raise ClientError((stdout + stderr).strip())
            self._debug("Resetting auth to default")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter auth chap set --default --adapter=" + adapter_name)
            self._debug("Removing targets")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter target portal list")
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                if not line.startswith(adapter_name): continue
                pieces = re.split("\s+", line)
                iqn = pieces[1]
                portal_ip = pieces[2]
                retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter discovery statictarget remove --adapter=" + adapter_name + " --address=" + portal_ip + " --name=" + iqn)

        elif self.RemoteOs == OsType.Linux:
            self._debug("Logging out of all targets")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -U all")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -o delete")
            retcode, stdout, stderr = self.ExecuteCommand("/etc/init.d/open-iscsi stop")
            time.sleep(3)

            if default_iscsid:
                self._debug("Restoring default iscsid.conf")
                sftp = self.SshSession.open_sftp()
                lib_path = os.path.dirname(os.path.realpath(__file__))
                sftp.put(os.path.join(lib_path, 'iscsid.conf.default.' + self.RemoteOsVersion), '/etc/iscsi/iscsid.conf')
                sftp.close()

            self._debug("Removing persistent configuration")
            retcode, stdout, stderr = self.ExecuteCommand("rm -rf /etc/iscsi/ifaces /etc/iscsi/nodes /etc/iscsi/send_targets")
            retcode, stdout, stderr = self.ExecuteCommand("rm -rf /var/lib/iscsi")
            retcode, stdout, stderr = self.ExecuteCommand("touch /etc/iscsi/iscsi.initramfs")
            retcode, stdout, stderr = self.ExecuteCommand("/etc/init.d/open-iscsi start")
            time.sleep(5)
            self._passed("Cleaned iSCSI")

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets disable")
            if retcode != 0: raise ClientError("Could not disable discovery: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify initiator-node --authentication=NONE")
            if retcode != 0: raise ClientError("Could not disable CHAP: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list discovery-address")
            if retcode != 0:raise ClientError("Could not get discovery address list: " + stderr)
            discovery_ips = []
            for line in stdout.split("\n"):
                m = re.search("Discovery Address: (\S+):", line)
                if m: discovery_ips.append(m.group(1))
            for ip in discovery_ips:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm remove discovery-address " + ip)
                if retcode != 0: raise ClientError("Could not remove discovery address " + ip + ": " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("devfsadm -C")
            if retcode != 0: raise ClientError("Could not clean devfs: " + stderr)
            self._passed("Cleaned iSCSI")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def SetupChap(self, pPortalAddress, pChapUser, pChapSecret):
        self.ChapCredentials[pPortalAddress] = [pChapUser, pChapSecret]
        self._info("Setting up CHAP credentials for portal " + pPortalAddress)

        if self.RemoteOs == OsType.Windows:
            # Make sure the CHAP secret is only alphameric
            # This restriction will exist until I can reliably figure out the multiple levels of shell quoting required
            if not pChapSecret.isalnum():
                raise ClientError("Sorry, CHAP secret must be alphanumeric")
            self._debug("Adding portal " + pPortalAddress + " to initiator")
            cmd = "diskapp.exe --add_portal --portal_address=" + pPortalAddress + " --chap_user=" + pChapUser + " --chap_secret=\"" + self._shell_quote(pChapSecret) + "\""
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode == 0:
                self._passed("Added portal and CHAP credentials")
            else:
                mylog.debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.RemoteOs == OsType.ESX:
            adapter_name = self._get_esx_iscsi_hba()
            self._debug("Setting CHAP credentials")
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter discovery sendtarget auth chap set --direction=uni --level=required --adapter=" + adapter_name + " --address=" + pPortalAddress + " --authname=" + pChapUser + " --secret=" + pChapSecret)
            if retcode != 0: raise ClientError(stderr)

        elif self.RemoteOs == OsType.Linux:
            self._debug("Updating iscsid.conf")

            # Turn on CHAP
            cmd = "sed 's/#*\s*node\.session\.auth\.authmethod\s*=.*/node\.session\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/#*\s*discovery\.sendtargets\.auth\.authmethod\s*=.*/discovery\.sendtargets\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)

            # Set username/password for one-way CHAP
            cmd = "sed 's/#*\s*discovery\.sendtargets\.auth\.username\s*=.*/discovery\.sendtargets\.auth\.username = " + pChapUser + "/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/#*\s*discovery\.sendtargets\.auth\.password\s*=.*/discovery\.sendtargets\.auth\.password = " + pChapSecret + "/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/#*\s*node\.session\.auth\.username\s*=.*/node\.session\.auth\.username = " + pChapUser + "/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/#*\s*node\.session\.auth\.password\s*=.*/node\.session\.auth\.password = " + pChapSecret + "/g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)

            # Disable 2-way CHAP
            cmd = "sed 's/^#*\s*discovery\.sendtargets\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/^#*\s*discovery\.sendtargets\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/^#*\s*node\.session\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)
            cmd = "sed 's/^#*\s*node\.session\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf"
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
            if retcode != 0:
                raise ClientError("Could not edit iscsid.conf - " + stderr)

            self._passed("Set CHAP credentials in iscsid.conf")

        elif self.RemoteOs == OsType.SunOS:
            self._debug("Setting CHAP credentials")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify initiator-node --CHAP-name=" + pChapUser)
            if retcode != 0: raise ClientError("Could not set CHAP user: " + stderr)

            # Use an expect script to set CHAP password because the command won't accept redirection
            sftp = self.SshSession.open_sftp()
            lib_path = os.path.dirname(os.path.realpath(__file__))
            sftp.put(os.path.join(lib_path, "solaris-chapsecret.exp"), "solaris-chapsecret.exp")
            sftp.close()
            retcode, stdout, stderr = self.ExecuteCommand("expect solaris-chapsecret.exp \"" + pChapSecret + "\"")
            if retcode != 0: raise ClientError("Could not set CHAP secret: " + stderr)

            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify initiator-node --authentication=CHAP")
            if retcode != 0: raise ClientError("Could not enable CHAP: " + stderr)

            # Don't bother enabling until we add a portal
            #retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            #if retcode != 0: raise ClientError("Could not enable discovery: " + stderr)
            self._passed("Successfully setup CHAP")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def RefreshTargets(self, pPortalAddress, pExpectedTargetCount=0):
        if self.RemoteOs == OsType.Windows:
            if not self.ChapCredentials.has_key(pPortalAddress):
                raise ClientError("Please setup CHAP for this portal before trying to discover or login")
            chap_user = self.ChapCredentials[pPortalAddress][0]
            chap_secret = self.ChapCredentials[pPortalAddress][1]
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --refresh_targets")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            if pExpectedTargetCount <= 0:
                self._passed("Refreshed all portals")
                return
            targets = self.GetAllTargets()
            if len(targets) < pExpectedTargetCount:
                raise ClientError("Expected " + str(pExpectedTargetCount) + " targets but discovered " + str(len(targets)))
            self._passed("Refreshed all portals")

        elif self.RemoteOs == OsType.Linux:
            self._debug("Refreshing target list on " + pPortalAddress)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m discovery -t sendtargets -p " + pPortalAddress)
            if retcode != 0:
                raise ClientError("Discovery error --\n" + "".join(stderr));
            targets = self.GetAllTargets()
            if len(targets) < pExpectedTargetCount:
                raise ClientError("Expected " + str(pExpectedTargetCount) + " targets but discovered " + str(len(targets)))
            if len(targets) <= 0:
                self._warn("There were no iSCSI targets discovered")
            self._passed("Refreshed portal " + pPortalAddress)

        elif self.RemoteOs == OsType.SunOS:
            # Refresh targets and LoginTargets do the same thing for Solaris
            self._debug("Refreshing target list on " + pPortalAddress)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm add discovery-address " + pPortalAddress)
            if retcode != 0: raise ClientError("Could not add discovery address: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets=disable")
            if retcode != 0: raise ClientError("Could not disable discovery: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            if retcode != 0: raise ClientError("Could not enable discovery: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("devfsadm -C")
            if retcode != 0: raise ClientError("Could not update devfs: " + stderr)
            self._passed("Refreshed all portals")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def _parse_diskapp_error(self, pStdout):
        for line in pStdout.split("\n"):
            m = re.search(": ERROR\s+(.+)", line)
            if m: return m.group(1)
        # If we couldn't find a recognizable error, just return the last line
        return pStdout.split("\n")[1]

    def LoginTargets(self, pPortalAddress, pLoginOrder = "serial", pTargetList = None):
        if self.RemoteOs == OsType.Windows:
            if pTargetList != None and len(pTargetList) > 0:
                raise ClientError("target_list is not implemented for Windows")
            if not self.ChapCredentials.has_key(pPortalAddress):
                raise ClientError("Please setup CHAP for this portal before trying to discover or login")
            chap_user = self.ChapCredentials[pPortalAddress][0]
            chap_secret = self.ChapCredentials[pPortalAddress][1]
            self._info("Logging in to all targets")
<<<<<<< HEAD:libclient.py
            cmd = "diskapp.exe --login_targets --portal_address=" + pPortalAddress + " --chap_user=\"" + self._shell_quote(chap_user) + "\" --chap_secret=\"" + self._shell_quote(chap_secret) + "\""
            retcode, stdout, stderr = self.ExecuteCommand(cmd)
=======
            cmd = "diskapp.exe --login_targets --portal_address=" + pPortalAddress + " --chap_user=\"" + self._shell_quote(chap_user) + "\" --chap_secret=\"" + self._shell_quote(chap_secret) + "\""
            #retcode, stdout, stderr = self.ExecuteCommand(cmd)
            retcode, stdout, stderr = self._execute_winexe_command(self.IpAddress, self.Username, self.Password, cmd, 300)
>>>>>>> 3ba6dc2... Refactor scripts to be importable as modules; various bugfixes and cleanup:lib/libclient.py
            if retcode == 0:
                self._passed("Logged in to all volumes")
            else:
                mylog.debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.RemoteOs == OsType.ESX:
            if pTargetList != None and len(pTargetList) > 0:
                raise ClientError("target_list is not implemented for ESX")
            self._info("Rescanning for iSCSI volumes")
            retcode, stdout, stderr = self.ExecuteCommand("/usr/sbin/esxcfg-swiscsi -s")

        elif self.RemoteOs == OsType.Linux:
            login_count = 0
            error_count = 0
            if pLoginOrder == "parallel":
                if pTargetList != None and len(pTargetList) > 0:
                    raise ClientError("Parallel login with a target_list is not currently implemented")
                self._info("Logging in to all targets in parallel")
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -L all")
                for line in stdout.split("\n"):
                    m = re.search("^Logging in to", line)
                    if (m):
                        continue
                    m = re.search("^Login to .+ target: (.+), portal.+]: (.+)", line)
                    if (m):
                        iqn = m.group(1)
                        status = m.group(2)
                        if (status != "successful"):
                            self._error("Failed to log in to '" + iqn + "'")
                        else:
                            login_count += 1
                if (login_count <= 0):
                    iqn = ""
                    for line in stderr.split("\n"):
                        line = line.strip()
                        m = re.search("Could not login to.+target: (.+), portal", line)
                        if (m):
                            iqn = m.group(1)
                        m = re.search("already exists", line)
                        if (m):
                            self._warn("Session already exists for '" + iqn + "'")
                            login_count += 1
                            continue
                        m = re.search("reported error", line)
                        if (m):
                            self._error("Failed to log in to '" + iqn + "' -- " + line)
                            error_count += 1
            elif pLoginOrder == "serial":
                self._info("Logging in to targets serially")
                targets = self.GetAllTargets()
                if not targets:
                    self._warn("There are no targets to log in to")
                    return
                if pTargetList != None and len(pTargetList) > 0:
                    targets = pTargetList
                self._debug("Found " + str(len(targets)) + " targets to log in to")
                login_count = 0
                error_count = 0
                for target in targets:
                    self._info("Logging in to " + target)
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -l -T " + target)
                    if retcode != 0:
                        if "already exists" in stderr:
                            self._warn("Session already exists for " + target)
                        else:
                            self._error("Failed to log in to " + target)
                            error_count += 1
                    else:
                        login_count += 1

            # Set up automatic login
            all_targets = self.GetAllTargets()
            for target in all_targets:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -o update -n node.startup -v automatic -T " + target)
                if retcode != 0:
                    self._error("Failed to set automatic login on " + target)
                    error_count += 1

            # Wait for SCSI devices for all sessions
            if login_count > 0:
                start_time = time.time()
                while True:
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P3 | egrep 'Target:|scsi disk' | wc -l")
                    stdout = stdout.strip()
                    # This should generate two lines of output for every target
                    # Target: iqn.2010-01.com.solidfire:8m8z.kvm-templates.38537
                    #    Attached scsi disk sdc          State: running
                    # Instead of parsing the output, we'll assume that an even number of lines means there is a device for every session
                    if int(stdout) % 2 == 0:
                        break
                    if time.time() - start_time > 120: # Wait up to 2 minutes
                        raise ClientError("Timeout waiting for all iSCSI sessions to have SCSI devices")
                    time.sleep(1)

            if (login_count > 0):
                self._passed("Successfully logged in to " + str(login_count) + " volumes")
            if (error_count > 0):
                self._error("Failed to login to " + str(error_count) + " volumes")
                raise ClientError("Failed to log in to all volumes")

        elif self.RemoteOs == OsType.SunOS:
            if pTargetList != None and len(pTargetList) > 0:
                raise ClientError("target_list is not implemented for SunOS")
            # Refresh targets and LoginTargets do the same thing for Solaris
            self._debug("Logging in to targets")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm add discovery-address " + pPortalAddress)
            if retcode != 0: raise ClientError("Could not add discovery address: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets=disable")
            if retcode != 0: raise ClientError("Could not disable discovery: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm modify discovery --sendtargets=enable")
            if retcode != 0: raise ClientError("Could not enable discovery: " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("devfsadm -C")
            if retcode != 0: raise ClientError("Could not update devfs: " + stderr)
            self._passed("Logged in to all volumes")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def LogoutTargets(self, pTargetList=None):
        self._info("Logging out of all iSCSI volumes")

        if self.RemoteOs == OsType.Windows:
            if pTargetList != None and len(pTargetList) > 0:
                raise ClientError("target_list is not implemented for Windows")
            retcode, stdout, stdin = self.ExecuteCommand("diskapp.exe --logout_targets --force_unmount --persistent")
            if retcode == 0:
                self._passed("Logged out of all volumes")
            else:
                mylog.debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))

        elif self.RemoteOs == OsType.Linux:
            # Log out of a list of targets
            if pTargetList != None and len(pTargetList) > 0:
                error = False
                for target in pTargetList:
                    retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -u -T")
                    if retcode != 0 and retcode != 21:
                        self._error("Failed to log out of " + target + ":")
                        error = True
                if error:
                    raise ClientError("Failed to log out of all targets")
                self._passed("Logged out of requested volumes")
                return

            # Log out of all volumes
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -U all")
            if retcode == 0 or retcode == 21: # 21 means there were no sessions to log out of
                self._passed("Logged out of all volumes")
                return
            else:
                logout_count = 0
                for line in stdout.split("\n"):
                    m = re.search("^Logging out of", line)
                    if (m):
                        continue
                    m = re.search("^Logout of .+ target: (.+), portal.+]: (.+)", line)
                    if (m):
                        iqn = m.group(1)
                        status = m.group(2)
                        if (status != "successful"):
                            self._error("Failed to log out of '" + iqn + "'")
                        else:
                            logout_count += 1
                error_count = 0
                if (logout_count <= 0):
                    for line in stderr.split("\n"):
                        line = line.strip()
                        iqn = ""
                        m = re.search("Could not logout to.+target: (.+), portal", line)
                        if (m):
                            iqn = m.group(1)
                        m = re.search("reported error", line)
                        if (m):
                            self._error("Failed to log out of '" + iqn + "' -- " + line)
                            error_count += 1
                raise ClientError("Failed to log out of all volumes")

        elif self.RemoteOs == OsType.SunOS:
            if pTargetList != None and len(pTargetList) > 0:
                raise ClientError("target_list is not implemented for SunOS")
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list discovery-address")
            if retcode != 0: raise ClientError("Could not get discovery address list: " + stderr)
            discovery_ips = []
            for line in stdout.split("\n"):
                m = re.search("Discovery Address: (\S+):", line)
                if m: discovery_ips.append(m.group(1))
            for ip in discovery_ips:
                retcode, stdout, stderr = self.ExecuteCommand("iscsiadm remove discovery-address " + ip)
                if retcode != 0: raise ClientError("Could not remove discovery address " + ip + ": " + stderr)
            retcode, stdout, stderr = self.ExecuteCommand("devfsadm -C")
            if retcode != 0: raise ClientError("Could not clean devfs: " + stderr)
            self._passed("Logged out of all volumes")

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetAllTargets(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp --list_targets")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            targets = []
            for line in stdout.split("\n"):
                m = re.search("(iqn\.\S+)", line)
                if m: targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.RemoteOs == OsType.ESX:
            adapter_name = self._get_esx_iscsi_hba()
            retcode, stdout, stderr = self.ExecuteCommand("esxcli iscsi adapter target list --adapter=" + adapter_name + " | grep " + adapter_name)
            for line in stdout.split("\n"):
                pass

        elif self.RemoteOs == OsType.Linux:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m node -P 1 | grep 'Target:'")
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                m = re.search("Target:\s+(.+)", line)
                if (m):
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target")
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.search("Target:\s+(.+)", line)
                if (m):
                    targets.append(m.group(1))
            targets.sort()
            return targets

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetLoggedInTargets(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --list_targets")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            targets = []
            for line in stdout.split("\n"):
                m = re.search("(iqn\..+)\s+\(LOGGED IN\)", line)
                if m:
                    #self._debug(m.group(1))
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.RemoteOs == OsType.Linux:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P 0")
            targets = []
            for line in stdout.split("\n"):
                line = line.strip()
                m = re.search("(iqn\..+)", line)
                if m:
                    targets.append(m.group(1))
            targets.sort()
            return targets

        elif self.RemoteOs == OsType.SunOS:
            return self.GetAllTargets()

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetVdbenchDevices(self):
        if self.RemoteOs == OsType.Windows:
           # Give this a longer than default timeout because it can take a while when there a large number of disks
            retcode, stdout, stderr = self._execute_winexe_command(self.IpAddress, self.Username, self.Password, "diskapp.exe --list_disks", 180)
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            devices = []
            for line in stdout.split("\n"):
                m = re.search("INFO\s+(\S+) => (\S+),", line)
                if m:
                    devices.append(m.group(2))
            return sorted(devices)

        elif self.RemoteOs == OsType.Linux:
            # First look for multipath devices
            retcode, stdout, stderr = self.ExecuteCommand("multipath -l | grep SolidFir | awk '{print $3}' | sort");
            if retcode == 0:
                dev_list = []
                for line in stdout.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    dev_list.append(line)
                mylog.debug(dev_list)
                return sorted(dev_list, key=lambda x: int(re.findall(r'\d+$', x)[0]))

            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P 3 | egrep 'Target:|State:|disk'");
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                m = re.search("Target:\s+(.+)", line)
                if(m):
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search("iSCSI Session State:\s+(.+)", line)
                if(m):
                    new_volume["state"] = m.group(1)
                m = re.search("disk\s+(\S+)\s", line)
                if(m):
                    new_volume["device"] = "/dev/" + m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            devices = []
            devs_by_length = dict()
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                if volume["state"] != "LOGGED_IN":
                    self._warn("Skipping " + volume["iqn"] + " because session state is " + volume["state"])
                    continue
                #self._debug("Adding device " + volume["device"])
                length = str(len(volume["device"]))
                if length not in devs_by_length:
                    #self._debug("Adding len = " + length + " (" + volume["device"] + ")")
                    devs_by_length[length] = []
                devs_by_length[length].append(volume["device"])
                devices.append(volume["device"])
            #self._debug("Sorting")
            sorted_devs = []
            for length in sorted(devs_by_length.keys(), key=int):
                devs_by_length[length].sort()
                sorted_devs += devs_by_length[length]
            #self._debug("Finished sorting")
            #self._debug(",".join(sorted_devs))
            return sorted_devs

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target -S")
            devices = []
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.search("OS Device Name:\s+(.+)", line)
                if (m):
                    devices.append(m.group(1))
            devices.sort()
            return devices

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetVolumeSummary(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --list_disks")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            line_list = []
            for line in stdout.split("\n"):
                m = re.search("INFO\s+(.+)", line)
                if m:
                    line_list.append(m.group(1))
            self._info("Found " + str(len(line_list)) + " iSCSI volumes")
            for line in line_list:
                self._info("    " + line)

        elif self.RemoteOs == OsType.ESX:
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-mpath -l")
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                m = re.search("^iqn", line)
                if m:
                    pieces = line.split(",")
                    new_volume = dict()
                    new_volume["iqn"] = pieces[1]
                m = re.search("Runtime Name: (\S+)", line)
                if m and new_volume:
                    new_volume["runtime"] = m.group(1)
                m = re.search("Device: (\S+)", line)
                if m and new_volume:
                    new_volume["device"] = m.group(1)
                    volumes[m.group(1)] = new_volume
                    new_volume = None
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-scsidevs -m")
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                pieces = re.split("\s+", line)
                device = pieces[0]
                device = device.split(":")[0]
                devfs = pieces[1]
                uuid = pieces[2]
                datastore = pieces[4]
                if device in volumes:
                    volumes[device]["devfs"] = devfs
                    volumes[device]["datastore"] = datastore
            for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v["iqn"]):
                outstr = "    " + volume["iqn"] + " -> " + volume["device"]
                if "datastore" in volume: outstr += " -> " + volume["datastore"]
                self._info(outstr)

        elif self.RemoteOs == OsType.Linux:
            # Sector size for all attached scsi block devices
            retcode, raw_devices, stderr = self.ExecuteCommand("for dev in `ls -d /sys/block/sd*`; do echo \"$dev=`cat $dev/queue/hw_sector_size`\"; done")
            if retcode != 0: raise ClientError("Could not get device list: " + stderr)
            sectors = dict()
            for line in raw_devices.split("\n"):
                if not line.strip(): continue
                pieces = line.split('=')
                dev = pieces[0][11:] # remove /sys/block/ off the front
                size = pieces[1]
                sectors[dev] = size

            retcode, raw_iscsiadm, stderr = self.ExecuteCommand("iscsiadm -m session -P 3 | egrep 'Target:|Portal:|State:|SID:|disk'")
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in raw_iscsiadm.split("\n"):
                m = re.search("Target:\s+(.+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search("Current Portal:\s+(.+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                m = re.search("SID:\s+(.+)", line)
                if m:
                    new_volume["sid"] = m.group(1)
                m = re.search("iSCSI Session State:\s+(.+)", line)
                if m:
                    new_volume["state"] = m.group(1)
                m = re.search("disk\s+(\S+)\s", line)
                if m:
                    new_volume["device"] = "/dev/" + m.group(1)
                    #retcode, stdout, stderr = self.ExecuteCommand("cat /sys/block/" + m.group(1) + "/queue/hw_sector_size")
                    #new_volume["sectors"] = stdout.strip()
                    if m.group(1) in sectors.keys():
                        new_volume["sectors"] = sectors[m.group(1)]
                    else:
                        new_volume["sectors"] = 0
                    volumes[new_volume["device"]] = new_volume
            return volumes

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target -v")
            if retcode != 0: raise ClientError("Could not get target list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.search("Target:\s+(.+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search("IP address \(Peer\):\s+(\S+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target -S")
            if retcode != 0: raise ClientError("Could not get target list: " + stderr)
            current_target = None
            for line in stdout.split("\n"):
                m = re.search("Target:\s+(.+)", line)
                if m:
                    current_target = m.group(1)
                m = re.search("OS Device Name:\s+(.+)", line)
                if m:
                    volumes[current_target]["device"] = m.group(1)
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                self._info("    " + volume["iqn"] + " -> " + volume["device"] + ", Portal: " + volume["portal"])

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def ListVolumes(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --list_disks")
            if retcode != 0:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
            line_list = []
            for line in stdout.split("\n"):
                m = re.search("INFO\s+(.+)", line)
                if m:
                    line_list.append(m.group(1))
            self._info("Found " + str(len(line_list)) + " iSCSI volumes")
            for line in line_list:
                self._info("    " + line)

        elif self.RemoteOs == OsType.ESX:
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-mpath -l")
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                m = re.search("^iqn", line)
                if m:
                    pieces = line.split(",")
                    new_volume = dict()
                    new_volume["iqn"] = pieces[1]
                m = re.search("Runtime Name: (\S+)", line)
                if m and new_volume:
                    new_volume["runtime"] = m.group(1)
                m = re.search("Device: (\S+)", line)
                if m and new_volume:
                    new_volume["device"] = m.group(1)
                    volumes[m.group(1)] = new_volume
                    new_volume = None
            retcode, stdout, stderr = self.ExecuteCommand("esxcfg-scsidevs -m")
            if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                pieces = re.split("\s+", line)
                device = pieces[0]
                device = device.split(":")[0]
                devfs = pieces[1]
                uuid = pieces[2]
                datastore = pieces[4]
                if device in volumes:
                    volumes[device]["devfs"] = devfs
                    volumes[device]["datastore"] = datastore
            for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v["iqn"]):
                outstr = "    " + volume["iqn"] + " -> " + volume["device"]
                if "datastore" in volume: outstr += " -> " + volume["datastore"]
                self._info(outstr)

        elif self.RemoteOs == OsType.Linux:
            #retcode, stdout, stderr = self.ExecuteCommand("iscsiadm -m session -P 3")
            #if retcode != 0: raise ClientError("Could not get volume list: " + stderr)
            #new_volume = None
            #volumes = dict()
            #for line in stdout.split("\n"):
            #    m = re.search("Target:\s+(.+)", line)
            #    if m:
            #        new_volume = dict()
            #        new_volume["iqn"] = m.group(1)
            #    m = re.search("Current Portal:\s+(.+):", line)
            #    if m:
            #        new_volume["portal"] = m.group(1)
            #    m = re.search("SID:\s+(.+)", line)
            #    if m:
            #        new_volume["sid"] = m.group(1)
            #    m = re.search("iSCSI Session State:\s+(.+)", line)
            #    if m:
            #        new_volume["state"] = m.group(1)
            #    m = re.search("disk\s+(\S+)\s", line)
            #    if m:
            #        new_volume["device"] = "/dev/" + m.group(1)
            #        retcode, stdout, stderr = self.ExecuteCommand("cat /sys/block/" + new_volume["device"] + "/queue/hw_sector_size")
            #        new_volume["sectors"] = stdout.strip()
            #        volumes[new_volume["device"]] = new_volume

            volumes = self.GetVolumeSummary()
            sort = "iqn" # or device, portal, state
            self._info("Found " + str(len(volumes.keys())) + " iSCSI volumes")
            for device, volume in sorted(volumes.iteritems(), key=lambda (k,v): v[sort]):
                outstr = "    " + volume["iqn"] + " -> " + volume["device"] + ", SID: " + volume["sid"] + ", SectorSize: " + volume["sectors"] + ", Portal: " + volume["portal"]
                if "state" in volume:
                    outstr += ", Session: " + volume["state"]
                self._info(outstr)

        elif self.RemoteOs == OsType.SunOS:
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target -v")
            if retcode != 0: raise ClientError("Could not get target list: " + stderr)
            new_volume = None
            volumes = dict()
            for line in stdout.split("\n"):
                line = line.strip()
                if not line: continue
                m = re.search("Target:\s+(.+)", line)
                if m:
                    new_volume = dict()
                    new_volume["iqn"] = m.group(1)
                m = re.search("IP address \(Peer\):\s+(\S+):", line)
                if m:
                    new_volume["portal"] = m.group(1)
                    volumes[new_volume["iqn"]] = new_volume
            retcode, stdout, stderr = self.ExecuteCommand("iscsiadm list target -S")
            if retcode != 0: raise ClientError("Could not get target list: " + stderr)
            current_target = None
            for line in stdout.split("\n"):
                m = re.search("Target:\s+(.+)", line)
                if m:
                    current_target = m.group(1)
                m = re.search("OS Device Name:\s+(.+)", line)
                if m:
                    volumes[current_target]["device"] = m.group(1)
            for iqn in sorted(volumes.keys()):
                volume = volumes[iqn]
                self._info("    " + volume["iqn"] + " -> " + volume["device"] + ", Portal: " + volume["portal"])

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def SetupVolumes(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("diskapp.exe --setup_disks --force_mountpoints --relabel")
            if retcode == 0:
                self._passed("Setup all disks")
            else:
                self._debug(stdout)
                raise ClientError(self._parse_diskapp_error(stdout))
        else:
            raise ClientError("Sorry, not implemented yet for " + self.RemoteOs)

    def KernelPanic(self):
        if self.RemoteOs == OsType.Linux:
            self.ExecuteCommand("echo \"Kernel panicking in 5 seconds\" > panic.sh")
            self.ExecuteCommand("echo \"sleep 5\" >> panic.sh")
            self.ExecuteCommand("echo \"echo c > /proc/sysrq-trigger\" >> panic.sh")
            self.ExecuteCommand("sync")
            self.ExecuteCommand("nohup bash panic.sh &")
            self._debug("Disconnecting SSH")
            self.SshSession.close()
            self.SshSession = None
            self._debug("Waiting to go down")
            while self.Ping(): pass

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetOsVersion(self):
        if self.RemoteOs == OsType.Windows:
            retcode, stdout, stderr = self.ExecuteCommand("wmic os get caption,servicepackmajorversion /format:csv")
            if retcode != 0: raise ClientError("Could not get OS info: " + stderr)
            for line in stdout.split("\n"):
                line = line.strip()
                if len(line) <= 0: continue
                if not line.lower().startswith(self.Hostname.lower()[:15]): continue
                pieces = line.split(",")
                osver = pieces[1].strip()
                if int(pieces[2]) > 0:
                    osver += " SP" + pieces[2]
                return osver
            raise ClientError("Could not find OS info")

        elif self.RemoteOs == OsType.Linux:
            if self.RemoteOsVersion == "ubuntu":
                retcode, stdout, stderr = self.ExecuteCommand("lsb_release -d")
                if retcode != 0: raise ClientError("Could not determine release: " + stderr)
                m = re.search("Description:\s+(.+)", stdout)
                if m:
                    return m.group(1).strip()
            elif self.RemoteOsVersion == "redhat":
                retcode, stdout, stderr = self.ExecuteCommand("cat /etc/redhat-release")
                if retcode != 0: raise ClientError("Could not determine release: " + stderr)
                return stdout.strip()

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def GetDhcpEnabled(self, InterfaceName=None, InterfaceMac=None):
        if not InterfaceName and not InterfaceMac:
            raise ClientError("Please specify interface name or interface MAC")
        interface_mac = InterfaceMac
        if interface_mac:
            interface_mac = interface_mac.replace(":", "")
            interface_mac = interface_mac.replace("-", "")
            interface_mac = interface_mac.lower()

        if self.RemoteOs == OsType.Linux:
            self._debug("Searching for network interfaces")
            retcode, stdout, stderr = self.ExecuteCommand("ifconfig -a")
            if retcode != 0: raise ClientError("Could not get interface list: " + stderr)
            ifaces = dict()
            for line in stdout.split("\n"):
                m = re.search("^(\S+)\s+.+HWaddr (\S+)", line)
                if m:
                    iface_name = m.group(1)
                    mac = m.group(2)
                    ifaces[iface_name] = dict()
                    ifaces[iface_name]["name"] = iface_name
                    ifaces[iface_name]["mac"] = mac.lower().replace(":", "")
                    continue
                m = re.search("inet addr:(\S+)", line)
                if m:
                    ip = m.group(1)
                    if ip == "127.0.0.1": continue
                    ifaces[iface_name]["ip"] = ip

            for name in ifaces.keys():
                output = []
                for k,v in ifaces[name].iteritems():
                    output.append(str(k) + "=" + str(v))
                self._debug(",".join(output))

            interface_name = None
            if InterfaceMac:
                for iface in ifaces.keys():
                    if ifaces[iface]["mac"] == interface_mac:
                        interface_name = iface
                if not interface_name: raise ClientError("Could not find interface with MAC '" + InterfaceMac + "'")
            if InterfaceName:
                if InterfaceName not in ifaces.keys():
                    raise ClientError("Could not find interface '" + InterfaceName + "'")
                interface_name = InterfaceName

            if self.RemoteOsVersion == "ubuntu":
                retcode, stdout, stderr = self.ExecuteCommand("egrep -c -i \"" + interface_name + ".+dhcp\" /etc/network/interfaces")
                if retcode != 0 and retcode != 1: raise ClientError("Could not search interfaces file: " + stderr)
                if stdout.strip() == "1":
                    return True
                else:
                    return False

            elif self.RemoteOsVersion == "redhat":
                retcode, stdout, stderr = self.ExecuteCommand("egrep -c -i dhcp /etc/sysconfig/network-scripts/ifcfg-" + interface_name)
                if retcode != 0 and retcode != 1: raise ClientError("Could not search interfaces file: " + stderr)
                if stdout.strip() == "1":
                    return True
                else:
                    return False
            else:
                raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOsVersion))

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def IsHealthy(self):
        if self.RemoteOs == OsType.Linux:
            #self._step("Checking health")

            # alphabetically first MAC
            return_code, stdout, stderr = self.ExecuteCommand("ifconfig | grep HWaddr | awk '{print $5}' | sed 's/://g' | sort | head -1")
            unique_id = stdout.strip()

            # Get uptime
            return_code, stdout, stderr = self.ExecuteCommand("cat /proc/uptime | awk '{print $1}'")
            uptime = stdout.strip()

            # Check memory usage
            return_code, stdout, stderr = self.ExecuteCommand("cat /proc/meminfo")
            mem_total = 0
            mem_free = 0
            mem_buff = 0
            mem_cache = 0
            for line in stdout.split("\n"):
                m = re.search("MemTotal:\s+(\d+) kB", line)
                if m:
                    mem_total = float(m.group(1))
                    continue
                m = re.search("MemFree:\s+(\d+) kB", line)
                if m:
                    mem_free = float(m.group(1))
                    continue
                m = re.search("Buffers:\s+(\d+) kB", line)
                if m:
                    mem_buff = float(m.group(1))
                    continue
                m = re.search("Cached:\s+(\d+) kB", line)
                if m:
                    mem_cache = float(m.group(1))
                    continue
            mem_usage = 0
            if mem_total > 0:
                mem_usage = "%.1f" % (100 - ((mem_free + mem_buff + mem_cache) * 100) / mem_total)

            # Check CPU usage
            cpu_usage = "-1";
            try:
                return_code, stdout, stderr = self.ExecuteCommand("top -b -d 1 -n 2 | grep Cpu | tail -1")
                m = re.search("(\d+\.\d+)%id", stdout)
                if (m):
                        cpu_usage = "%.1f" % (100.0 - float(m.group(1)))
            except ValueError: pass

            # Check if vdbench is running here
            return_code, stdout, stderr = self.ExecuteCommand("ps -ef | grep -v grep | grep java | grep vdbench | wc -l")
            vdbench_count = 0
            try: vdbench_count = int(stdout.strip())
            except ValueError: pass

            # See if vdbenchd is in use
            return_code, stdout, stderr = self.ExecuteCommand("if [ -f /opt/vdbench/last_vdbench_pid ]; then echo 'True'; else echo 'False'; fi")
            vdbenchd = bool(stdout.strip())

            # See if we have a vdbench last exit status
            vdbench_exit = -1
            return_code, stdout, stderr = self.ExecuteCommand("cat /opt/vdbench/last_vdbench_exit")
            try: vdbench_exit = int(stdout.strip())
            except ValueError:pass

            self._step("Checking health")
            self._info("Hostname " + self.Hostname + " MAC " + unique_id)
            self._info("Uptime " + str(uptime))

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
                self._info("CPU usage " + str(cpu_usage) + "%")
            if mem_usage > 0:
                self._info("Mem usage " + str(mem_usage) + "%")

            if healthy:
                self._passed("Client is healthy")
            else:
                self._error("Client is not healthy")

            return healthy

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))

    def IsHealthySilent(self):
        if self.RemoteOs == OsType.Linux:

            # Check if vdbench is running here
            return_code, stdout, stderr = self.ExecuteCommand("ps -ef | grep -v grep | grep java | grep vdbench | wc -l")
            vdbench_count = 0
            try: vdbench_count = int(stdout.strip())
            except ValueError: pass

            # See if vdbenchd is in use
            return_code, stdout, stderr = self.ExecuteCommand("if [ -f /opt/vdbench/last_vdbench_pid ]; then echo 'True'; else echo 'False'; fi")
            vdbenchd = bool(stdout.strip())

            # See if we have a vdbench last exit status
            vdbench_exit = -1
            return_code, stdout, stderr = self.ExecuteCommand("cat /opt/vdbench/last_vdbench_exit")
            try: vdbench_exit = int(stdout.strip())
            except ValueError:pass

            # Use vdbench status to determine health
            healthy = True
            if vdbench_count > 0:
                pass
            elif not vdbenchd and vdbench_count <= 0:
                healthy = False
            elif vdbenchd and vdbench_exit == 0:
                pass
            else:
                healthy = False

            return healthy

        else:
            raise ClientError("Sorry, this is not implemented for " + str(self.RemoteOs))
