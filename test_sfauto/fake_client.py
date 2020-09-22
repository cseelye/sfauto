#!/usr/bin/env python2.7
#pylint: skip-file

from .testutil import RandomString, RandomIP, RandomSequence
from . import globalconfig
from libsf import shellutil, netutil
from libsf import ClientError, ClientRefusedError, SolidFireError
from libsf.logutil import GetLogger
import copy
import random
import re
import string
import threading

def FakeShellCommand(command, timeout=1800):
    """Intercept local shell commands to inject responses"""

    # Fake out winexe commands
    if "winexe" in command:
        return FakeWinexeCommand(command)

    # Fake out ping commands
    elif command.startswith("ping"):
        return FakeShellPing(command)

    # Fake out ipmi commands
    elif command.startswith("ipmitool"):
        return FakeIPMICommand(command)

    # Any other command, run it like usual
    else:
        return shellutil.Shell_original(command, timeout)

def FakeWinexeCommand(command):
    # TODO: implement fake Windows command responses

    # Return a refused error, as if this is not a Windows client
    return 1, "NT_STATUS_CONNECTION_REFUSED", ""

def FakeShellPing(command):
    GetLogger().fake("Running local command=[{}]".format(command))
    result = random.choice([0, 1])
    return result, "", ""

def FakeIPMICommand(command):
    GetLogger().fake("Running local command=[{}]".format(command))
    result = globalconfig.clients.GetClientCommandFailure(command, CLIENT_ALL)
    if result:
        return result

    # Return a random power state if asked for
    if "chassis power status" in command:
        return 0, "Chassis Power is " + random.choice(["on", "off"]), ""

    return 0, "Fake unit test success", ""

#=======================================================================
# This set of classes is to fake out a paramiko SSHClient and related machinery

class FakeParamikoTransport(object):
    
    def is_active(self):
        return True

class FakeParamikoChannel(object):

    def __init__(self, returnCode):
        self.returnCode = returnCode

    def recv_exit_status(self):
        return self.returnCode

class FakeParamikoStream(object):

    def __init__(self, data):
        self.data = ["{}\n".format(line) for line in data.split("\n") if len(line) > 0]

    def readlines(self):
        return self.data

class FakeParamikoSFTP(object):
    
    def put(self, localPath, remotePath):
        pass
    
    def close(self):
        pass

class FakeParamikoSSHClient(object):

    def __init__(self):
        self.ip = None
        self.responses = {}

    def set_missing_host_key_policy(self, *args, **kwargs):
        pass

    def load_system_host_keys(self, *args, **kwargs):
        pass

    def connect(self, ipAddress, **kwargs):

        ex = globalconfig.clients.GetClientConnectError(ipAddress)
        if ex:
            raise ex

        self.ip = ipAddress

    def close(self):
        pass

    def get_transport(self):
        return FakeParamikoTransport()

    def open_sftp(self):
        return FakeParamikoSFTP()

    def exec_command(self, command):

        client = globalconfig.clients.GetClient(self.ip)
        retcode, stdout_data, stderr_data = client.ExecuteCommand(command)

        # Put the result in something that looks like a paramiko result
        stderr = FakeParamikoStream(stderr_data)
        stdout = FakeParamikoStream(stdout_data)
        stdout.channel = FakeParamikoChannel(retcode)
        return None, stdout, stderr

#=======================================================================


CLIENTS_PATH = "clients"
CLIENT_COMMAND_ERROR_PATH = "commanderrors"
CLIENT_CONNECT_ERROR_PATH = "connecterrors"
CLIENT_ALL = "all"


#=======================================================================
# Context managers for creating failures in UTs

class ClientConnectFailure(object):

    def __init__(self, clientIP=CLIENT_ALL):
        self.clientIP = clientIP

    def __enter__(self):
        globalconfig.clients.AddClientConnectError(self.clientIP)

    def __exit__(self, ex_type, ex_value, traceback):
        globalconfig.clients.RemoveClientConnectError(self.clientIP)

class ClientCommandFailure(object):

    def __init__(self, command, result=None, clientIP=CLIENT_ALL):
        self.command = command
        self.result = result or (1, "", "Fake unit test failure")
        self.clientIP = clientIP

    def __enter__(self):
        globalconfig.clients.AddClientCommandFailure(self.command, self.result, self.clientIP)

    def __exit__(self, ex_type, ex_value, traceback):
        globalconfig.clients.RemoveClientCommandFailure(self.command, self.clientIP)

#=======================================================================


class FakeClientRegister(object):
    """Generate and manage fake clients"""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.clients = {}
        self.connectErrors = {}
        self.commandErrors = {}

    def GetClientIPs(self):
        with self.lock:
            return copy.deepcopy(self.clients.keys())

    def CreateClient(self, hostname=None, ipAddress=None, osType=None, osDistro=None):
        with self.lock:
            return self._CreateClientUnlocked(hostname=hostname, ipAddress=ipAddress, osType=osType, osDistro=osDistro)

    def _CreateClientUnlocked(self, hostname=None, ipAddress=None, osType=None, osDistro=None):
        hostname = hostname or RandomString(16)
        ipAddress = ipAddress or RandomIP()
        osType = osType or "linux"
        osDistro = osDistro or "ubuntu"

        new_client = FakeClient(ipAddress, hostname, osType, osDistro)
        self.clients[ipAddress] = new_client
        return copy.deepcopy(new_client)

    def GetClient(self, ipAddress, createIfMissing=True):
        with self.lock:
            if ipAddress in self.clients:
                return copy.deepcopy(self.clients[ipAddress])
            if createIfMissing:
                return self._CreateClientUnlocked(ipAddress=ipAddress)
            else:
                raise ClientError("Could not find client {}".format(ipAddress))

    def UpdateClient(self, ipAddress, client):
        with self.lock:
            if ipAddress in self.clients:
                self.clients[ipAddress] = copy.deepcopy(client)
            else:
                raise ClientError("Could not find client {}".format(ipAddress))

    def AddClientConnectError(self, clientIP=CLIENT_ALL, exception=None):
        exception = exception or SolidFireError("SSH error: Fake unit test failure")
        with self.lock:
            self.connectErrors[clientIP] = exception

    def RemoveClientConnectError(self, clientIP=CLIENT_ALL):
        with self.lock:
            self.connectErrors.pop(clientIP, None)

    def GetClientConnectError(self, clientIP):
        with self.lock:
            ex = self.connectErrors.get(CLIENT_ALL, None) or self.connectErrors.get(clientIP, None)
            return ex

    def AddClientCommandFailure(self, command, result, clientIP=CLIENT_ALL):
        with self.lock:
            if not clientIP in self.commandErrors:
                self.commandErrors[clientIP] = {}
            self.commandErrors[clientIP][command] = result

    def RemoveClientCommandFailure(self, command, clientIP=CLIENT_ALL):
        with self.lock:
            if not self.commandErrors[clientIP]:
                return
            self.commandErrors[clientIP].pop(command, None)

    def GetClientCommandFailure(self, command, clientIP):
        with self.lock:
            keys = [CLIENT_ALL, clientIP]
            for key in keys:
                if key in self.commandErrors.keys():
                    for comm in self.commandErrors[key].keys():
                        if comm == command:
                            return self.commandErrors[key][comm]
                    for comm in self.commandErrors[key].keys():
                        if command.startswith(comm):
                            return self.commandErrors[key][comm]
            return None

#=======================================================================


def _nextDiskName(diskName):
    m = re.search("sd([a-z]+)", diskName)
    if not m:
        return "sda"

    disk_index = m.group(1)
    pieces = list(disk_index)
    if pieces[-1] == "z":
        return _nextDiskName("sd" + "".join(pieces[:-1])) + "a"

    idx = string.ascii_lowercase.find(pieces[-1])
    return "sd" + "".join(pieces[:-1]) + string.ascii_lowercase[idx + 1]

class FakeClient(object):
    
    def __init__(self, ipAddress, hostname, osType, osDistro):
        self.ip = ipAddress
        self.hostname = hostname
        self.type = osType
        self.distro = osDistro

        self.volumes = {}
        self.portal = "0.0.0.0"
        self.fakeDiscovery = None
        self.iqn = "iqn.2010-01.net.solidfire.eng:{}".format(self.hostname.lower())
        self.chapUsername = None
        self.chapPassword = None
        self.discoveredTargets = []

        # Default Ubuntu responses
        self.responses = {
            "hostname"                                                          : \
                (0, "{}\n".format(self.hostname), ""),
            "uname -a"                                                          : \
                (0, "Linux {} 3.13.0-65-generic #105~precise1-Ubuntu SMP Tue Sep 22 13:22:42 UTC 2015 x86_64 x86_64 x86_64 GNU/Linux\n".format(self.hostname), ""),
            "ifconfig | grep 'inet '"                                           : \
                (0, "          inet addr:10.99.75.100  Bcast:10.99.95.255  Mask:255.255.224.0\n          inet addr:{}  Bcast:172.30.95.255  Mask:255.255.224.0\n          inet addr:127.0.0.1  Mask:255.0.0.0\n".format(self.ip), ""),
            "cat /etc/iscsi/initiatorname.iscsi | grep -v '#' | cut -d'=' -f2"  : \
                (0, self.iqn, ""),
            "( [[ -e /etc/open-iscsi/initiatorname.iscsi ]] && cat /etc/open-iscsi/initiatorname.iscsi || cat /etc/iscsi/initiatorname.iscsi ) | grep -v '#' | cut -d'=' -f2" : \
                (0, self.iqn, ""),
            "cat /sys/class/fc_host/*/port_name"                                : \
                (1, "", "cat: /sys/class/fc_host/*/port_name: No such file or directory\n"),
            r"sed 's/#*\s*node\.session\.auth\.authmethod\s*=.*/node\.session\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            r"sed 's/#*\s*discovery\.sendtargets\.auth\.authmethod\s*=.*/discovery\.sendtargets\.auth\.authmethod = CHAP/g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            r"sed 's/#*\s*discovery\.sendtargets\.auth\.username\s*=.*/discovery\.sendtargets\.auth\.username = ": \
                self.set_chap_username,
            r"sed 's/#*\s*discovery\.sendtargets\.auth\.password\s*=.*/discovery\.sendtargets\.auth\.password = ": \
                self.set_chap_password,
            r"sed 's/#*\s*node\.session\.auth\.username\s*=.*/node\.session\.auth\.username = " : \
                (0, "", ""),
            r"sed 's/#*\s*node\.session\.auth\.password\s*=.*/node\.session\.auth\.password = " : \
                (0, "", ""),
            r"sed 's/^#*\s*discovery\.sendtargets\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            r"sed 's/^#*\s*discovery\.sendtargets\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            r"sed 's/^#*\s*node\.session\.auth\.username_in\s*=.*/#node\.session\.auth\.username_in = /g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            r"sed 's/^#*\s*node\.session\.auth\.password_in\s*=.*/#node\.session\.auth\.password_in = /g' -i /etc/iscsi/iscsid.conf": \
                (0, "", ""),
            "iscsiadm -m session -P 0" : self.iscsiadm_get_sessions,
            "iscsiadm -m session -P 3 | egrep 'Target:|Portal:|State:|SID:|disk'": self.iscsiadm_get_session_details,
            "for dev in `ls -d /sys/block/sd*`; do echo \"$dev=`cat $dev/queue/hw_sector_size`\"; done" : self.get_devices_blocksize,
            "iscsiadm -m node -U all": self.iscsiadm_logout_all,
            "iscsiadm -m session -o delete":
                (21, "", "iscsiadm: No active sessions."),
            "/etc/init.d/*open-iscsi stop":
                (0, " * Unmounting iscsi-backed filesystems\n...done.\n * Disconnecting iSCSI targets\niscsiadm: No matching sessions found\n    ...done.\n * Stopping iSCSI initiator service\n   ...done.", ""),
            "systemctl stop iscsid":
                (0, " * Unmounting iscsi-backed filesystems\n...done.\n * Disconnecting iSCSI targets\niscsiadm: No matching sessions found\n    ...done.\n * Stopping iSCSI initiator service\n   ...done.", ""),
            "killall -9 iscsid":
                (0, "", ""),
            "rm -rf /etc/iscsi/ifaces /etc/iscsi/nodes /etc/iscsi/send_targets":
                (0, "", ""),
            "rm -rf /var/lib/iscsi":
                (0, "", ""),
            "touch /etc/iscsi/iscsi.initramfs":
                (0, "", ""),
            "/etc/init.d/*open-iscsi start":
                (0, " * Starting iSCSI initiator service iscsid\n   ...done.\n * Setting up iSCSI targets\n   ...done.\n * Mounting network filesystems\n   ...done.", ""),
            "systemctl start iscsid":
                (0, " * Starting iSCSI initiator service iscsid\n   ...done.\n * Setting up iSCSI targets\n   ...done.\n * Mounting network filesystems\n   ...done.", ""),
            "iscsiadm -m node -o update -n node.startup -v automatic -T":
                (0, "", ""),
            "iscsiadm -m discovery -t sendtargets -p": self.iscsiadm_discovery,
            "iscsiadm -m node -l -T ": self.iscsiadm_login_target,
            "iscsiadm -m node -P 1 | grep 'Target:'": self.iscsiadm_list_targets,
            "iscsiadm -m session -P3 | egrep 'Target:|scsi disk' | wc -l": self.get_session_count,
            "iscsiadm -m node -L all" : self.iscsiadm_login_all,
            "mkdir -p /mnt/" :
                (0, "", ""),
            "parted":
                (0, "", ""),
            "mkfs":
                (0, "", ""),
            "mount":
                (0, "", ""),
        }

        if self.distro == "ubuntu":
            pass

        elif self.distro == "element":
            self.responses["uname -a"] = (0, "Linux {} 3.8.0-28-solidfire-element-p205 #41~precise1 SMP Mon Dec 7 17:11:49 UTC 2015 x86_64 x86_64 x86_64 GNU/Linux".format(self.hostname), "")
            self.responses["ifconfig | grep 'inet '"] = (0, "          inet addr:10.99.75.100  Bcast:10.99.95.255  Mask:255.255.224.0\n          inet addr:{}  Bcast:172.30.95.255  Mask:255.255.224.0\n          inet addr:127.0.0.1  Mask:255.0.0.0\n".format(self.ip), "")

        else:
            raise NotImplementedError("{} distro has not been faked".format(self.distro))

    def GetNextDiskName(self):
        if not self.volumes:
            return "sdb"
        longest = max([len(disk) for disk in self.volumes.keys()])
        names = sorted([disk for disk in self.volumes.keys() if len(disk) == longest])
        return _nextDiskName(names[-1])

    def SetNoVolumes(self):
        self.fakeDiscovery = None

    def SetClientConnectedVolumes(self, volumeCount):
        """
        Make the state of this client look like it has connected iSCSI volumes

        Args:
            volumeCount:    connect this many fake volumes
        """

        self.SetNoVolumes()
        self.fakeDiscovery = []

        if volumeCount > 0:
            self.portal = RandomIP()
            volume_ids = RandomSequence(volumeCount)
            cluster_id = RandomString(4).lower()
            for idx in xrange(1, volumeCount+1):
                iqn = "iqn.2010-01.com.solidfire:{}.v-{:0>5d}.{}".format(cluster_id, volume_ids[idx-1], volume_ids[idx-1])

                device = self.GetNextDiskName()
                self.volumes[device] = iqn
                self.fakeDiscovery.append(iqn)

        globalconfig.clients.UpdateClient(self.ip, self)

    def SetClientDiscoverableVolumes(self, volumeCount):
        """
        Make the state of this client look like it has discover-able/login-able volumes

        Args:
            volumeCount:    number of fake volumes
        """
        self.SetNoVolumes()
        self.fakeDiscovery = []
        volume_ids = RandomSequence(volumeCount)
        cluster_id = RandomString(4).lower()
        if volumeCount > 0:
            for idx in xrange(1, volumeCount+1):
                iqn = "iqn.2010-01.com.solidfire:{}.v-{:0>5d}.{}".format(cluster_id, volume_ids[idx-1], volume_ids[idx-1])
                self.fakeDiscovery.append(iqn)
        globalconfig.clients.UpdateClient(self.ip, self)

    def ExecuteCommand(self, command):
        """
        Pretend to execute a command on this fake client

        Args:
            command:    the command to execute

        Returns:
            A tuple of return code, stdout, stderr from the command
        """

        # Strip off the prefix that libsf SSHConnection.RunCommand adds
        command_prefix = "set -o pipefail; "
        if command.startswith(command_prefix):
            command = command[len(command_prefix):]

        # See if this command result has been overridden
        result = globalconfig.clients.GetClientCommandFailure(command, self.ip)

        # Look up the command result
        if not result:
            # Look for an exact match
            result = self.responses.get(command, None)

            # Look for a partial "startswith" match
            if not result:
                for comm in self.responses.keys():
                    if command.startswith(comm):
                        result = self.responses[comm]

        if not result:
            raise NotImplementedError("[{}] command has not been faked".format(command))

        if callable(result):
            return result(command)
        else:
            return result

    def set_chap_username(self, command):
        m = re.search(r"sed 's/#\*\\s\*discovery\\\.sendtargets\\\.auth\\\.username\\s\*=\.\*/discovery\\\.sendtargets\\\.auth\\\.username = (\S+)/g", command)
        print "CALL TO SET CHAP USERNAME"
        if m:
            print "SETTING CHAP USERNAME"
            self.chapUsername = m.group(1)
            globalconfig.clients.UpdateClient(self.ip, self)
        return (0, "", "")

    def set_chap_password(self, command):
        m = re.search(r"sed 's/#\*\\s\*discovery\\\.sendtargets\\\.auth\\\.password\\s\*=\.\*/discovery\\\.sendtargets\\\.auth\\\.password = (\S+)/g", command)
        if m:
            self.chapPassword = m.group(1)
            globalconfig.clients.UpdateClient(self.ip, self)
        return (0, "", "")

    def iscsiadm_discovery(self, command):
        m = re.search("iscsiadm -m discovery -t sendtargets -p (\S+)", command)
        if m:
            self.portal = m.group(1)
            self.discoveredTargets = []

            cluster_info = globalconfig.cluster.GetClusterInfo({})
            if self.fakeDiscovery != None:
                if len(self.fakeDiscovery) == 0:
                    return (0, "iscsiadm: No portals found", "")
                self.discoveredTargets = copy.deepcopy(self.fakeDiscovery)
            elif self.portal == cluster_info["clusterInfo"]["svip"]:
                # Discover from the cluster
                iqns = globalconfig.cluster.GetClientVisibleVolumes(self.iqn, self.chapUsername, self.chapPassword)
                self.discoveredTargets = copy.deepcopy(iqns)
            else:
                return (4,
                        "",
                        "iscsiadm: connect to {} timed out\niscsiadm: connection login retries (reopen_max) 5 exceeded\niscsiadm: Could not perform SendTargets discovery: encountered connection failure".format(self.portal))

            globalconfig.clients.UpdateClient(self.ip, self)
            return (0,
                    "\n".join(["{}:3260,1 {}".format(self.portal, iqn) for iqn in self.discoveredTargets]),
                    "")

    def iscsiadm_list_targets(self, command):
        # iscsiadm -m node -P 1 | grep 'Target:'
        cluster_info = globalconfig.cluster.GetClusterInfo({})
        if self.fakeDiscovery != None:
            if len(self.fakeDiscovery) == 0:
                return (0, "iscsiadm: No portals found", "")
            return (0,
                    "\n".join(["Target: {}".format(iqn) for iqn in self.fakeDiscovery]),
                    "")
        else:
            iqns = globalconfig.cluster.GetClientVisibleVolumes(self.iqn, self.chapUsername, self.chapPassword)
            return (0,
                    "\n".join(["Target: {}".format(iqn) for iqn in iqns]),
                    "")

    def iscsiadm_login_target(self, command):
        m = re.search(r"iscsiadm -m node -l -T (\S+)", command)
        if m:
            target_iqn = m.group(1)
            for iqn in self.volumes.itervalues():
                if iqn == target_iqn:
                    return (0, "", "")

            self.volumes[self.GetNextDiskName()] = target_iqn
            globalconfig.clients.UpdateClient(self.ip, self)
            return (0,
                    "Logging in to [iface: default, target: {}, portal: 10.26.64.70,3260] (multiple)\nLogin to [iface: default, target: {}, portal: 10.26.64.70,3260] successful.".format(target_iqn, target_iqn),
                    "")

    def get_session_count(self, command):
        # iscsiadm -m session -P3 | egrep 'Target:|scsi disk' | wc -l
        return (0,
                str(2 * len(self.volumes)),
                "")

    def iscsiadm_get_session_details(self, command):
        # iscsiadm -m session -P 3 | egrep 'Target:|Portal:|State:|SID:|disk'

        if not self.volumes:
            return (21, "", "iscsiadm: No active sessions.")

        idx = 1
        session_details = []
        for device, iqn in self.volumes.iteritems():
            session_details.append("Target: {}".format(iqn))
            session_details.append("        Current Portal: {}:3260,1".format(self.portal))
            session_details.append("        Persistent Portal: {}:3260,1".format(self.portal))
            session_details.append("                SID: {}".format(idx))
            session_details.append("                iSCSI Connection State: LOGGED IN")
            session_details.append("                iSCSI Session State: LOGGED IN")
            session_details.append("                Internal iscsid Session State: NO CHANGE")
            session_details.append("                Host Number: {}     State: running".format(idx+2))
            session_details.append("                        Attached scsi disk {}       State: running".format(device))
            idx += 1
        return (0,
                "\n".join(session_details),
                "")

    def iscsiadm_get_sessions(self, command):
        # iscsiadm -m session -P 0
        if not self.volumes:
            return (21, "", "iscsiadm: No active sessions.")

        sessions = []
        for idx, iqn in enumerate(self.volumes.values()):
            sessions.append("tcp: [{}] {}:3260,1 {}".format(idx, self.portal, iqn))
        return (0,
                "\n".join(sessions),
                "")

    def get_devices_blocksize(self, command):
        # for dev in `ls -d /sys/block/sd*`; do echo \"$dev=`cat $dev/queue/hw_sector_size`\"; done
        devices = ["/sys/block/sda=512"]
        for device in self.volumes.keys():
            devices.append("/sys/block/{}={}".format(device, random.choice(["512", "4096"])))
        return (0,
                "\n".join(devices),
                "")

    def iscsiadm_logout_all(self, command):
        # iscsiadm -m node -U all
        if not self.volumes:
            return (21, "", "iscsiadm: No matching sessions found")

        logout_details = []
        for idx, iqn in enumerate(self.volumes.values()):
            logout_details.append("Logging out of session [sid: {}, target: {}, portal: {},3260]".format(idx, iqn, self.portal))
            logout_details.append("Logout of [sid: {}, target: {}, portal: {},3260] successful.".format(idx, iqn, self.portal))
        self.volumes = {}
        return (0,
                "\n".join(sorted(logout_details)),
                "")

    def iscsiadm_login_all(self, command):
        # iscsiadm -m node -L all
        login_details = []
        for target_iqn in self.discoveredTargets:
            login_details.append("Logging in to [iface: default, target: {}, portal: {},3260] (multiple)".format(target_iqn, self.portal))
            login_details.append("Login to [iface: default, target: {}, portal: {},3260] successful.".format(target_iqn, self.portal))
            self.volumes[self.GetNextDiskName()] = target_iqn
        globalconfig.clients.UpdateClient(self.ip, self)
        return (0,
                "\n".join(sorted(login_details)),
                "")


