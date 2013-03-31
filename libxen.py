import libsf
from libsf import mylog
import socket
import XenAPI
from xml.etree import ElementTree



# Generic exception for all errors
class XenError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def Connect(VmHost, HostUser, HostPass):
    session = None
    while True:
        try:
            url = "http://" + VmHost
            mylog.debug("Connecting to XenServer at " + url + " as " + HostUser + ":" + HostPass)
            session = XenAPI.Session(url)
            session.xenapi.login_with_password(HostUser, HostPass)
            break
        except socket.error as e:
            if e.errno == 10060:
                raise XenError("Error connecting to " + VmHost + " - server is not responding")
            else:
                raise XenError("Error connecting to " + VmHost + " - " + str(e))
        except XenAPI.Failure as e:
            if e.details[0] == "HOST_IS_SLAVE":
                mylog.debug("Server is not the pool master")
                VmHost = e.details[1]
                continue
            if e.details[0] == "SESSION_AUTHENTICATION_FAILED":
                raise XenError("Error connecting to " + VmHost + " - authentication failure")
            else:
                raise XenError("Error connecting to " + VmHost + " - [" + e.details[0] + "] " + e.details[2] + "(" + e.details[1] + ")")

    return session

def GetIscsiTargets(XenSession, Host, Svip, ChapUser, ChapPass):
    sr_args = {
            "target": Svip,
            "chapuser": ChapUser,
            "chappassword": ChapPass
    }
    sr_type = "lvmoiscsi"
    xml_str = None
    try:
        XenSession.xenapi.SR.probe(Host, sr_args, sr_type)
    except XenAPI.Failure as e:
        if e.details[0] == "SR_BACKEND_FAILURE_96":
            xml_str = e.details[3]
        else:
            raise XenError("Could not discover iSCSI volumes: " + str(e))

    targets_xml = ElementTree.fromstring(xml_str)
    target_list = []
    for node in targets_xml.findall("TGT/TargetIQN"):
        iqn = node.text.strip()
        if iqn != "*":
            target_list.append(iqn)
    return target_list

def GetScsiLun(XenSession, Host, TargetIqn, Svip, ChapUser, ChapPass):
        sr_args = {
                "target": Svip,
                "targetIQN": TargetIqn,
                "chapuser": ChapUser,
                "chappassword": ChapPass
        }
        sr_type = "lvmoiscsi"
        xml_str = None
        try:
            XenSession.xenapi.SR.probe(Host, sr_args, sr_type)
        except XenAPI.Failure as e:
            if e.details[0] == "SR_BACKEND_FAILURE_107":
                xml_str = e.details[3]
            else:
                mylog.error("Could not discover SCSI LUN for target " + TargetIqn + " - " + str(e))
                sys.exit(1)

        lun_xml = ElementTree.fromstring(xml_str)
        scsi_id = None
        node = lun_xml.find("LUN/SCSIid")
        if node != None:
            scsi_id = node.text.strip()
        if not scsi_id:
            mylog.debug("Could not find scsi ID in " + xml_str)
            mylog.error("Could not determine SCSI ID for target " + TargetIqn)
        sr_size = int(lun_xml.find("LUN/size").text.strip())

        return scsi_id, sr_size
