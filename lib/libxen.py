import libsf as libsf
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

def GetIscsiTargets(XenSession, Host, Svip, ChapUser=None, ChapPass=None):
    sr_args = {
            "target": Svip,
    }
    if ChapUser:
        sr_args["chapuser"] = ChapUser
    if ChapPass:
        sr_args["chappassword"] = ChapPass

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

def GetScsiLun(XenSession, Host, TargetIqn, Svip, ChapUser=None, ChapPass=None):
    sr_args = {
            "target": Svip,
            "targetIQN": TargetIqn,
    }
    if ChapUser:
        sr_args["chapuser"] = ChapUser
    if ChapPass:
        sr_args["chappassword"] = ChapPass
    sr_type = "lvmoiscsi"
    xml_str = None
    try:
        XenSession.xenapi.SR.probe(Host, sr_args, sr_type)
    except XenAPI.Failure as e:
        if e.details[0] == "SR_BACKEND_FAILURE_107":
            xml_str = e.details[3]
        else:
            raise XenError("Could not discover SCSI LUN for target " + TargetIqn + " - " + str(e))

    lun_xml = ElementTree.fromstring(xml_str)
    scsi_id = None
    node = lun_xml.find("LUN/SCSIid")
    if node != None:
        scsi_id = node.text.strip()
    if not scsi_id:
        mylog.debug("Could not find scsi ID in " + xml_str)
        raise XenError("Could not determine SCSI ID for target " + TargetIqn)
    sr_size = int(lun_xml.find("LUN/size").text.strip())

    return scsi_id, sr_size

def GetAllVMs(xenSession):
    try:
        vm_ref_list = xenSession.xenapi.VM.get_all()
    except XenAPI.Failure as e:
        raise XenError("Could not get VM list: " + str(e))

    vm_list = dict()
    for vm_ref in vm_ref_list:
        try:
            vm = xenSession.xenapi.VM.get_record(vm_ref)
        except XenAPI.Failure as e:
            raise XenError("Could not query VM record: " + str(e))

        if not vm["is_a_template"] and not vm["is_control_domain"]: # and vm["power_state"] == "Running":
            vm["ref"] = vm_ref
            vname = vm["name_label"]
            vm_list[vname] = vm
    return vm_list
