#!/usr/bin/env python2.7

"""
This action will set an IP address on a hypervisor
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.virtutil import VMHost
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, StrType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vmhost_ip" : (IPv4AddressType, sfdefaults.vm_names),
    "vnic_name" : (StrType, None),
    "ipaddress" : (IPv4AddressType, None),
    "netmask" :  (IPv4AddressType, None),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_pass),
})
def VmhostSetIp(vmhost_ip,
                vnic_name,
                ipaddress,
                netmask,
                vm_mgmt_server,
                vm_mgmt_user,
                vm_mgmt_pass):
    """
    Set an IP address
    
    Args:
        vmhost_ip:              the hypervisor IP address
        vm_mgmt_server:         the management server for the VMs (vSphere for VMware, hypervisor for KVM)
        vm_mgmt_user:           the management user for the VMs
        vm_mgmt_pass:           the management password for the VMs
    """
    log = GetLogger()

    log.info("Connecting to {}".format(vm_mgmt_server))
    try:
        host = VMHost.Attach(vmhost_ip, vm_mgmt_server, vm_mgmt_user, vm_mgmt_pass)
    except SolidFireError as ex:
        log.error("Could not connect to hypervisor: {}".format(str(ex)))
        return False

    log.info("Configuring '{}'".format(vnic_name))
    try:
        host.SetNetworkInfo(vnic_name, ipaddress, netmask)
    except SolidFireError as ex:
        log.error("Failed to configure: {}".format(str(ex)))
        return False


    log.passed("Successfully configured '{}'".format(vnic_name))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vmhost-ip", type=IPv4AddressType, required=True, metavar="IP", help="the IP address of the hypervisor")
    parser.add_argument("--vnic-name", type=StrType, required=True, metavar="NAME", help="the name of the vnic to configure")
    parser.add_argument("--ipaddress", type=IPv4AddressType, metavar="IP", required=True, help="IP address to set")
    parser.add_argument("--netmask", type=IPv4AddressType, metavar="IP", required=True, help="IP address to set")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VmhostSetIp, args)
    app.Run(**args)
