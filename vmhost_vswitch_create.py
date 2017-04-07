#!/usr/bin/env python2.7

"""
This action will create a vswitch on a hypervisor
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.virtutil import VMHost
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, StrType, PositiveNonZeroIntegerType, ItemList
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vmhost_ip" : (IPv4AddressType, sfdefaults.vm_names),
    "vswitch_name" : (StrType, None),
    "vswitch_mtu" : (PositiveNonZeroIntegerType, 1500),
    "vswitch_pnics" : (ItemList(StrType), None),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_pass),
})
def VmhostVswitchCreate(vmhost_ip,
                        vswitch_name,
                        vswitch_mtu,
                        vswitch_pnics,
                        vm_mgmt_server,
                        vm_mgmt_user,
                        vm_mgmt_pass):
    """
    Create a vswitch
    
    Args:
        vmhost_ip:              the hypervisor IP address
        include_internal:       create datastores on internal drives (like satadimm/dom)
        include_slots:          create datastores on external drives in chassis slots
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

    log.info("Creating vswitch")
    try:
        host.CreateVswitch(vswitch_name, vswitch_pnics, vswitch_mtu)
    except SolidFireError as ex:
        log.error("Could not create vswitch: {}".format(str(ex)))
        return False


    log.passed("Successfully created vswitch {}".format(vswitch_name))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vmhost-ip", type=IPv4AddressType, required=True, metavar="IP", help="the IP address of the hypervisor")
    parser.add_argument("--vswitch-name", type=StrType, required=True, metavar="NAME", help="the name of the switch to create")
    parser.add_argument("--vswitch-mtu", type=PositiveNonZeroIntegerType, default=1500, required=True, metavar="MTU", help="the MTU of the switch")
    parser.add_argument("--vswitch-pnics", type=ItemList(StrType), metavar="NIC1,NIC2,...", help="the physical NICs to attach to the vswitch")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VmhostVswitchCreate, args)
    app.Run(**args)
