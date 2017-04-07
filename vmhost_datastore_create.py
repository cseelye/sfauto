#!/usr/bin/env python2.7

"""
This action will create datastores on a hypervisor
By default only iSCSI and FC disks are used
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.virtutil import VMHost
from libsf.util import ValidateAndDefault, IPv4AddressType, OptionalValueType, StrType, BoolType
from libsf import sfdefaults
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vmhost_ip" : (IPv4AddressType, sfdefaults.vm_names),
    "include_internal" : (BoolType, False),
    "include_slots" : (BoolType, False),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_pass),
})
def VmhostCreateDatastores(vmhost_ip,
                           include_internal,
                           include_slots,
                           vm_mgmt_server,
                           vm_mgmt_user,
                           vm_mgmt_pass):
    """
    Create datastores
    
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

    try:
        host.CreateDatastores(includeInternalDrives=include_internal, includeSlotDrives=include_slots)
    except SolidFireError as ex:
        log.error("Error creating datastores: {}".format(str(ex)))
        return False

    log.passed("Successfully created datastores")
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vmhost-ip", type=IPv4AddressType, required=True, metavar="IP", help="the IP address of the hypervisor")
    parser.add_argument("--include-internal", action="store_true", default=False, help="include internal drives (like satadim/dom)")
    parser.add_argument("--include-slots", action="store_true", default=False, help="include external drives in slots")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VmhostCreateDatastores, args)
    app.Run(**args)
