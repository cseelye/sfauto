#!/usr/bin/env python

"""
This action will delete a port group on a hypervisor
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
    "portgroup_name" : (StrType, None),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_pass),
})
def VmhostPortGroupDelete(vmhost_ip,
                          portgroup_name,
                          vm_mgmt_server,
                          vm_mgmt_user,
                          vm_mgmt_pass):
    """
    Create a port group
    
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

    log.info("Deleting port group '{}'".format(portgroup_name))
    try:
        host.DeletePortgroup(portgroup_name)
    except SolidFireError as ex:
        log.error("Could not delete port group: {}".format(str(ex)))
        return False


    log.passed("Successfully deleted port group to '{}'".format(portgroup_name))
    return True

if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vmhost-ip", type=IPv4AddressType, required=True, metavar="IP", help="the IP address of the hypervisor")
    parser.add_argument("--portgroup-name", type=StrType, required=True, metavar="NAME", help="the name of the port group to delete")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VmhostPortGroupDelete, args)
    app.Run(**args)
