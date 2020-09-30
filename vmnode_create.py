#!/usr/bin/env python

"""
This action will create a virtual node
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs
from libsf.virtutil import VMHost
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, StrType, PositiveNonZeroIntegerType, SelectionType
from libsf import sfdefaults
from libsf import SolidFireError, InvalidArgumentError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vmhost_ip" : (IPv4AddressType, None),
    "vm_name" : (StrType, None),
    "management_net" : (StrType, None),
    "storage_net" : (str, ""),
    "datastores" : (ItemList(StrType), None),
    "node_type" : (SelectionType(["storage", "management"]), "storage"),
    "cpu_count" : (PositiveNonZeroIntegerType, 6),
    "mem_size" : (PositiveNonZeroIntegerType, 24),
    "slice_size" : (PositiveNonZeroIntegerType, 60),
    "block_size" : (PositiveNonZeroIntegerType, 50),
    "block_count" : (PositiveNonZeroIntegerType, 10),
    "vm_mgmt_server" : (IPv4AddressType, sfdefaults.vm_mgmt_server),
    "vm_mgmt_user" : (StrType, sfdefaults.vm_mgmt_user),
    "vm_mgmt_pass" : (StrType, sfdefaults.vm_mgmt_pass),
})
def VmnodeCreate(vmhost_ip,
                 vm_name,
                 management_net,
                 storage_net,
                 datastores,
                 node_type,
                 cpu_count,
                 mem_size,
                 slice_size,
                 block_size,
                 block_count,
                 vm_mgmt_server,
                 vm_mgmt_user,
                 vm_mgmt_pass):
    """
    Create a virtual node

    Args:
        vm_name:                name of the VM to create
        management_net:         network to connect the management NICs to
        storage_net:            network to connect the storage NICs to
        datastores:             the datastores to put the VM in
        node_type:              the type of node to create (management, storage)
        cpu_count:              number of vCPUs for the VM
        mem_size:               how much memory for the VM, in GB
        slice_size:             size of the slice drive
        block_size:             size of the block drives
        block_count:            how many block drives
        vm_mgmt_server:         the management server for the VMs (vSphere for VMware, hypervisor for KVM)
        vm_mgmt_user:           the management user for the VMs
        vm_mgmt_pass:           the management password for the VMs
    """
    if node_type == "storage" and not storage_net:
        raise InvalidArgumentError("Storage network is required")

    log = GetLogger()

    log.info("Connecting to {}".format(vm_mgmt_server))
    try:
        host = VMHost.Attach(vmhost_ip, vm_mgmt_server, vm_mgmt_user, vm_mgmt_pass)
    except SolidFireError as ex:
        log.error("Could not connect to hypervisor: {}".format(str(ex)))
        return False

    # Create the VM
    log.info("Creating {}".format(vm_name))
    if node_type == "storage":
        host.CreateVMNode(vm_name, management_net, storage_net, datastores, cpu_count, mem_size, slice_size, block_size, block_count)
    else:
        host.CreateVMNodeManagement(vm_name, management_net, datastores[0])

    log.passed("Successfully created virtual {} node {}".format(node_type, vm_name))
    return True


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vmhost-ip", type=IPv4AddressType, required=True, metavar="IP", help="the IP address of the hypervisor")
    parser.add_argument("--vm-name", type=StrType, required=True, metavar="NAME", help="the name of the VM to create")
    parser.add_argument("--management-net", type=StrType, required=True, metavar="NAME", help="the name of the network to connect the management NICs to")
    parser.add_argument("--storage-net", type=str, metavar="NAME", default="", help="the name of the network to connect the storage NICs to")
    parser.add_argument("--datastores", type=ItemList(StrType), required=True, metavar="NAME", help="the name of the datastores to put the VM in")
    parser.add_argument("--node-type", type=StrType, required=True, choices=["storage", "management"], default="storage", help="type of node to create")
    parser.add_vm_mgmt_args()

    adv_confg = parser.add_argument_group("Storage Node Advanced Configuration")
    adv_confg.add_argument("--cpu-count", type=PositiveNonZeroIntegerType, required=True, default=6, metavar="COUNT", help="how many vCPU for the VM")
    adv_confg.add_argument("--mem-size", type=PositiveNonZeroIntegerType, required=True, default=24, metavar="GB", help="how much memory for the VM, in GB")
    adv_confg.add_argument("--slice-size", type=PositiveNonZeroIntegerType, required=True, default=60, metavar="GB", help="size of the slice drive, in GB")
    adv_confg.add_argument("--block-size", type=PositiveNonZeroIntegerType, required=True, default=50, metavar="GB", help="size of the block drives, in GB")
    adv_confg.add_argument("--block-count", type=PositiveNonZeroIntegerType, required=True, default=10, metavar="COUNT", help="number of block drives")
    args = parser.parse_args_to_dict()

    app = PythonApp(VmnodeCreate, args)
    app.Run(**args)
