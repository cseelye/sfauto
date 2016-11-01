#!/usr/bin/env python2.7

"""
This action will power on a VM
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.virtutil import VirtualMachine
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, OptionalValueType, StrType, PositiveNonZeroIntegerType, BoolType
from libsf import sfdefaults, threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vm_names" : (OptionalValueType(ItemList(StrType)), sfdefaults.vm_names),
    "timeout" : (PositiveNonZeroIntegerType, sfdefaults.client_boot_timeout),
    "wait" : (BoolType, True),
    "vm_mgmt_server" : (OptionalValueType(IPv4AddressType), sfdefaults.vmware_mgmt_server),
    "vm_mgmt_user" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_user),
    "vm_mgmt_pass" : (OptionalValueType(StrType), sfdefaults.vmware_mgmt_pass),
})
def NodePowerOn(vm_names,
                wait,
                timeout,
                vm_mgmt_server,
                vm_mgmt_user,
                vm_mgmt_pass):
    """
    Power on VMs

    Args:
        vm_names:               the VM names
        wait:                   whether or not to wait for the guest OS
        timeout:                how long to wait for the guest OS
        vm_mgmt_server:         the management server for the VMs (vSphere for VMware, hypervisor for KVM)
        vm_mgmt_user:           the management user for the VMs
        vm_mgmt_pass:           the management password for the VMs
    """
    log = GetLogger()

    log.info("Powering on {} VMs".format(len(vm_names)))
    pool = threadutil.GlobalPool()
    results = []
    for vm_name in vm_names:
        results.append(pool.Post(_VMThread, vm_name, wait, timeout, vm_mgmt_server, vm_mgmt_user, vm_mgmt_pass))

    allgood = True
    for idx, vm_name in enumerate(vm_names):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error powering on VM {}: {}".format(vm_name, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully powered on all VMs")
        return True
    else:
        log.error("Could not power on all VMs")
        return False

@threadutil.threadwrapper
def _VMThread(vm_name, wait, timeout, vm_mgmt_server, vm_mgmt_username, vm_mgmt_password):
    log = GetLogger()
    SetThreadLogPrefix(vm_name)

    vm = VirtualMachine.Create(vm_name, vm_mgmt_server, vm_mgmt_username, vm_mgmt_password)
    log.info("Powering on")
    vm.PowerOn()
    if wait:
        vm.WaitForUp(timeout)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vm-names", type=StrType, required=True, metavar="NAME1,NAME2,...", help="the name of the VMs")
    parser.add_argument("--timeout", type=PositiveNonZeroIntegerType, default=sfdefaults.client_boot_timeout, metavar="SECONDS", help="how long to wait for the guest OS to boot up")
    parser.add_argument("--nowait", dest="wait", action="store_false", help="do not wait for the VM guest OS to be up")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(NodePowerOn, args)
    app.Run(**args)
