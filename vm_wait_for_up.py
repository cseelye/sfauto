#!/usr/bin/env python

"""
This action will wait for a VM to be powered on and guest booted
"""

from libsf.apputil import PythonApp
from libsf.argutil import SFArgumentParser, GetFirstLine, SFArgFormatter
from libsf.logutil import GetLogger, logargs, SetThreadLogPrefix
from libsf.virtutil import VirtualMachine
from libsf.util import ValidateAndDefault, ItemList, IPv4AddressType, StrType, PositiveNonZeroIntegerType
from libsf import sfdefaults, threadutil
from libsf import SolidFireError

@logargs
@ValidateAndDefault({
    # "arg_name" : (arg_type, arg_default)
    "vm_names" : (ItemList(StrType), sfdefaults.vm_names),
    "timeout" : (PositiveNonZeroIntegerType, sfdefaults.client_boot_timeout),
    "vm_mgmt_server" : (IPv4AddressType, sfdefaults.vm_mgmt_server),
    "vm_mgmt_user" : (StrType, sfdefaults.vm_mgmt_user),
    "vm_mgmt_pass" : (StrType, sfdefaults.vm_mgmt_pass),
})
def VmWaitForUp(vm_names,
                timeout,
                vm_mgmt_server,
                vm_mgmt_user,
                vm_mgmt_pass):
    """
    Wait for VMs to be up and running

    Args:
        vm_names:               the VM names
        timeout:                how long to wait for the guest OS
        vm_mgmt_server:         the management server for the VMs (vSphere for VMware, hypervisor for KVM)
        vm_mgmt_user:           the management user for the VMs
        vm_mgmt_pass:           the management password for the VMs
    """
    log = GetLogger()

    log.info("Waiting on {} VMs".format(len(vm_names)))
    pool = threadutil.GlobalPool()
    results = []
    for vm_name in vm_names:
        results.append(pool.Post(_VMThread, vm_name, timeout, vm_mgmt_server, vm_mgmt_user, vm_mgmt_pass))

    allgood = True
    for idx, vm_name in enumerate(vm_names):
        try:
            results[idx].Get()
        except SolidFireError as e:
            log.error("  Error waiting for VM {}: {}".format(vm_name, e))
            allgood = False
            continue

    if allgood:
        log.passed("Successfully waited for all VMs")
        return True
    else:
        log.error("Could not wait for all VMs")
        return False

@threadutil.threadwrapper
def _VMThread(vm_name, timeout, vm_mgmt_server, vm_mgmt_username, vm_mgmt_password):
    log = GetLogger()
    SetThreadLogPrefix(vm_name)

    vm = VirtualMachine.Attach(vm_name, vm_mgmt_server, vm_mgmt_username, vm_mgmt_password)
    log.info("Waiting for VM to be powered on")
    vm.WaitForUp(timeout)


if __name__ == '__main__':
    parser = SFArgumentParser(description=GetFirstLine(__doc__), formatter_class=SFArgFormatter)
    parser.add_argument("--vm-names", type=StrType, required=True, metavar="NAME1,NAME2,...", help="the name of the VMs")
    parser.add_argument("--timeout", type=PositiveNonZeroIntegerType, default=sfdefaults.client_boot_timeout, metavar="SECONDS", help="how long to wait for the guest OS to boot up")
    parser.add_vm_mgmt_args()
    args = parser.parse_args_to_dict()

    app = PythonApp(VmWaitForUp, args)
    app.Run(**args)
