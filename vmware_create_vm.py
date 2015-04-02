#!/usr/bin/env python

"""
This action will create a new VM

When run as a script, the following options/env variables apply:
    --mgmt_server       The IP/hostname of the vSphere Server

    --mgmt_user         The vsphere admin username

    --mgmt_pass         The vsphere admin password

    --vm_name           The name of the VM

"""

import sys
from optparse import OptionParser
from pyVmomi import vim

import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
import lib.libvmware as libvmware

class VmwareCreateVmAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"mgmt_server" : libsf.IsValidIpv4Address,
                            "mgmt_user" : None,
                            "mgmt_pass" : None,
                            'vmhost' : libsf.IsValidIpv4Address,
                            "vm_name" : None,
                            "mem_size" : libsf.IsPositiveInteger,
                            "cpu_count" : libsf.IsPositiveInteger,
                            "disk_size" : libsf.IsPositiveInteger},
            args)

    def Execute(self, vm_name, mem_size, cpu_count, disk_size, net_name, vmhost, datastore_name=None, mgmt_server=sfdefaults.fc_mgmt_server, mgmt_user=sfdefaults.fc_vsphere_user, mgmt_pass=sfdefaults.fc_vsphere_pass, bash=False, csv=False, debug=False):
        """
        Create the VM
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.showDebug()
        else:
            mylog.hideDebug()
        if bash or csv:
            mylog.silence = True

        mylog.info("Connecting to vSphere " + mgmt_server)
        try:
            with libvmware.VsphereConnection(mgmt_server, mgmt_user, mgmt_pass) as vsphere:

                try:
                    libvmware.FindObjectGetProperties(vsphere, vm_name, vim.VirtualMachine, ['name'])
                    # Successfully found the VM, so this is a duplicate name
                    mylog.error("A VM with this name already exists")
                    return False
                except libvmware.VmwareError:
                    # Could not find the VM so this name is usable
                    pass
                host = libvmware.FindObjectGetProperties(vsphere, vmhost, vim.HostSystem, ['name', 'network', 'datastore'])
                # Find the default resource pool - either right on the host (if it is standalone) or on the cluster this host is in
                parent = libvmware.FindClusterHostIsIn(host) or host
                pool = libvmware.FindResourcePool(vsphere, 'Resources', parent=parent)

                # Find the datacenter this host is in, and the default VM folder
                datacenter = libvmware.FindDatacenterHostIsIn(host)
                folder = datacenter.vmFolder

                # Find the requested network
                network = None
                for net in host.network:
                    if net.name == net_name:
                        network = net
                        break
                if not network:
                    mylog.error("Could not find the requested network on the specified host")
                    return False

                # Find the requested datastore, or pick the datastore with the most free space
                if datastore_name:
                    datastore = libvmware.FindObjectGetProperties(vsphere, datastore_name, vim.Datastore, ['name'])
                else:
                    dsmap = {}
                    for ds in host.datastore:
                        if ds.summary.type != 'VMFS':
                            continue
                        dsmap[ds] = ds.summary.freeSpace
                    if not dsmap:
                        mylog.error('Could not find any datastores to use')
                        return False
                    datastore = sorted(dsmap.keys(), key=lambda key: dsmap[key], reverse=True)[0]
                    if datastore.summary.freeSpace < (disk_size + mem_size + 10) * 1024 * 1024 * 1024:
                        mylog.error('Could not find a datastore with enough free space')
                        return False
                    mylog.debug('Auto selected datastore {} with free space {}GB'.format(datastore.name, datastore.summary.freeSpace/1024/1024/1024))

                mylog.info('Creating VM in:')
                mylog.info('  Datacenter: {}'.format(datacenter.name))
                mylog.info('  Host:       {}'.format(host.name))
                mylog.info('  Datastore:  {}'.format(datastore.name))
                mylog.info('  Network:    {}'.format(network.name))
                mylog.info('With specifications:')
                mylog.info('  Memory:    {}GB'.format(mem_size))
                mylog.info('  CPU count: {}'.format(cpu_count))
                mylog.info('  Disk size: {}GB'.format(disk_size))

                # SCSI controller
                scsi_spec = vim.vm.device.VirtualDeviceSpec()
                scsi_spec.device = vim.vm.device.VirtualLsiLogicController()
                scsi_spec.device.key = 1
                scsi_spec.device.sharedBus = vim.VirtualSCSISharing.noSharing
                scsi_spec.operation = vim.VirtualDeviceConfigSpecOperation.add

                # Disk
                disk_spec = vim.vm.device.VirtualDeviceSpec()
                disk_spec.fileOperation = vim.VirtualDeviceConfigSpecFileOperation.create
                disk_spec.operation = vim.VirtualDeviceConfigSpecOperation.add
                disk_spec.device = vim.vm.device.VirtualDisk()
                disk_spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
                disk_spec.device.backing.diskMode = vim.VirtualDiskMode.persistent
                disk_spec.device.backing.thinProvisioned = True
                disk_spec.device.backing.datastore = datastore
                disk_spec.device.unitNumber = 0
                disk_spec.device.controllerKey = 1 # Must match scsi_spec.device.key above
                disk_spec.device.capacityInKB = disk_size * 1024 * 1024

                # Network
                nic_spec = vim.vm.device.VirtualDeviceSpec()
                nic_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
                nic_spec.device = vim.vm.device.VirtualVmxnet3()
                #nic_spec.device = vim.vm.device.VirtualE1000()
                nic_spec.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                nic_spec.device.backing.network = network
                nic_spec.device.backing.deviceName = net_name
                nic_spec.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
                nic_spec.device.connectable.startConnected = True
                nic_spec.device.connectable.allowGuestControl = True


                config = vim.vm.ConfigSpec()
                config.files = vim.vm.FileInfo(vmPathName='[{}] {}'.format(datastore.name, vm_name))
                config.deviceChange = [disk_spec, scsi_spec, nic_spec]
                config.memoryMB = mem_size * 1024
                config.numCPUs = cpu_count
                config.name = vm_name
                #config.guestId = vim.VirtualMachineGuestOsIdentifier.ubuntu64Guest
                config.guestId = vim.VirtualMachineGuestOsIdentifier.otherLinux64Guest

                # Create the VM
                task = folder.CreateVM_Task(config=config, pool=pool, host=host)
                libvmware.WaitForTasks(vsphere, [task])

        except libvmware.VmwareError as e:
            mylog.error(str(e))
            return False

        return True


# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("-s", "--mgmt_server", type="string", dest="mgmt_server", default=sfdefaults.fc_mgmt_server, help="the IP of the vSphere Server [%default]")
    parser.add_option("-m", "--mgmt_user", type="string", dest="mgmt_user", default=sfdefaults.fc_vsphere_user, help="the vsphere admin username [%default]")
    parser.add_option("-a", "--mgmt_pass", type="string", dest="mgmt_pass", default=sfdefaults.fc_vsphere_pass, help="the vsphere admin password [%default]")
    parser.add_option("-v", "--vmhost", type="string", dest="vmhost", help="the IP of the ESX server")
    parser.add_option("--datastore", type="string", dest="datastore", help="the name of the datastore to use (autoselect if not specified)")
    parser.add_option("--vm_name", type="string", dest="vm_name", default=None, help="the name of the VM to modify")
    parser.add_option("--mem_size", type="int", dest="mem_size", default=None, help="the amount of memory for the VM (in GB)")
    parser.add_option("--cpu_count", type="int", dest="cpu_count", default=None, help="the number of CPUs for the VM")
    parser.add_option("--disk_size", type="int", dest="disk_size", default=None, help="the size of the root disk for the VM (in GB)")
    parser.add_option("--network", type="string", dest="network", default=None, help="the name of the virtual network to attach the VM to")
    parser.add_option("--csv", action="store_true", dest="csv", default=False, help="display a minimal output that is formatted as a comma separated list")
    parser.add_option("--bash", action="store_true", dest="bash", default=False, help="display a minimal output that is formatted as a space separated list")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(vm_name=options.vm_name, mem_size=options.mem_size, cpu_count=options.cpu_count, disk_size=options.disk_size, net_name=options.network, vmhost=options.vmhost, datastore_name=options.datastore, mgmt_server=options.mgmt_server, mgmt_user=options.mgmt_user, mgmt_pass=options.mgmt_pass, bash=options.bash, csv=options.csv, debug=options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)
