#!/usr/bin/perl
use strict;

use VMware::VIRuntime;
use libsf;
use libvmware;
use Data::UUID;
use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "administrator");
Opts::set_option("password", "solidfire");

# Set default vCenter Server
# This can be overridden with --mgmt_server
Opts::set_option("server", "172.26.75.47"); # default to FC vcenter

my %opts = (
    mgmt_server => {
        type => "=s",
        help => "The hostname/IP of the vCenter Server (replaces --server)",
        required => 0,
        default => Opts::get_option("server"),
    },
    source_vm => {
        type => "=s",
        help => "The name of the virtual machine to add the disk to",
        required => 1,
    },
    datastore => {
        type => "=s",
        help => "Name of the datastore to put the new disk in",
        required => 1,
    },
    thin => {
        type => "",
        help => "Create a thin provisioned VMDK instead of thick provisioned",
        required => 0,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    debug => {
        type => "",
        help => "Display more verbose messages",
        required => 0,
    },
);

Opts::add_options(%opts);
if (scalar(@ARGV) < 1)
{
   print "Add a VMDK disk a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $source_vm_name = Opts::get_option('source_vm');
my $dest_datastore_name = Opts::get_option('datastore');
my $enable_debug = Opts::get_option('debug');
my $thin_prov = Opts::get_option('thin');
my $result_address = Opts::get_option('result_address');
Opts::validate();

# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;

# Turn off cert validation so we can get away with self signed certs
mylog::debug("Disabling SSL cert verification");
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

# Connect to vSphere
mylog::info("Connecting to vSphere at $vsphere_server...");
eval
{
   Util::connect();
};
if ($@)
{
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

# Find the source VM
mylog::info("Searching for source VM $source_vm_name");
my $source_vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$source_vm_name$/i});
if (!$source_vm)
{
    mylog::error("Could not find source VM '$source_vm_name'");
    exit 1;
}

# Find the destination datastore
mylog::info("Searching for destination datastore $dest_datastore_name");
my $dest_datastore = Vim::find_entity_view(view_type => 'Datastore', filter => {'name' => qr/^$dest_datastore_name/i});
if (!$dest_datastore)
{
    mylog::error("Could not find destination datastore '$dest_datastore_name'");
    exit 1;
}

# Calculate size of the VMDK to create, in kB
my $ds_free = $dest_datastore->summary->freeSpace; # in bytes
my $vmdk_size = $ds_free - 500 * 1024 * 1024; # 500 MB less than datastore size
$vmdk_size = $vmdk_size / 1024; # convert to kB

# Find the SCSI controller for the VM
my $scsi_controller;
foreach my $device (@{$source_vm->config->hardware->device})
{
    my $class = ref $device;
    if ($class->isa("VirtualBusLogicController") ||
        $class->isa("VirtualLsiLogicController") ||
        $class->isa("VirtualLsiLogicSASController") ||
        $class->isa("ParaVirtualSCSIController"))
    {
        $scsi_controller = $device;
        last;
    }
}
if (!$scsi_controller)
{
    mylog::error("Could not find SCSI controller for VM '$source_vm_name'");
    exit 1;
}

# Get a list of existing LUN numbers
my @unit_numbers;
foreach my $device (@{$source_vm->config->hardware->device})
{
    my $class = ref $device;
    if ($class->isa("VirtualDisk"))
    {
        push @unit_numbers, $device->unitNumber;
    }
}

# FInd any gaps in existing LUN numbers and determine where the new disk fits
@unit_numbers = sort(@unit_numbers);
my $new_unit_number;
my @gaps;
my $previous -1;
my $highest = 0;
for (my $i=1; $i < scalar(@unit_numbers); $i++)
{
    if ($previous < 0)
    {
        $previous = $unit_numbers[$i];
        next;
    }
    if ($previous + 1 == 7)
    {
        $previous = 7;
    }
    if ($unit_numbers[$i] > $highest)
    {
        $highest = $unit_numbers[$i];
    }
    if ($unit_numbers[$i] != $previous + 1)
    {
        push @gaps, $previous+1..$unit_numbers[$i]-1;
        $previous = $gaps[-1];
    }
    else
    {
        $previous = $unit_numbers[$i];
    }
}
if (scalar(@gaps) <= 0)
{
    push @gaps, $highest+1;
}
$new_unit_number = shift @gaps;
if ($new_unit_number == 7)
{
    if (scalar(@gaps) <= 0)
    {
        $new_unit_number = 8;
    }
    else
    {
        $new_unit_number = shift @gaps;
    }
}

# Generate filename
my $uuid = Data::UUID->new();
my $filename = "[" . $dest_datastore->name . "]/" . $source_vm->name . "/" . $source_vm->name . "-" . $uuid->create_str() . ".vmdk";
mylog::info("Creating VMDK file '$filename' and attaching to VM as LUN $new_unit_number");

# Create the new virtual disk spec
my $backing_info = VirtualDiskFlatVer2BackingInfo->new(diskMode => "persistent",
                                                        fileName => $filename,
                                                        eagerlyScrub => 1,
                                                        thinProvisioned => 0);

my $disk_info = VirtualDisk->new(controllerKey => $scsi_controller->key,
                                    unitNumber => $new_unit_number,
                                    key => -1,
                                    backing => $backing_info,
                                    capacityInKB => $vmdk_size);

my $device_spec = VirtualDeviceConfigSpec->new(operation => VirtualDeviceConfigSpecOperation->new("add"),
                                                device => $disk_info,
                                                fileOperation => VirtualDeviceConfigSpecFileOperation->new("create"));


# Add the disk to the VM
mylog::info("Adding disk to VM...");
my $vm_spec = VirtualMachineConfigSpec->new(deviceChange => [$device_spec]);
eval
{
    $source_vm->ReconfigVM(spec => $vm_spec);
};
if ($@)
{
    my $fault = $@;
    libvmware::DisplayFault("Failed to add disk", $fault);
    exit 1;
}



# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;
