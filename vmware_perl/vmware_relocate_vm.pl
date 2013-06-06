#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_user");
Opts::set_option("password", "password");

# Set default vCenter Server
# This can be overridden with --mgmt_server
Opts::set_option("server", "vcenter.domain.local");

my %opts = (
    mgmt_server => {
        type => "=s",
        help => "The hostname/IP of the vCenter Server (replaces --server)",
        required => 0,
        default => Opts::get_option("server"),
    },
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to relocate",
        required => 1,
    },
    datastore_name => {
        type => "=s",
        help => "The name of the datastore to relocate the VM to",
        required => 1,
    },
    vm_provisioning => {
        type => "=s",
        help => "How to provision the VM disks (full, thin or same)",
        required => 0,
        default => "same",
    },
    reserve_space => {
        type => "=i",
        help => "The amount of extra space (GB) to reserve in the datasatore when determining if the VM will fit",
        required => 0,
        default => 10,
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
   print "Relocate (Storage vMotion) a VM to a new datastore";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $datastore_name = Opts::get_option('datastore_name');
my $vm_provisioning = Opts::get_option('vm_provisioning');
my $reserve_space = Opts::get_option('reserve_space');
my $enable_debug = Opts::get_option('debug');
Opts::validate();

$reserve_space = $reserve_space*1024*1024*1024;

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

# Find the VM
mylog::info("Searching for VM $vm_name");
my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i}, properties => ['summary', 'config', 'layout', 'layoutEx']);
if (!$vm)
{
    mylog::error("Could not find VM '$vm_name'");
    exit 1;
}

my $vm_is_currently_thin = 0;
my $vm_provisioned_capacity = 0;
mylog::debug("VM has " . scalar(@{$vm->layout->disk}) . " disks");
foreach my $disk (@{$vm->layout->disk})
{
    my $key = $disk->key;
    my $thin = 0;
    my $capacity = 0;
    foreach my $device (@{$vm->config->hardware->device})
    {
        if ($device->key == $key)
        {
            $capacity = $device->capacityInKB*1024;
            $vm_is_currently_thin = 1 if ($device->backing->thinProvisioned);
            last;
        }
    }
    $vm_provisioned_capacity += $capacity;
}

my $vm_used_capacity = $vm->summary->storage->committed;
my $vm_memory = $vm->config->hardware->memoryMB * 1024 * 1024;

if ($vm_is_currently_thin)
{
    mylog::debug("VM is currently thinly provisioned");
}
else
{
    mylog::debug("VM is currently fully provisioned");
}
mylog::debug("Provisioned space: " . sprintf("%.2f", $vm_provisioned_capacity/1024/1024/1024) . " Gib");
mylog::debug("Used space:        " . sprintf("%.2f", $vm_used_capacity/1024/1024/1024) . " Gib");
mylog::debug("Memory size:       " . sprintf("%.2f", $vm_memory/1024/1024/1024) . " Gib");

# Find the destination datastore
mylog::info("Searching for datastore $datastore_name");
my $datastore = Vim::find_entity_view(view_type => 'Datastore', filter => {'name' => qr/^$datastore_name$/i}, properties => ['summary']);

# Determine if the VM will fit in the datastore
my $ds_free = $datastore->summary->freeSpace;
mylog::debug($datastore_name . " has " . sprintf("%.2f", $ds_free/1024/1024/1024) . " GiB free");
my $vm_usage;
if ($vm_provisioning =~ /thin/i)
{
    $vm_usage = $vm_used_capacity + $vm_memory;
}
elsif ($vm_provisioning =~ /full/i)
{
    $vm_usage = $vm_provisioned_capacity + $vm_memory;
}
else
{
    if ($vm_is_currently_thin)
    {
        $vm_usage = $vm_used_capacity + $vm_memory;
    }
    else
    {
        $vm_usage = $vm_provisioned_capacity + $vm_memory;
    }
}
mylog::debug($datastore_name . " needs " . sprintf("%.2f", ($vm_usage + $reserve_space)/1024/1024/1024) . " GiB free");
if ($ds_free - $vm_usage - $reserve_space < 0)
{
    mylog::error("$datastore_name does not have enough free space");
    exit 1;
}

# Relocate the VM to the new datastore
my $relocate_spec = VirtualMachineRelocateSpec->new(datastore => $datastore);
if ($vm_provisioning =~ /thin/i)
{
    $relocate_spec->transform = VirtualMachineRelocateTransformation->new('sparse');
}
elsif ($vm_provisioning =~ /full/i)
{
    $relocate_spec->transform = VirtualMachineRelocateTransformation->new('flat');
}

mylog::info("Relocating $vm_name to $datastore_name...");
my $start = time();
eval
{
    $vm->RelocateVM(spec => $relocate_spec, priority => VirtualMachineMovePriority->new('highPriority'));
};
if ($@)
{
    my $fault = $@;
    libsf::DisplayFault("Relocation failed", $fault);
    exit 1;
}
my $end = time();
mylog::info("Relocate took " . libsf::SecondsToElapsed($end - $start));
mylog::pass("Successfully relocated");
exit 0;
