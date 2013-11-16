#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;
use constant { TRUE => 1, FALSE => 0 };

use threads;
use threads::shared;
no warnings 'threads';
# This script uses threads, and works around the following bugs:
# 6 year old bug in perl causes segfault without a modified VILib.pm to remove the call to binmode
#   http://stackoverflow.com/questions/2644238/why-am-i-getting-a-segmentation-fault-when-i-use-binmode-with-threads-in-perl
# Joining in a loop causes some segfault/memory corruption
#   perl -d shows "*** glibc detected *** Hiding the command line arguments: malloc(): smallbin double linked list corrupted: 0x0000000002a92b80 ***
# Detaching threads causes free to wrong pool or double free segfault
# The workarounds are to suppress thread warnings, never detach or join any threads, and let the interpreter clean up the threads on exit.

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_usr");
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
        required => 0,
    },
    vm_regex => {
        type => "=s",
        help => "The regex to match names of virtual machines to shutdown",
        required => 0,
    },
    vm_count => {
        type => "=i",
        help => "The number of matching virtual machines to shutdown",
        required => 0,
    },
    datastore_name => {
        type => "=s",
        help => "The name of the datastore to relocate the VM to",
        required => 0,
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
    parallel_max => {
        type => "=i",
        help => "The max number of threads to use",
        required => 0,
        default => 4,
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
   print "Relocate (Storage vMotion) a VM to a new datastore";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $vm_regex = Opts::get_option('vm_regex');
my $vm_count = Opts::get_option('vm_count');
my $datastore_name = Opts::get_option('datastore_name');
my $vm_provisioning = Opts::get_option('vm_provisioning');
my $reserve_space = Opts::get_option('reserve_space');
my $parallel_max = Opts::get_option('parallel_max');
my $enable_debug = Opts::get_option('debug');
my $result_address = Opts::get_option('result_address');
Opts::validate();

$reserve_space = $reserve_space*1024*1024*1024;

# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;

# Turn off cert validation so we can get away with self signed certs
mylog::debug("Disabling SSL cert verification");
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

# Connect to vSphere
my $mainvim;
eval
{
    $mainvim = Vim::login(service_url => Opts::get_option('url'), user_name => Opts::get_option('username'), password => Opts::get_option('password'));
    $Vim::vim_global = undef;
};
if ($@)
{
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

# Get a list of matching VMs
my @vm_list;
eval
{
    @vm_list = libvmware::SearchForVms(vim => $mainvim, vm_name => $vm_name, vm_regex => $vm_regex, vm_count => $vm_count);
    if (scalar(@vm_list) <= 0)
    {
        mylog::warn("There are no matching VMs");
        exit 1;
    }
};
if ($@)
{
    libvmware::DisplayFault("Error searching for VMs", $@);
    exit 1;
}
mylog::info("Found " . scalar(@vm_list) . " matching VMs to relocate");


my @datastore_list;

if ($datastore_name) # We are moving all VMs to the same datastore
{
    # Find the destination datastore
    mylog::info("Searching for datastore $datastore_name");
    my $datastore = Vim::find_entity_view($mainvim, view_type => 'Datastore', filter => {'name' => qr/^$datastore_name/i}, properties => ['name', 'summary']);
    if (!$datastore)
    {
        mylog::error("Could not find datastore");
        exit 1;
    }

    # Figure out the total space needed
    my $total_space_needed = 0;
    for my $vm_mor (@vm_list)
    {
        my $vm = Vim::get_view($mainvim, mo_ref => $vm_mor, properties => ['name', 'layout', 'config', 'summary', 'datastore']);
        if ($vm->datastore->[0]->{value} ne $datastore->{mo_ref}->{value})
        {
            $total_space_needed += calculateVMSpaceUsage($vm);
        }
    }

    # Determine if the VM will fit in the datastore
    my $ds_free = $datastore->summary->freeSpace;
    mylog::debug($datastore->name . " has " . sprintf("%.2f", $ds_free/1024/1024/1024) . " GiB free");
    mylog::debug($datastore->name . " needs " . sprintf("%.2f", ($total_space_needed + $reserve_space)/1024/1024/1024) . " GiB free");
    if ($ds_free - $total_space_needed < $reserve_space)
    {
        mylog::error($datastore->name . " does not have enough free space");
        exit 1;
    }
    for my $vm_mor (@vm_list)
    {
        push (@datastore_list, $datastore->{mo_ref});
    }
}
else # Find each destination datastore based on the name of the VM
{
    for my $vm_mor (@vm_list)
    {
        my $vm = Vim::get_view($mainvim, mo_ref => $vm_mor, properties => ['name', 'layout', 'config', 'summary', 'datastore']);
        my $vm_usage = calculateVMSpaceUsage($vm);
        
        # Find the dest datastore
        my $vmname = $vm->name;
        my $datastore = Vim::find_entity_view($mainvim, view_type => 'Datastore', filter => {'name' => qr/^$vmname/i}, properties => ['name', 'summary']);
        if (!$datastore)
        {
            mylog::error("Could not find destination datastore for " . $vm->name);
            exit 1;
        }
        
        if ($vm->datastore->[0]->{value} ne $datastore->{mo_ref}->{value})
        {
            # Determine if the VM will fit in the datastore
            my $ds_free = $datastore->summary->freeSpace;
            mylog::debug($datastore->name . " has " . sprintf("%.2f", $ds_free/1024/1024/1024) . " GiB free");
            mylog::debug($datastore->name . " needs " . sprintf("%.2f", ($vm_usage + $reserve_space)/1024/1024/1024) . " GiB free");
            if ($ds_free - $vm_usage < $reserve_space)
            {
                mylog::error($datastore->name . " does not have enough free space");
                exit 1;
            }
        }
        push (@datastore_list, $datastore->{mo_ref});
    }    
}

my $th_success : shared;
$th_success = 0;
my @active_threads;
for (my $i=0; $i < scalar(@vm_list); $i++)
{
    my $vm_mor = $vm_list[$i];
    my $ds_mor = $datastore_list[$i];
    my $th = threads->create(\&relocateVM, $vm_mor, $ds_mor);
    push (@active_threads, $th);
    while (scalar(@active_threads) >= $parallel_max)
    {
        # Iterate backwards through the array and remove any threads that are finished
        for (my $i = $#active_threads; $i >= 0; --$i)
        {
            if (!$active_threads[$i]->is_running())
            {
                splice (@active_threads, $i, 1);
            }
        }
        sleep 5 if (scalar(@active_threads) >= $parallel_max);
    }
}

# Wait for all threads to complete
while (1)
{
    my $done = 1;
    for my $t (@active_threads)
    {
        if ($t->is_running())
        {
            $done = 0;
            last;
        }
    }
    last if $done;
    sleep 1;
}

my $exitcode = 0;
if ($th_success == scalar(@vm_list))
{
    mylog::pass("Successfully migrated all VMs");
}
else
{
    mylog::error("Failed to migrate all VMs");
    $exitcode = 1;
}

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $th_success);
}
exit $exitcode;


sub calculateVMSpaceUsage
{
    my $vm = shift;
    my $vm_is_currently_thin = 0;
    my $vm_provisioned_capacity = 0;
    mylog::debug($vm->name . " has " . scalar(@{$vm->layout->disk}) . " disks");
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
        mylog::debug($vm->name . " is currently thinly provisioned");
    }
    else
    {
        mylog::debug($vm->name . " is currently fully provisioned");
    }
    mylog::debug($vm->name . " provisioned space: " . sprintf("%.2f", $vm_provisioned_capacity/1024/1024/1024) . " GiB");
    mylog::debug($vm->name . " used space:        " . sprintf("%.2f", $vm_used_capacity/1024/1024/1024) . " GiB");
    mylog::debug($vm->name . " memory size:       " . sprintf("%.2f", $vm_memory/1024/1024/1024) . " GiB");

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
    return $vm_usage;
}

sub relocateVM
{
    my ($vm_mor, $ds_mor) = @_;
    my $tid = threads->self()->tid;
    mylog::debug("  Thread $tid connecting to vSphere");
    my $threadvim;
    eval
    {
        $threadvim = Vim::login(service_url => Opts::get_option('url'), user_name => Opts::get_option('username'), password => Opts::get_option('password'));
        $Vim::vim_global = undef;
    };
    if ($@)
    {
        mylog::error("  Thread $tid could not connect to $vsphere_server: $@");
        return FALSE;
    }
    my $vm;
    eval
    {
        $vm = Vim::get_view($threadvim, mo_ref => $vm_mor, properties => ['name']);
    };
    if ($@)
    {
        libvmware::DisplayFault("  Thread $tid could not get VM info", $@);
        return FALSE;
    }
    mylog::debug("  Thread $tid is operating on " . $vm->name);

    my $ds;
    eval
    {
        $ds = Vim::get_view($threadvim, mo_ref => $ds_mor, properties => ['name']);
    };
    if ($@)
    {
        libvmware::DisplayFault("  " . $vm->name . ": Could not get datastore info", $@);
        return FALSE;
    }

    my $relocate_spec = VirtualMachineRelocateSpec->new(datastore => $ds_mor);
    if ($vm_provisioning =~ /thin/i)
    {
        $relocate_spec->{transform} = VirtualMachineRelocateTransformation->new('sparse');
    }
    elsif ($vm_provisioning =~ /full/i)
    {
        $relocate_spec->{transform} = VirtualMachineRelocateTransformation->new('flat');
    }

    mylog::info("  " . $vm->name . ": Relocating to " . $ds->name);
    my $relo_start = time();
    eval
    {
        $vm->RelocateVM(spec => $relocate_spec, priority => VirtualMachineMovePriority->new('highPriority'));
    };
    if ($@)
    {
        my $fault = $@;
        libvmware::DisplayFault("  " . $vm->name . ": Relocation failed", $fault);
        return FALSE;
    }
    my $relo_end = time();

    {
        lock($th_success);
        $th_success++;
    }

    mylog::pass("  " . $vm->name . ": Sucessfully relocated (" . libsf::SecondsToElapsed($relo_end - $relo_start) . ")");
    return TRUE;
}

# Relocate the VM to the new datastore
#my $relocate_spec = VirtualMachineRelocateSpec->new(datastore => $datastore);
#if ($vm_provisioning =~ /thin/i)
#{
#    $relocate_spec->transform = VirtualMachineRelocateTransformation->new('sparse');
#}
#elsif ($vm_provisioning =~ /full/i)
#{
#    $relocate_spec->transform = VirtualMachineRelocateTransformation->new('flat');
#}
#
#mylog::info("Relocating $vm_name to $datastore_name...");
#my $start = time();
#eval
#{
#    $vm->RelocateVM(spec => $relocate_spec, priority => VirtualMachineMovePriority->new('highPriority'));
#};
#if ($@)
#{
#    my $fault = $@;
#    libvmware::DisplayFault("$vm_name relocation failed", $fault);
#    exit 1;
#}
#my $end = time();
#mylog::info("$vm_name relocate took " . libsf::SecondsToElapsed($end - $start));
#mylog::pass("$vm_name successfully relocated");
## Send the info back to parent script if requested
#if (defined $result_address)
#{
#    libsf::SendResultToParent(result_address => $result_address, result => 1);
#}
#exit 0;
