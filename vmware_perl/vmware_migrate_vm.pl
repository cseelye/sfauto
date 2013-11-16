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
        help => "The name of the virtual machine to migrate",
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
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to migrate the VM to",
        required => 0,
    },
    host_index => {
        type => "=i",
        help => "The zero-based index of the host in the cluster to migrate the VM to",
        required => 0,
    },
    parallel_max => {
        type => "=i",
        help => "The max number of threads to use",
        required => 0,
        default => 20,
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
   print "Migrate (vMotion) a VM to a new host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $vm_regex = Opts::get_option('vm_regex');
my $vm_count = Opts::get_option('vm_count');
my $vmhost = Opts::get_option('vmhost');
my $host_index = Opts::get_option('host_index');
my $parallel_max = Opts::get_option('parallel_max');
my $enable_debug = Opts::get_option('debug');
my $result_address = Opts::get_option('result_address');
Opts::validate();

if ($vmhost && defined $host_index)
{
    mylog::error("Please specify only one of host_name, host_index");
    exit 1;
}
if (!$vmhost && !defined $host_index)
{
    mylog::error("Please specify one of host_name, host_index");
    exit 1;
}

# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;

# Turn off cert validation so we can get away with self signed certs
mylog::debug("Disabling SSL cert verification");
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

# Connect to vSphere
mylog::info("Connecting to vSphere at $vsphere_server...");
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

mylog::info("Found " . scalar(@vm_list) . " matching VMs to migrate");

# Find the host
my $host;
if ($vmhost)
{
    mylog::info("Searching for host $vmhost");
    $host = Vim::find_entity_view($mainvim, view_type => 'HostSystem', filter => {'name' => qr/^$vmhost$/i}, properties => []);
    if (!$host)
    {
        mylog::error("Could not find host '$vmhost'");
        exit 1;
    }
}
elsif ($host_index >= 0)
{
    # Use the first VM to find the cluster
    my $vm = Vim::get_view($mainvim, mo_ref => $vm_list[0], properties => ['name', 'runtime']);
    
    mylog::info("Finding the cluster to use for migration");
    my $parent_host = Vim::get_view($mainvim, mo_ref => $vm->runtime->host, properties => ['parent']);
    if ($parent_host->parent->type ne "ClusterComputeResource")
    {
        mylog::error("$vm_name is not in a cluster");
        exit 1;
    }
    my $parent_cluster = Vim::get_view($mainvim, mo_ref => $parent_host->parent, properties => ['host', 'name']);
    my $host_count = scalar(@{$parent_cluster->host});
    mylog::debug("There are $host_count hosts in the cluster");
    mylog::info("Finding host $host_index in cluster " . $parent_cluster->name);
    if ($host_index > $host_count - 1)
    {
        mylog::error("hostindex is outside the range of hosts in the cluster (0-" . ($host_count - 1) . ")");
        exit 1;
    }
    my %name2host;
    my $host_list = Vim::get_views($mainvim, mo_ref_array => \@{$parent_cluster->host}, properties => ['name']);
    foreach my $h (@{$host_list})
    {
        $name2host{$h->name} = $h;
    }
    my @name_list = sort keys(%name2host);
    $vmhost = $name_list[$host_index];
    $host = $name2host{$vmhost};
}

my $th_success : shared;
$th_success = 0;
my @active_threads;
foreach my $vm_mor (@vm_list)
{
    my $th = threads->create(\&migrateVM, $vm_mor);
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




sub migrateVM
{
    my ($vm_mor, $host_mor) = @_;
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
    my $vm = Vim::get_view($threadvim, mo_ref => $vm_mor, properties => ['name']);
    mylog::debug("  Thread $tid is operating on " . $vm->name);

    mylog::info("  " . $vm->name . ": Migrating to $vmhost");
    eval
    {
        $vm->MigrateVM(host => $host_mor, priority => VirtualMachineMovePriority->new('highPriority'));
    };
    if ($@)
    {
        my $fault = $@;
        libvmware::DisplayFault("  " . $vm->name . ": Migration failed", $fault);
        return FALSE;
    }

    {
        lock($th_success);
        $th_success++;
    }

    mylog::pass("  " . $vm->name . ": Sucessfully migrated");
    return TRUE;
}

