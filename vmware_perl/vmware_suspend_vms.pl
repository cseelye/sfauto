#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;

use threads;
use threads::shared;
no warnings 'threads';
# This script uses threads, and works around the following bugs:
#   6 year old bug in perl causes segfault without a modified VILib.pm to remove the call to binmode
#   http://stackoverflow.com/questions/2644238/why-am-i-getting-a-segmentation-fault-when-i-use-binmode-with-threads-in-perl
# Joining in a loop causes segfault/memory corruption
#   perl -d shows "*** glibc detected *** Hiding the command line arguments: malloc(): smallbin double linked list corrupted: 0x0000000002a92b80 ***
# Detaching threads causes free to wrong pool or double free
# The workarounds are to suppress thread warnings, never detach or join any threads, and let the interpreter clean up the threads on exit.

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
    datacenter => {
        type => "=s",
        help => "Name of the datacenter to search",
        required => 0,
    },
    folder => {
        type => "=s",
        help => "Name of vm folder to search",
        required => 0,
    },
    pool => {
        type => "=s",
        help => "Name of resource pool to search",
        required => 0,
    },
    cluster => {
        type => "=s",
        help => "Name of ESX cluster to search",
        required => 0,
    },
    recurse => {
        type => "",
        help => "Include VMs in subfolders/pools",
        required => 0,
    },
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to suspend",
        required => 0,
    },
    vm_regex => {
        type => "=s",
        help => "The regex to match names of virtual machines to suspend",
        required => 0,
    },
    vm_count => {
        type => "=i",
        help => "The number of matching virtual machines to suspend",
        required => 0,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    parallel_max => {
        type => "=i",
        help => "The max number of threads to use",
        required => 0,
        default => 20,
    },
    debug => {
        type => "",
        help => "Display more verbose messages",
        required => 0,
    },
);

Opts::add_options(%opts);
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $dc_name = Opts::get_option("datacenter");
my $folder_name = Opts::get_option("folder");
my $pool_name = Opts::get_option('pool');
my $cluster_name = Opts::get_option('cluster');
my $recurse = Opts::get_option('recurse');
my $vm_name = Opts::get_option('vm_name');
my $vm_regex = Opts::get_option('vm_regex');
my $vm_count = Opts::get_option('vm_count');
my $parallel_max = Opts::get_option('parallel_max');
my $result_address = Opts::get_option('result_address');
my $enable_debug = Opts::get_option('debug');
Opts::validate();

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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}

# Get a list of matching VMs
my @vm_list;
eval
{
    @vm_list = libvmware::SearchForVms(vim => $mainvim, datacenter_name => $dc_name, cluster_name => $cluster_name, pool_name => $pool_name, folder_name => $folder_name, recurse => $recurse, vm_name => $vm_name, vm_regex => $vm_regex, vm_count => $vm_count, vm_powerstate => "poweredOn");
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
mylog::info("Found " . scalar(@vm_list) . " matching VMs to suspend");

my $th_success : shared;
$th_success = 0;
my @active_threads;
foreach my $vm_mor (@vm_list)
{
    my $th = threads->create(\&suspendVM, $vm_mor);
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
    mylog::pass("Successfully suspended all VMs");
}
else
{
    mylog::error("Failed to suspend all VMs");
    $exitcode = 1;
}

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => scalar(@vm_list));
}
exit $exitcode;

sub suspendVM
{
    my $vm_mor = shift;
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
        return 0;
    }
    my $vm = Vim::get_view($threadvim, mo_ref => $vm_mor, properties => ['name']);
    mylog::debug("  Thread $tid is operating on " . $vm->name);

    mylog::info("  " . $vm->name . ": Suspending");
    my $task_ref;
    eval
    {
        $task_ref = $vm->SuspendVM_Task();
    };
    if ($@)
    {
        libvmware::DisplayFault("  " . $vm->name . ": Failed to suspend", $@);
        return 0;
    }
    
    eval
    {
        libvmware::WaitForTask(vim => $threadvim, task_ref => $task_ref, fail_message => "  Failed to snapshot " . $vm->name);
    };
    if ($@)
    {
        my $er = $@;
        $er =~ s/\s+$//;
        mylog::error("  " . $vm->name . ": Failed to suspend - $er");
        return 0;
    }

    {
        lock($th_success);
        $th_success++;
    }
    mylog::pass("  " . $vm->name . ": Sucessfully suspended");
    return 1;
}

