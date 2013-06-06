#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;

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
        help => "The name of the virtual machine",
        required => 0,
    },
    vm_regex => {
        type => "=s",
        help => "The regex to match names of virtual machines",
        required => 0,
    },
    vm_count => {
        type => "=s",
        help => "The number of matching virtual machines",
        required => 0,
    },
    vm_power => {
        type => "=s",
        help => "The power state to match VMs (on, off)",
        required => 0,
    },
    csv => {
        type => "",
        help => "Display a minimal output that is formatted as a comma separated list",
        required => 0,
    },
    bash => {
        type => "",
        help => "Display a minimal output that is formatted as a space separated list",
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
#if (scalar(@ARGV) < 1)
#{
#   print "Power off a virtual machine.";
#   Opts::usage();
#   exit 1;
#}
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
my $vm_power = Opts::get_option('vm_power');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $result_address = Opts::get_option('result_address');
Opts::validate();

$mylog::DisplayDebug = 1 if $enable_debug;
$mylog::Silent = 1 if ($bash || $csv);

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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}

if ($vm_power && $vm_power =~ /on/i)
{
    $vm_power = "poweredOn";
}
elsif ($vm_power && $vm_power =~ /off/i)
{
    $vm_power = "poweredOff";
}

# Get a list of matching VMs
my @vm_list;
eval
{
    @vm_list = libvmware::SearchForVms(datacenter_name => $dc_name, cluster_name => $cluster_name, pool_name => $pool_name, folder_name => $folder_name, recurse => $recurse, vm_name => $vm_name, vm_regex => $vm_regex, vm_count => $vm_count, vm_powerstate => $vm_power);
    if (scalar(@vm_list) <= 0)
    {
        mylog::warn("There are no matching VMs");
        exit 1;
    }
};
if ($@)
{
    libvmware::DisplayFault("Error", $@);
    exit 1;
}

# Find all the VMs with IP addresses
my %vms;
foreach my $vm_mo (@vm_list)
{
    my $vm = Vim::get_view(mo_ref => $vm_mo, properties => ['name', 'runtime', 'guest']);

    # Skip if it's not a VM
    next if !$vm->isa("VirtualMachine");

    my $vm_name = $vm->name;

    # Skip if it's not powered on
    if ($vm->runtime->powerState->val !~ /poweredOn/)
    {
        mylog::debug("$vm_name is not powered on");
        next;
    }

    # Try to find an IP address
    my $vm_ip;
    if (defined $vm->guest && defined $vm->guest->net)
    {
        foreach my $net (@{$vm->guest->net})
        {
            if (defined $net->ipAddress)
            {
                foreach my $ip (@{$net->ipAddress})
                {
                    if ($ip =~ /^172/)
                    {
                        $vm_ip = $ip;
                        last;
                    }
                }
                last if $vm_ip;
            }
            else
            {
                mylog::debug("$vm_name does not have a defined ipAddress object on " . $net->macAddress . " (" . $net->network . ")");
            }
        }
        # Skip if we couldn't find an IP - either the VM doesn't have one, it's not fully booted, VMware Tools not running, etc.
        if (!$vm_ip)
        {
            mylog::warn("$vm_name is powered on but has no 172 IP address");
            next;
        }
        $vms{$vm_name} = $vm_ip;
    }
    else
    {
        mylog::debug("$vm_name does not have a defined guest/net object");
    }
}

eval
{
    mylog::debug("Disconnecting from vSphere");
    Util::disconnect();
};

if ($csv || $bash)
{
    my @ips;
    foreach my $vm (sort keys %vms)
    {
        push @ips, $vms{$vm};
    }
    my $separator = ",";
    $separator = " " if $bash;
    print join($separator, @ips) . "\n";
}
else
{
    foreach my $vm_name (sort keys %vms)
    {
        my $vm_ip = $vms{$vm_name};
        mylog::info("  $vm_name - $vm_ip");
    }
}
# Send the info back to parent script if requested
if (defined $result_address)
{
    my @ips;
    foreach my $vm (sort keys %vms)
    {
        push @ips, $vms{$vm};
    }
    libsf::SendResultToParent(result_address => $result_address, result => \@ips);
}
exit 0;
