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
        help => "The name of the virtual machine to power off",
        required => 0,
    },
    vm_regex => {
        type => "=s",
        help => "The regex to match names of virtual machines to power off",
        required => 0,
    },
    vm_count => {
        type => "=s",
        help => "The number of matching virtual machines to power off",
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
my $enable_debug = Opts::get_option('debug');
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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}

# Get a list of matching VMs
my @vm_list;
eval
{
    @vm_list = libvmware::SearchForVms(datacenter_name => $dc_name, cluster_name => $cluster_name, pool_name => $pool_name, folder_name => $folder_name, recurse => $recurse, vm_name => $vm_name, vm_regex => $vm_regex, vm_count => $vm_count, vm_powerstate => "poweredOn");
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

# Power the VMs off
my $error = 0;
foreach my $vm_mor (@vm_list)
{
    my $vm = Vim::get_view(mo_ref => $vm_mor, properties => ['name']);
    mylog::info("Powering off " . $vm->name);
    eval
    {
        $vm->PowerOffVM();
        mylog::pass("Sucessfully powered off " . $vm->name);
    };
    if ($@)
    {
        libvmware::DisplayFault("Failed to power off VM", $@);
        $error++;
    }
}

if ($error > 0)
{
    mylog::error("Failed to power off all VMs");
    exit 1;
}
elsif (scalar(@vm_list) > 1)
{
    mylog::pass("Successfully powered off all VMs");
}
exit 0;

