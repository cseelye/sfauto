#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to migrate",
        required => 1,
    },
    host_name => {
        type => "=s",
        help => "The hostname/IP of the host to migrate the VM to",
        required => 0,
    },
    host_index => {
        type => "=i",
        help => "The zero-based index of the host in the cluster to migrate the VM to",
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
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $host_name = Opts::get_option('host_name');
my $host_index = Opts::get_option('host_index');
my $enable_debug = Opts::get_option('debug');

if ($host_name && defined $host_index)
{
    mylog::error("Please specify only one of host_name, host_index");
    exit 1;
}
if (!$host_name && !defined $host_index)
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
my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i}, properties => ['runtime']);
if (!$vm)
{
    mylog::error("Could not find host '$vm_name'");
    exit 1;
}

# Find the host
my $vmhost;
if ($host_name)
{
    mylog::info("Searching for host $host_name");
    $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i}, properties => []);
    if (!$vmhost)
    {
        mylog::error("Could not find host '$host_name'");
        exit 1;
    }
}
elsif ($host_index >= 0)
{
    mylog::info("Finding the cluster $vm_name is in");
    my $parent_host = Vim::get_view(mo_ref => $vm->runtime->host, properties => ['parent']);
    if ($parent_host->parent->type ne "ClusterComputeResource")
    {
        mylog::error("$vm_name is not in a cluster");
        exit 1;
    }
    my $parent_cluster = Vim::get_view(mo_ref => $parent_host->parent, properties => ['host']);
    my $host_count = scalar(@{$parent_cluster->host});
    mylog::debug("There are $host_count hosts in the cluster");
    mylog::info("Finding host $host_index");
    if ($host_index > $host_count - 1)
    {
        mylog::error("hostindex is outside the range of hosts in the cluster (0-" . ($host_count - 1) . ")");
        exit 1;
    }
    my %name2host;
    my $host_list = Vim::get_views(mo_ref_array => \@{$parent_cluster->host}, properties => ['name']);
    foreach my $host (@{$host_list})
    {
        $name2host{$host->name} = $host;
    }
    my @name_list = sort keys(%name2host);
    $host_name = $name_list[$host_index];
    $vmhost = $name2host{$host_name};
}

mylog::info("Migrating $vm_name to $host_name");
eval
{
    $vm->MigrateVM(host => $vmhost, priority => VirtualMachineMovePriority->new('highPriority'));
};
if ($@)
{
    my $fault = $@;
    libsf::DisplayFault("Migration failed", $fault);
    exit 1;
}

mylog::pass("Successfully migrated");
exit 0;







