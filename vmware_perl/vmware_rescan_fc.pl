#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;
use JSON::XS;

use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "administrator");
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
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to create datastores on",
        required => 1,
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
   print "Rescan FC HBAs in the given host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $enable_debug = Opts::get_option('debug');
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

# Find the host
mylog::info("Searching for host $host_name");
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i});
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}
my $storage_manager = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);

my $success = 1;
my $adapter_list = $vmhost->config->storageDevice->hostBusAdapter;
foreach my $adapter (@{$adapter_list})
{
    if (ref($adapter) =~ /FibreChannel/)
    {
        mylog::info("Rescanning $adapter->{device}");
        eval
        {
            $storage_manager->RescanHba(hbaDevice => $adapter->device)
        };
        if ($@)
        {
            my $fault = $@;
            libvmware::DisplayFault("Failed to rescan $adapter->{device}", $fault);
            $success = 0;
        }
    }
}
eval
{
    mylog::info("Rescan VMFS file systems...");
    $storage_manager->RescanVmfs();
    mylog::info("Refresh storage system...");
    $storage_manager->RefreshStorageSystem();
};
if ($@)
{
    my $fault = $@;
    libvmware::DisplayFault("Failed to rescan storage", $fault);
    $success = 0;
}

if ($success)
{
    mylog::pass("Successfully rescanned $host_name");
}

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $success);
}

exit $success;

