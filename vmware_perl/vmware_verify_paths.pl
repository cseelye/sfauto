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
    vmhost => {
        type => "=s",
        help => "The name of the host to verify volumes/paths on",
        required => 1,
    },
    expected_paths => {
        type => "=i",
        help => "The expected number of paths per volume",
        required => 1,
    },
    expected_volumes => {
        type => "=i",
        help => "The expected number of volumes on the host",
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
    print "Check the count of volumes and paths per volume on a host";
    Opts::usage();
    exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $expected_paths = Opts::get_option('expected_paths');
my $expected_volumes = Opts::get_option('expected_volumes');
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

mylog::info("Getting a list of volumes and paths...");

my $allgood = 1;

my %lun2multipath;
if (!$vmhost->config->storageDevice->multipathInfo->lun)
{
    mylog::error("No multipath LUNs detected");
    $allgood = 0;
}
else
{
    foreach my $mp (@{$vmhost->config->storageDevice->multipathInfo->lun})
    {
        $lun2multipath{$mp->lun} = $mp;
    }
}

my $volume_count = 0;
my $total_paths = 0;
my $total_unhealthy_paths = 0;
foreach my $lun (@{$vmhost->config->storageDevice->scsiLun})
{
    # Skip non-SolidFire devices
    next if ($lun->vendor ne "SolidFir");

    $volume_count++;

    my $key = $lun->key;
    my $mp = $lun2multipath{$key};
    my @pieces = split(/\./, $lun->canonicalName);
    my $disk_serial = pop @pieces;
    my $volume_id = substr($disk_serial, 24, 8);
    $volume_id =~ s/^0+//;
    $volume_id = hex($volume_id);

    my $volume_paths = scalar (@{$mp->path});
    $total_paths += $volume_paths;
    if ($volume_paths < $expected_paths)
    {
        mylog::error("Volume " . $lun->canonicalName . " (volumeID " . $volume_id . ") only has " . $volume_paths . " paths but expected " . $expected_paths);
        $allgood = 0;
    }
    #else
    #{
    #    mylog::info("Volume " . $lun->canonicalName . " (volumeID " . $volume_id . ") has " . $volume_paths . " paths");
    #}
    my $unhealthy_paths = 0;
    foreach my $path (@{$mp->path})
    {
        if ($path->{state} ne "active")
        {
            $unhealthy_paths++;
        }
    }
    $total_unhealthy_paths += $unhealthy_paths;
    if ($volume_paths - $unhealthy_paths < $expected_paths)
    {
        mylog::error("Volume " . $lun->canonicalName . " (volumeID " . $volume_id . ") only has " . ($volume_paths - $unhealthy_paths) . " healthy paths but expected " . $expected_paths);
        $allgood = 0;
    }
    
    foreach my $message (@{$lun->operationalState})
    {
        if ($message =~ /error/i)
        {
            mylog::error("Volume " . $lun->canonicalName . " (volumeID " . $volume_id . ") is in an error state");
            $allgood = 0;
        }
    }
}
if ($volume_count < $expected_volumes)
{
    mylog::error("Found $volume_count volumes but expected $expected_volumes");
    $allgood = 0;
}
else
{
    mylog::pass("Found $volume_count volumes");
}

if ($expected_volumes * $expected_paths > $total_paths)
{
    mylog::error("Found $total_paths total paths");
}
else
{
    mylog::pass("Found $total_paths total paths");
}
if ($total_unhealthy_paths > 0)
{
    mylog::error("Found $total_unhealthy_paths unhealthy paths");
}



# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $allgood);
}

if ($allgood)
{
    mylog::pass("All volumes and paths are present with the expected number of healthy paths");
    exit 0;
}
else
{
    exit 1;
}
