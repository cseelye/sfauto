#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

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
    cluster => {
        type => "=s",
        help => "Name of ESX cluster to search",
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
   print "Check the health of datastores on a given cluster.";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');
my $result_address = Opts::get_option('result_address');
Opts::validate();

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

# Find the cluster
mylog::info("Searching for cluster $cluster_name");
my $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i}, properties => ['datastore', 'host']);
if (!$cluster)
{
    mylog::error("Could not find $cluster_name");
    exit 1;
}

# Get the info about the hosts in the cluster
my $host_count = scalar(@{$cluster->host});
my %hosts;
for my $hr (@{$cluster->host})
{
    my $host = Vim::get_view(mo_ref => $hr, properties => ['name']);
    $hosts{$hr} = $host->name;
}

# Make a table of datastores that are on iscsi volumes
mylog::info("Querying storage configuration");

my $host = Vim::get_view(mo_ref => $cluster->host->[0], properties => ['configManager']);
my $storage_manager = Vim::get_view(mo_ref => $host->configManager->storageSystem, properties => ['storageDeviceInfo', 'fileSystemVolumeInfo']);
my %sf_disks;
for my $disk (@{$storage_manager->storageDeviceInfo->scsiLun})
{
    # Skip disks that are not solidfire
    next if $disk->vendor !~ /solidfir/i;

    $sf_disks{$disk->canonicalName} = 1;
}
my %sf_datastores;
for my $mount (@{$storage_manager->fileSystemVolumeInfo->mountInfo})
{
    if ($mount->volume->type !~ /VMFS/i)
    {
        if (!$mount->volume->name || length($mount->volume->name) <= 0)
        {
            mylog::debug("Skipping " . $mount->mountInfo->path . " because it is not a VMFS volume");
        }
        else
        {
            mylog::debug("Skipping " . $mount->volume->name . " because it is not a VMFS volume");
        }
        next;
    }
    if (!$sf_disks{$mount->volume->extent->[0]->diskName})
    {
        if (!$mount->volume->name || length($mount->volume->name) <= 0)
        {
            mylog::debug("Skipping " . $mount->mountInfo->path . " because it is not on a solidfire volume");
        }
        else
        {
            mylog::debug("Skipping " . $mount->volume->name . " because it is not on a solidfire volume");
        }
        next;
    }
    $sf_datastores{$mount->volume->name} = 1;
}

# Go through each datastore and look at it's status
my $allgood = 1;
my $datastore_list = Vim::get_views(mo_ref_array => $cluster->datastore, properties => ['host', 'overallStatus', 'name']);
for my $ds (@{$datastore_list})
{
    # Skip non-SF datastores
    next if (!$sf_datastores{$ds->name});

    if (lc($ds->overallStatus->val) ne "green")
    {
        mylog::error("  " . $ds->name . " overallStatus is " . $ds->overallStatus->val);
        $allgood = 0;
    }
    
    if (scalar(@{$ds->host}) != $host_count)
    {
        mylog::error("  " . $ds->name . " does not have the correct number of host mounts");
        $allgood = 0;
    }
    
    for my $mount (@{$ds->host})
    {
        my $host_ref = $mount->key;
        if (!$mount->mountInfo->accessible)
        {
            my $out = "  " . $ds->name . " is not accessible on " . $hosts{$host_ref};
            if ($mount->mountInfo->inaccessibleReason)
            {
                $out .= ": " . $mount->mountInfo->inaccessibleReason;
            }
            mylog::error($out);
            $allgood = 0;
        }
        if (!$mount->mountInfo->mounted)
        {
            mylog::error("  " . $ds->name . " is not mounted on " . $hosts{$host_ref});
            $allgood = 0;
        }
    }
}

if ($allgood)
{
    mylog::pass("All datastores are healthy");
}
else
{
    mylog::error("Not all datastores are healthy");
}


# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $allgood);
}
exit $allgood;
