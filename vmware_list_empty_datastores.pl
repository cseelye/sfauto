#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
      cluster => {
         type => "=s",
         help => "Name of ESX cluster to search",
         required => 0,
      },
      batch => {
         type => "",
         help => "Display a minimal output that is suited for piping to other programs",
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
   print "Get a list of powered-on VMs in the specified folder.";
   Opts::usage();
   exit 1;
}

Opts::parse();
my $vsphere_server = Opts::get_option("server");
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');
my $batch = Opts::get_option('batch');
Opts::validate();

$mylog::DisplayDebug = 1 if $enable_debug;
$mylog::Silent = 1 if $batch;

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

mylog::info("Searching for cluster $cluster_name");
my $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i}, properties => ['datastore', 'host']);
if (!$cluster)
{
    mylog::error("Could not find $cluster_name");
    exit 1;
}

# Make a table of datastores that are on iscsi volumes
my $host = Vim::get_view(mo_ref => $cluster->host->[0], properties => ['configManager']);
my $storage_manager = Vim::get_view(mo_ref => $host->configManager->storageSystem, properties => ['storageDeviceInfo', 'fileSystemVolumeInfo']);
my %sf_disks;
for my $disk (@{$storage_manager->storageDeviceInfo->scsiLun})
{
    # Skip disks that are not solidfire
    next if $disk->vendor !~ /solidfir/i;

    #print $disk->displayName . "\n";
    $sf_disks{$disk->canonicalName} = 1;
}
my %sf_datastores;
for my $mount (@{$storage_manager->fileSystemVolumeInfo->mountInfo})
{
    #print $mount->volume->name . "\n";
    
    if ($mount->volume->type !~ /VMFS/i)
    {
        mylog::debug("Skipping " . $mount->volume->name . " because it is not a VMFS volume");
        next;
    }
    if (!$sf_disks{$mount->volume->extent->[0]->diskName})
    {
        if (!$mount->volume->name)
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


my @empty_ds;
my $datastore_list = Vim::get_views(mo_ref_array => $cluster->datastore, properties => ['vm', 'name']);
for my $ds (@{$datastore_list})
{
    # Skip non-SF datastores
    next if (!$sf_datastores{$ds->name});
    
    if (!$ds->vm || scalar $ds->vm <= 0)
    {
        mylog::debug($ds->name . " has no VMs in it");
        push (@empty_ds, $ds->name);
        next;
    }
    
    mylog::debug($ds->name . " has " . scalar(@{$ds->vm}) . " VMs in it");
}


@empty_ds = sort @empty_ds;
foreach my $ds (@empty_ds)
{
    mylog::info("  $ds");
}
print join(',', @empty_ds) . "\n" if $batch;




