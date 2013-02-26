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
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to create datastores on",
        required => 1,
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
   print "Find new iSCSI volumes and create datastores on them";
   Opts::usage();
   exit 1;
}
Opts::parse();

Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $host_name = Opts::get_option('vmhost');
my $enable_debug = Opts::get_option('debug');

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

# Rescan the host
eval
{
    libsf::VMwareRescanIscsi($vmhost);
};
if ($@)
{
    my $fault = $@;
    libsf::DisplayFault("Rescan failed", $fault);
    exit 1;
}

# Find the iSCSI adapter in this host
mylog::info("Searching for iSCSI adapter on $host_name");
my $iscsi_hba = libsf::VMwareFindIscsiHba($vmhost);

# Map out LUN UUID <-> canonical name <-> iSCSI iqn 
mylog::info("Getting a list of SCSI LUNS...");
mylog::debug("Refreshing storage manager");
my $storage_manager = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);
mylog::debug("Building a map of LUNS");
my %lun2target;
for my $adapter (@{$storage_manager->storageDeviceInfo->scsiTopology->adapter})
{
    if ($adapter->adapter eq $iscsi_hba->key)
    {
        for my $target (@{$adapter->target})
        {
            for my $lun (@{$target->lun})
            {
                $lun2target{$lun->scsiLun} = $target->transport->iScsiName
            }
        }
    }
}
my %device2lun;
for my $disk (@{$storage_manager->storageDeviceInfo->scsiLun})
{
    $device2lun{$disk->canonicalName} = $disk->key;
}

# Find the disks without datastores and create them
eval
{
    mylog::debug("Getting a list of available disks");
    my $datastore_manager = Vim::get_view(mo_ref => $vmhost->configManager->datastoreSystem);
    my $disk_list = $datastore_manager->QueryAvailableDisksForVmfs();
    foreach my $disk (@{$disk_list})
    {
        my $options_list = $datastore_manager->QueryVmfsDatastoreCreateOptions(devicePath => $disk->devicePath);
        my $create_option = $options_list->[0];
    
        my $canonical_name = $create_option->spec->vmfs->extent->diskName;
        my $lun_name = $device2lun{$canonical_name};
        my $iqn = $lun2target{$lun_name}; 
    
        my @pieces = split(/\./, $iqn);
        my $datastore_name = pop @pieces;
        $datastore_name = pop(@pieces) . "." . $datastore_name;
    
        mylog::info("Creating datastore $datastore_name on disk $canonical_name...");
        $create_option->spec->vmfs->volumeName($datastore_name);
        my $newDatastore = $datastore_manager->CreateVmfsDatastore(spec => $create_option->spec);
    }
};
if ($@)
{
    my $fault = $@;
    libsf::DisplayFault("Creating datastore failed", $fault);
    exit 1;
}



mylog::pass("Successfully created datastores on $host_name");
exit 0;
