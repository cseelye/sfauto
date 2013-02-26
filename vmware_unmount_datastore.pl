#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
    datastore => {
        type => "=s",
        help => "The name of the datastore to unmount",
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
   print "Unmount a datastore from all hosts";
   Opts::usage();
   exit 1;
}
Opts::parse();
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $datastore_name = Opts::get_option('datastore');
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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}


eval
{
    mylog::info("Searching for datastore '$datastore_name'...");
    my $ds = Vim::find_entity_view(view_type => 'Datastore', filter => {'name' => qr/^$datastore_name$/i});
    if (!$ds)
    {
        mylog::error("Could not find datastore '$datastore_name'");
        exit 1;
    }
    #use Data::Dumper;
    #print Dumper($ds) . "\n";
    #exit 1;
    
    mylog::info("Searching for connected hosts...");
    #my $disk_name = $ds->info->vmfs->extent->[0]->diskName;
    if (!$ds->host)
    {
        mylog::pass("$datastore_name is not mounted");
        exit 0;
    }
    foreach my $host_mor (@{$ds->host})
    {
        my $host = Vim::get_view(mo_ref => $host_mor->key, properties => ['name','configManager.storageSystem']);
        my $storage = Vim::get_view(mo_ref => $host->{'configManager.storageSystem'});
        my $fs_list = eval { $storage->fileSystemVolumeInfo->mountInfo || [] };
        foreach my $mount (@{$fs_list})
        {
            if ($mount->volume->name eq $datastore_name)
            {
                mylog::info("  Unmounting from " . $host->name);
                $storage->UnmountVmfsVolume(vmfsUuid => $mount->volume->uuid);
                last;
            }
        }
        
        # Detach SCSI LUN
#        my $device_list = eval{$storage->storageDeviceInfo->scsiLun || []};
#        foreach my $device (@{$device_list})
#        {
#            if ($device->cannonicalName eq $disk_name)
#            {
#                mylog::info("  Detaching from " . $host->name);
#                $storage->DetachScsiLun(lunUuid => $device->uuid);
#                last;
#            }
#        }
    }
};
if ($@)
{
    my $fault = $@;
    if (ref($fault) ne 'SoapFault')
    {
        mylog::error($fault);
        exit 1;
    }
    mylog::error(ref($fault->name) . ": " . $fault->fault_string);
    exit 1;
}

exit 0;
