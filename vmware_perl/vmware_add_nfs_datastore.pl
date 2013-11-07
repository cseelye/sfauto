#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "user");
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
        help => "The hostname/IP of the host to rescan",
        required => 1,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    nfs_address => {
        type => "=s",
        help => "Address of the nfs datastore",
        required => 1,
    },
    nfs_path => {
        type => "=s",
        help => "The path on the nfs datastore to mount",
        required => 1,
    },
    nfs_local_path => {
        type => "=s",
        help => "The path on the vmhost to mount the nfs datastore. Can be just name and will default to /vmfs/volumes/nfs_local_path",
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
   print "Rescan iSCSI on an ESX host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $nfs_address = Opts::get_option('nfs_address');
my $nfs_path = Opts::get_option('nfs_path');
my $nfs_local_path = Opts::get_option('nfs_local_path');
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
eval
{
   Util::connect();
};
if ($@)
{
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

my @host_list;
# Find the host
mylog::info("Searching for host $host_name");
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i});
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}

push (@host_list, $vmhost);  
my $newDatastore;
eval 
{
    my $host_nfs_spec = HostNasVolumeSpec->new(
                                    accessMode => "readWrite",
                                    localPath => $nfs_local_path,
                                    remoteHost => $nfs_address,
                                    remotePath => $nfs_path);

    mylog::info("Adding the NFS datastore at $nfs_address$nfs_path with the following name $nfs_local_path");
    my $datastore_manager = Vim::get_view(mo_ref => $vmhost->configManager->datastoreSystem);
    $newDatastore = $datastore_manager->CreateNasDatastore(spec => $host_nfs_spec);

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

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}

mylog::pass("The new NFS datastore has been added");

exit 0;

