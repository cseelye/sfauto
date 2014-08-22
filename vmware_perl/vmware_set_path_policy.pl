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
    policy => {
        type => "=s",
        help => "The policy to set (fixed/rr/mru)",
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
   print "Set the path selection policy for the LUNS on a particular host\n";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $policy = Opts::get_option('policy');
my $enable_debug = Opts::get_option('debug');
my $result_address = Opts::get_option('result_address');
Opts::validate();

if ($policy !~ /fixed/i &&
    $policy !~ /rr/i &&
    $policy !~ /mru/)
{
    print "Please provide one of fixed/rr/mru for policy\n";
    exit 1;
}
$policy = "VMW_PSP_FIXED" if ($policy =~ /fixed/i);
$policy = "VMW_PSP_RR" if ($policy =~ /rr/i);
$policy = "VMW_PSP_MRU" if ($policy =~ /mru/i);

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
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i}, properties => ['configManager', 'config']);
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}
my $storage_manager = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);
#my $a = $storage_manager->QueryPathSelectionPolicyOptions();
#print Dumper($a) . "\n";
#exit 1;

# Convert LUN to cannonical name
my %lun2name;
foreach my $lun (@{$vmhost->config->storageDevice->scsiLun})
{
    $lun2name{$lun->key} = $lun->canonicalName;
}

# Find the SolidFire disks and set the multipath policy
my $allgood = 1;
foreach my $lun (@{$storage_manager->storageDeviceInfo->multipathInfo->lun})
{
    my $name = $lun2name{$lun->lun};
    # Skip non-SF devices
    if ($name !~ /f47acc/)
    {
        mylog::debug("Skipping $name");
        next;
    }
    
#    print Dumper($lun->policy) . "\n";
    if ($lun->policy->policy =~ $policy)
    {
        mylog::debug("$name is already using policy $policy");
        next;
    }

    mylog::debug("Setting path policy on $name");
#    print Dumper(HostMultipathInfoLogicalUnitPolicy->new(policy => $policy)) . "\n";
    eval
    {
        $storage_manager->SetMultipathLunPolicy(lunId => $lun->id,
                                                policy => HostMultipathInfoLogicalUnitPolicy->new(policy => $policy));
    };
    if ($@)
    {
        my $fault = $@;
        print Dumper($fault);
        libvmware::DisplayFault("Setting path policy on $name failed", $fault);
        $allgood = 0;
    }
    last;
}

if ($allgood)
{
    mylog::pass("Sucessfully set path policy on $host_name");
}
else
{
    mylog::error("Failed to set path policy on all volumes");
}

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $allgood);
}
exit $allgood;
