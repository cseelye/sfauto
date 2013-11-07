#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_user");
Opts::set_option("password", "solidfire");

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
        help => "The hostname/IP of the host to configure",
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
   print "Setup CHAP credentials on an ESX host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
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

# Find the host
mylog::info("Searching for host $host_name");
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i}, properties => ['name', 'config', 'configManager']);
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}

# Get a reference to the storage system and find the iSCSI HBA
mylog::debug("Getting a reference to the storage manager");
my $storage = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem, properties => []);
my $iscsi_hba = libsf::VMwareFindIscsiHba($vmhost);
my $hba_name = $iscsi_hba->device;


# Remove all discovery portals
if ($iscsi_hba->configuredSendTarget)
{
    my $portal_count = scalar(@{$iscsi_hba->configuredSendTarget});
    mylog::info("Removing $portal_count discovery portals");
    eval
    {
        $storage->RemoveInternetScsiSendTargets(iScsiHbaDevice => $hba_name, targets => $iscsi_hba->configuredSendTarget);
    };
    if ($@)
    {
        libvmware::DisplayFault("Failed to remove discovery portals", $@);
        exit 1;
    }
}
else
{
    mylog::info("No discovery portals found");
}

# Remove configured/discovered static targets
if ($iscsi_hba->configuredStaticTarget)
{
    my $static_targets = scalar(@{$iscsi_hba->configuredStaticTarget});
    mylog::info("Removing $static_targets static targets");
    eval
    {
        $storage->RemoveInternetScsiStaticTargets(iScsiHbaDevice => $hba_name, targets => $iscsi_hba->configuredStaticTarget);
    };
    if ($@)
    {
        libvmware::DisplayFault("Failed to remove static targets", $@);
        exit 1;
    }
}
else
{
    mylog::info("No static targets found");
}

# Deconfigure CHAP
mylog::info("Resetting CHAP to default");
my $chap_spec = HostInternetScsiHbaAuthenticationProperties->new(
                                    chapAuthEnabled => 0, 
                                    chapAuthenticationType => "chapProhibited",
                                    mutualChapAuthenticationType => "chapProhibited",
);
eval
{
    $storage->UpdateInternetScsiAuthenticationProperties(iScsiHbaDevice => $hba_name, authenticationProperties => $chap_spec);
};
if ($@)
{
    libvmware::DisplayFault("Failed to reset CHAP settings", $@);
    exit 1;
}


# Rescan the iSCSI HBA
mylog::info("Rescanning iSCSI");
eval
{
    $storage->RescanHba(hbaDevice => $hba_name);
    $storage->RescanVmfs();
    $storage->RefreshStorageSystem();
};
if ($@)
{
    libvmware::DisplayFault("Failed to rescan iSCSI", $@);
    exit 1;
}


mylog::pass("Successfully cleaned iSCSI initiator on $host_name");
# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;

