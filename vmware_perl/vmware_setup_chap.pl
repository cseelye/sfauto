#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "sfauto");
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
        help => "The hostname/IP of the host to configure",
        required => 1,
    },
    chap_name => {
        type => "=s",
        help => "The chap usernamename to use",
        required => 1,
    },
    init_secret => {
        type => "=s",
        help => "The initiator secret to use",
        required => 1,
    },
    targ_secret => {
        type => "=s",
        help => "The target secret to use",
        required => 0,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    svip => {
        type => "=s",
        help => "Discovery address of the iSCSI storage",
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
   print "Setup CHAP credentials on an ESX host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $chap_name = Opts::get_option('chap_name');
my $init_secret = Opts::get_option('init_secret');
my $targ_secret = Opts::get_option('targ_secret');
my $svip = Opts::get_option('svip');
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

my $chap_spec;
# CHAP settings for mutual CHAP if specified
if ($targ_secret)
{
    $chap_spec = HostInternetScsiHbaAuthenticationProperties->new(
                                        chapAuthEnabled => 1, 
                                        chapAuthenticationType => "chapRequired",
                                        chapInherited => 0,
                                        chapName => $chap_name, 
                                        chapSecret => $init_secret, 
                                        mutualChapAuthenticationType => "chapRequired",
                                        mutualChapInherited => 0,
                                        mutualChapName => $chap_name,
                                        mutualChapSecret => $targ_secret,
    );
}
# CHAP settings for one-way CHAP
else
{
    $chap_spec = HostInternetScsiHbaAuthenticationProperties->new(
                                        chapAuthEnabled => 1, 
                                        chapAuthenticationType => "chapRequired",
                                        chapInherited => 0,
                                        chapName => $chap_name, 
                                        chapSecret => $init_secret, 
                                        mutualChapAuthenticationType => "chapProhibited",
    );
}

# See if the target address already exists on the host and update it
mylog::debug("Looking for existing send targets");
if ($iscsi_hba->configuredSendTarget)
{
    for my $st (@{$iscsi_hba->configuredSendTarget})
    {
        if ($st->address eq $svip)
        {
            # Update the existing target
            mylog::info("Updating auth on $svip");
            eval
            {
                my $target_set = HostInternetScsiHbaTargetSet->new(sendTargets => [$st]);
                $storage->UpdateInternetScsiAuthenticationProperties(iScsiHbaDevice => $hba_name, authenticationProperties => $chap_spec, targetSet => $target_set);
            };
            if ($@)
            {
                libvmware::DisplayFault("Failed to update target auth", $@);
                exit 1;
            }
            mylog::pass("Successfully updated CHAP settings for $svip");
            # Send the info back to parent script if requested
            if (defined $result_address)
            {
                libsf::SendResultToParent(result_address => $result_address, result => 1);
            }
            exit 0;
        }
    }
}

# Add the target if it did not exist
mylog::info("Adding $svip");
my $send_target = HostInternetScsiHbaSendTarget->new(address => $svip, authenticationProperties => $chap_spec);
eval
{
    $storage->AddInternetScsiSendTargets(iScsiHbaDevice => $hba_name, targets => [$send_target]);
};
if ($@)
{
    libvmware::DisplayFault("Failed to add target auth", $@);
    exit 1;
}
mylog::pass("Successfully added CHAP settings for $svip");
# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;

