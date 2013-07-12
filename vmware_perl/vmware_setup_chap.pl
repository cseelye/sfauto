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
Opts::set_option("server", "192.168.144.20");

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
    chap_name => {
        type => "=s",
        help => "The chap name to use",
        required => 1,
    },
    chap_secret => {
        type => "=s",
        help => "The chap secret to use",
        required => 1,
    },
    chap_target => {
        type => "=s",
        help => "The chap target to use",
        required => 1,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    svip => {
        type => "=s",
        help => "Address of the cluster storage)",
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
my $chap_name = Opts::get_option('chap_name');
my $chap_secret = Opts::get_option('chap_secret');
my $chap_target = Opts::get_option('chap_target');
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


#sets up the chap settings: must use chap, not inherited from client, and sets name and secrets
my $chap_spec = HostInternetScsiHbaAuthenticationProperties->new(
                                    chapAuthEnabled => 1, 
                                    chapAuthenticationType => "chapRequired",
                                    chapInherited => 0,
                                    chapName => $chap_name, 
                                    chapSecret => $chap_secret, 
                                    mutualChapAuthenticationType => "chapRequired",
                                    mutualChapInherited => 0,
                                    mutualChapName => $chap_name, 
                                    mutualChapSecret => $chap_target);

#set up the settings for discovery
my $host_internet_scsi_hba_send_targets = HostInternetScsiHbaSendTarget->new(address => $svip, authenticationProperties => $chap_spec); 
my @host_set;
push (@host_set, $host_internet_scsi_hba_send_targets);

eval 
{
    my $storage = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);

    my $target_set = HostInternetScsiHbaTargetSet->new(sendTargets => \@host_set);

    #get the hba for the iscsi device
    my $iscsi_hba = libsf::VMwareFindIscsiHba($vmhost);
    my $iscsi_key = $iscsi_hba->key;
    my $rindex = rindex($iscsi_key, "-");
    my $vmhba = substr($iscsi_key, $rindex + 1);

    #update the iscsi storage settings
    mylog::info("Setting the CHAP settings on $vmhba \n CHAP Name: $chap_name  \n CHAP Secret: $chap_secret \n CHAP Target Secret: $chap_target");
    $storage->AddInternetScsiSendTargets(iScsiHbaDevice => $vmhba, targets => \@host_set);
    $storage->UpdateInternetScsiAuthenticationProperties(iScsiHbaDevice => $vmhba, authenticationProperties => $chap_spec, targetSet => $target_set);
    $storage->UpdateInternetScsiAuthenticationProperties(iScsiHbaDevice => $vmhba, authenticationProperties => $chap_spec);
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

mylog::pass("The CHAP settings have been updated");

exit 0;

