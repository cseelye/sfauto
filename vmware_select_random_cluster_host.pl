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
        help => "The name of the cluster to rescan all hosts in",
        required => 1,
    },
    csv => {
        type => "",
        help => "Display a minimal output that is formatted as a comma separated list",
        required => 0,
    },
    bash => {
        type => "",
        help => "Display a minimal output that is formatted as a space separated list",
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
   print "Rescan iSCSI on an ESX host";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $cluster_name = Opts::get_option('cluster');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $enable_debug = Opts::get_option('debug');
Opts::validate();

# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;
$mylog::Silent = 1 if ($bash || $csv);

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
my $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i}, properties => ['host']);
if (!$cluster)
{
    mylog::error("Could not find $cluster_name");
    exit 1;
}
mylog::info("Picking a random host");
my $host_count = scalar(@{$cluster->host});
mylog::debug("There are $host_count hosts in the cluster");
my $host_index = int(rand($host_count));
mylog::debug("Selecting host $host_index");
my $vmhost;
for my $host (@{$cluster->host})
{
    if ($host_index == 0)
    {
        $vmhost = Vim::get_view(mo_ref => $host, properties => ['name']);
        last;
    }
    $host_index--;
}
#my $vmhost = Vim::get_view(mo_ref => $cluster->host->[$host_index]);

if ($csv || $bash)
{
    print $vmhost->name . "\n";
}
else
{
    mylog::info("Selected host " . $vmhost->name);
}

exit 0;
