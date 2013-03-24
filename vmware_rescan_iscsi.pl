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
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to rescan",
        required => 0,
    },
    cluster => {
        type => "=s",
        help => "The name of the cluster to rescan all hosts in",
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
my $host_name = Opts::get_option('vmhost');
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');

if ($host_name && $cluster_name)
{
   print STDERR "Please specify only one of vmhost or cluster\n";
   exit 1;
}
if (!$host_name && !$cluster_name)
{
   print STDERR "Please specify either vmhost or cluster\n";
   exit 1;
}

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
if ($cluster_name)
{
    mylog::info("Searching for cluster $cluster_name");
    my $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i});
    if (!$cluster)
    {
        mylog::error("Could not find $cluster_name");
        exit 1;
    }
    mylog::info("Searching for hosts in cluster");
    foreach my $vmhost (@{$cluster->host})
    {
        push @host_list, Vim::get_view(mo_ref => $vmhost);
    }
}
elsif ($host_name)
{
    # Find the host
    mylog::info("Searching for host $host_name");
    my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i});
    if (!$vmhost)
    {
        mylog::error("Could not find host '$host_name'");
        exit 1;
    }
    push @host_list, $vmhost;
}

foreach my $vmhost (@host_list)
{
    eval
    {
        libsf::VMwareRescanIscsi($vmhost)
    };
    if ($@)
    {
        my $fault = $@;
        libsf::DisplayFault("Rescan failed", $fault);
        exit 1;
    }
}

mylog::pass("Successfully rescanned");
exit 0;
