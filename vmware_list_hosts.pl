#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
      datacenter => {
         type => "=s",
         help => "Name of the datacenter to search",
         required => 1,
      },
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
   print "Get a list of hosts in the specified cluster.";
   Opts::usage();
   exit 1;
}

Opts::parse();
Opts::validate();
my $vsphere_server = Opts::get_option("server");
my $dc_name = Opts::get_option("datacenter");
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');
my $batch = Opts::get_option('batch');

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

# Find the specified datacenter
my $dc = Vim::find_entity_view(
            view_type => 'Datacenter',
            filter => { 'name' => $dc_name }
);
if (!$dc)
{
    mylog::error("Cannot find datacenter $dc_name");
    exit 1;
}

# Find the cluster and make sure it is in the datacenter
mylog::info("Searching for cluster $cluster_name");
my $cluster_list = Vim::find_entity_views(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i}, properties => ['parent', 'host']);
my $cluster;
foreach my $c (@{$cluster_list})
{
    if (libsf::VMwareGetParentDatacenterName($c) =~ /$dc_name/i)
    {
        $cluster = $c;
        last;
    }
}
if (!$cluster)
{
    mylog::error("Could not find $cluster_name in datacenter $dc_name");
    exit 1;
}

# Find the hosts in the cluster
mylog::info("Searching for hosts in cluster");
my $host_list = Vim::get_views(mo_ref_array => \@{$cluster->host}, properties => ['name']);
my @host_names;
foreach my $host (@{$host_list})
{
    push @host_names, $host->name;
}

# Sort and display
@host_names = sort @host_names;
foreach my $host (@host_names)
{
    mylog::info("  $host");
}
print join(",", @host_names) . "\n" if $batch;
exit 0;









