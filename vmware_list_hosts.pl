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
    datacenter => {
        type => "=s",
        help => "Name of the datacenter to search",
        required => 0,
    },
    cluster_name => {
        type => "=s",
        help => "Name of ESX cluster to search",
        required => 0,
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

#if (scalar(@ARGV) < 1)
#{
#   print "Get a list of hosts in the specified cluster.";
#   Opts::usage();
#   exit 1;
#}

Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $dc_name = Opts::get_option("datacenter");
my $cluster_name = Opts::get_option('cluster_name');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
Opts::validate();

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

# Find the datacenter if specified
my $dc;
if ($dc_name)
{
    mylog::info("Searching for datastore $dc_name");
    $dc = Vim::find_entity_view(view_type => 'Datacenter', filter => { 'name' => $dc_name });
    if (!$dc)
    {
        mylog::error("Cannot find datacenter $dc_name");
        exit 1;
    }
}

my @host_names;
if ($cluster_name)
{
    # Find the cluster
    mylog::info("Searching for cluster $cluster_name");
    my $cluster_list;
    if ($dc)
    {
        $cluster_list = Vim::find_entity_views(view_type => 'ClusterComputeResource', begin_entity => $dc, filter => {'name' => qr/^$cluster_name$/i}, properties => ['parent', 'host']);
    }
    else
    {
        $cluster_list = Vim::find_entity_views(view_type => 'ClusterComputeResource', filter => {'name' => qr/^$cluster_name$/i}, properties => ['parent', 'host']);
    }
    if (!$cluster_list || scalar($cluster_list) <= 0)
    {
        mylog::error("Could not find cluster $cluster_name in datacenter $dc_name");
        exit 1;
    }

    my $cluster = $cluster_list->[0];

    # Get the host names from the MOR
    if ($cluster->host && scalar(@{$cluster->host}) > 0)
    {
        mylog::debug("Getting host details for " . scalar(@{$cluster->host}) . " cluster hosts");
        my $host_list = Vim::get_views(mo_ref_array => \@{$cluster->host}, properties => ['name']);
        foreach my $host (@{$host_list})
        {
            push @host_names, $host->name;
        }
    }
}
else
{
    mylog::info("Searching for hosts");
    my $host_mor_list;

    # Find hosts that are not in a cluster
    my $list = Vim::find_entity_views(view_type => 'ComputeResource');
    foreach my $h (@{$list})
    {
        if ($h->{mo_ref}->{type} eq "ComputeResource")
        {
            push @{$host_mor_list}, $h->{mo_ref}
        }
    }
    if ($host_mor_list and scalar(@$host_mor_list))
    {
        mylog::debug("Getting host details for " . scalar(@$host_mor_list) . " standalone hosts");
        my $host_list = Vim::get_views(mo_ref_array => \@{$host_mor_list}, properties => ['name']);
        foreach my $host (@{$host_list})
        {
            push @host_names, $host->name;
        }
    }

    # Find hosts that are in clusters
    my $cluster_list;
    if ($dc)
    {
        $cluster_list = Vim::find_entity_views(view_type => 'ClusterComputeResource', begin_entity => $dc, properties => ['host', 'name']);
    }
    else
    {
        $cluster_list = Vim::find_entity_views(view_type => 'ClusterComputeResource', properties => ['host', 'name']);
    }
    $host_mor_list = undef;
    foreach my $cluster (@{$cluster_list})
    {
        mylog::debug("Found cluster " . $cluster->name);
        if ($cluster->host and scalar(@{$cluster->host}))
        {
            foreach my $h (@{$cluster->host})
            {
                push @{$host_mor_list}, $h
            }
        }
    }
    if ($host_mor_list and scalar(@$host_mor_list))
    {
        mylog::debug("Getting host details for " . scalar(@$host_mor_list) . " cluster hosts");
        my $host_list = Vim::get_views(mo_ref_array => \@{$host_mor_list}, properties => ['name']);
        foreach my $host (@{$host_list})
        {
            push @host_names, $host->name;
        }
    }
}

# Display the results
if (scalar(@host_names) <= 0)
{
    mylog::info("There are no hosts");
}
else
{
    @host_names = sort @host_names;
    if ($bash || $csv)
    {
        my $separator = ",";
        $separator = " " if $bash;
        print join($separator, @host_names) . "\n";
    }
    else
    {
        foreach my $host (@host_names)
        {
            mylog::info("  $host");
        }
    }
}
exit 0;
