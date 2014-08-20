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
    print "Get the WWPNs of the FC HBAs in an ESX host";
    Opts::usage();
    exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $result_address = Opts::get_option('result_address');
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

# Find the host
mylog::info("Searching for host $host_name");
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i}, properties => ['config']);
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}

my @wwns;
my $adapter_list = $vmhost->config->storageDevice->hostBusAdapter;
foreach my $adapter (@{$adapter_list})
{
    if (ref($adapter) =~ /FibreChannel/)
    {
        my $port_wwn = sprintf("%x", $adapter->portWorldWideName);
        my @pieces;
        for (my $i=0; $i < length($port_wwn); $i += 2)
        {
            push(@pieces, substr($port_wwn, $i, 2));
        }
        push(@wwns, join(":", @pieces));
    }
}

if ($csv || $bash)
{
    my $separator = ",";
    $separator = " " if ($bash);
    print join($separator, @wwns) . "\n";
}
else
{
    foreach my $wwn (@wwns)
    {
        mylog::info("  $wwn");
    }
}

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => @wwns);
}

exit 0;





