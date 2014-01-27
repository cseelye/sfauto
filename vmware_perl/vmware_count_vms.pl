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
    vm_prefix => {
        type => "=s",
        help => "The prefix of the virtual machines",
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
   print "Pick the next number in a sequence of VM names with a given prefix. E.g. myvm-01, myvm-02, ...";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_prefix = Opts::get_option('vm_prefix');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $result_address = Opts::get_option('result_address');
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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}


eval
{
    # Find the source VM
    mylog::info("Searching for VMs with prefix '$vm_prefix'");
    my $vm_list = Vim::find_entity_views(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_prefix/i});
    my $count = 0;
    foreach my $vm (@$vm_list)
    {
        # my $name = $vm->name;
        # mylog::debug("Found $name");
        # if ($name =~ /^${vm_prefix}0*(\d+)$/i)
        # {
        #     $count = $count + 1;
        # }
        my $name = $vm->name;
        mylog::debug("Found $name");
        if ($name =~ m/$vm_prefix/)
        {
            $count = $count + 1;
        }
    }

    # Show the next higher number
    if ($bash || $csv)
    {
        print "$count\n";
    }
    else
    {
        mylog::info("There are $count VMs that match prefix $vm_prefix");
    }
    # Send the info back to parent script if requested
    if (defined $result_address)
    {
        libsf::SendResultToParent(result_address => $result_address, result => $count);
    }
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

exit 0;
