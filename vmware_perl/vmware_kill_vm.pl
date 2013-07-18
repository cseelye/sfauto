#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_usr");
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
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to kill",
        required => 1,
    },
    host_user => {
        type => "=s",
        help => "The SSH username for the ESX host",
        required => 0,
        default => "root",
    },
    host_pass => {
        type => "=s",
        help => "The SSH password for the ESX host",
        required => 0,
        default => "solidfire",
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
   print "Power off a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $ssh_user = Opts::get_option('host_user');
my $ssh_pass = Opts::get_option('host_pass');
my $result_address = Opts::get_option('result_address');
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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}


my $vm_host;
my $host_ip;
eval
{
    # Find the source VM
    mylog::info("Searching for $vm_name...");
    my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$vm)
    {
        mylog::error("Could not find VM '$vm_name'");
        exit 1;
    }
    $vm_name = $vm->name;

    # Skip if it's not powered on
    if ($vm->runtime->powerState->val eq "poweredOff")
    {
        mylog::pass("$vm_name is already powered off");
        exit 0;
    }

    mylog::info("Finding the host $vm_name is running on...");
    my $host = Vim::get_view(mo_ref => $vm->runtime->host, properties => ['name', 'configManager']);
    $vm_host = $host->name;

    # Get the first management IP of the host
    my $netsys = Vim::get_view(mo_ref => $host->configManager->networkSystem, properties => ['networkInfo']);
    foreach my $vnic (@{$netsys->networkInfo->vnic})
    {
        $host_ip = $vnic->spec->ip->ipAddress;
        last;
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
eval
{
   mylog::debug("Disconnecting from vSphere");
   Util::disconnect();
};

if (!$vm_host)
{
    mylog::error("Could not find host for $vm_name");
    exit 1;
}

mylog::info("Killing " . $vm_name . " on $vm_host");

mylog::debug("Searching for $vm_name world ID on $vm_host");
my $command = "esxcli vm process list | grep -A1 '^$vm_name' | tail -1 | awk '{print \$3}' 2>&1";
my ($return_code, $stdout) = libsf::SshCommand(client_ip => $host_ip, client_user => $ssh_user, client_pass => $ssh_pass, command => $command);
if ($return_code != 0)
{
    mylog::error("Could not run esxcli: $stdout");
    exit 1;
}
my $world_id = int($stdout);
if ($world_id <= 0)
{
    mylog::error("Could not find VM world ID");
    exit 1;
}
$command = "esxcli vm process kill -t force -w $world_id 2>&1";
($return_code, $stdout) = libsf::SshCommand(client_ip => $host_ip, client_user => $ssh_user, client_pass => $ssh_pass, command => $command);
if ($return_code != 0)
{
    mylog::error("Could not run kill $vm_name: $stdout");
    exit 1;
}

mylog::pass("Successfully killed $vm_name");
exit 0;
