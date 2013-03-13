#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to get the IP address",
        required => 1,
    },
    network => {
        type => "=s",
        help => "The name of the virtual network to get the MAC address on",
        required => 0,
        default => "192.168.128.0 VM Network",
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
   print "Get the MAC address of a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();

Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $batch = Opts::get_option('batch');
my $vm_network = Opts::get_option('network');

# Turn on debug events if requested
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
   mylog::error("Could not connect to $vsphere_server: $@");
   exit 1;
}


eval
{
    # Find the source VM
    mylog::info("Searching for VM");
    my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$vm)
    {
        mylog::error("Could not find VM '$vm_name'");
        exit 1;
    }
    
    # Skip if it's not powered on
    if ($vm->runtime->powerState->val !~ /poweredOn/)
    {
        mylog::error("$vm_name is not powered on");
        exit 1;
    }
    
    # Quit if VMware tools are not installed and running
    if ($vm->guest->toolsStatus->val eq "toolsNotInstalled")
    {
        mylog::error("VMware Tools are not installed in this VM; cannot detect VM MAC address");
        exit 1;
    }
    if ($vm->guest->toolsStatus->val eq "toolsNotRunning")
    {
        mylog::error("VMware Tools are not running in this VM; cannot detect VM MAC address");
        exit 1;
    }

    # Try to find an IP address
    mylog::info("Looking for MAC address");
    my $vm_mac;
    if (defined $vm->guest && defined $vm->guest->net)
    {
        foreach my $net (@{$vm->guest->net})
        {
            next if (lc($net->network) ne lc($vm_network));
            
            $vm_mac = $net->macAddress;
            last;
        }
        # Quit if we couldn't find an IP - either the VM doesn't have one, it's not fully booted, VMware Tools not running, etc.
        if (!$vm_mac)
        {
            mylog::error("Cannot read the MAC address of " . $vm->name);
            exit 1;
        }
        mylog::info("$vm_name MAC address is $vm_mac");

        print "$vm_mac\n" if $batch;
    }
    else
    {
        mylog::error("$vm_name does not have a defined guest/net object");
        exit 1;
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
