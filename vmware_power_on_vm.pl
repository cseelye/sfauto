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
        help => "The name of the virtual machine to power on",
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
   print "Power on a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');

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
    $vm_name = $vm->name;
    
    # Skip if it's not powered on
    if ($vm->runtime->powerState->val eq "poweredOn")
    {
        mylog::pass("$vm_name is already powered on");
        exit 0;
    }

    mylog::info("Powering on " . $vm_name);
    $vm->PowerOnVM()
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

mylog::pass("Successfully powered on $vm_name");
exit 0;
