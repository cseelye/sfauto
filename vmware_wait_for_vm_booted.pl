#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "QA\\script_user");
Opts::set_option("password", "password");

my %opts = (
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to wait for",
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
   print "Wait for a virtual machine to be fully booted and healthy.";
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
        mylog::error("Could not find source VM '$vm_name'");
        exit 1;
    }
    
    mylog::info("Checking VM health");
    my $previous_status = "";
    my $status = "";
    
    # Wait for the VM to be powered on
    $status = $vm->runtime->powerState->val;
    $previous_status = "";
    while ($status ne "poweredOn")
    {
        if ($status ne $previous_status)
        {
            mylog::info("  VM is " . $status);
            $previous_status = $status;
        }
        mylog::debug("Refreshing view");
        $vm->update_view_data();
        $status = $vm->runtime->powerState->val;
        mylog::debug($status);
        sleep 10 if ($status ne "poweredOn");
    }
    mylog::info("  VM is poweredOn");
                
    # See if VMware tools are installed
    if ($vm->guest->toolsStatus->val eq "toolsNotInstalled")
    {
        mylog::warn("VMware Tools are not installed in this VM; cannot detect VM boot/health");
        exit 0;
    }
    
    $previous_status = "";
    $status = $vm->guestHeartbeatStatus->val;
    while ($status ne "green")
    {
        if ($status ne $previous_status)
        {
            mylog::info("  VM heartbeat is " . $status);
            $previous_status = $status;
        }
        mylog::debug("Refreshing view");
        $vm->update_view_data();
        $status = $vm->guestHeartbeatStatus->val;
        mylog::debug($status);
        sleep 10 if ($status ne "green");
    }
    mylog::info("  VM heartbeat is green");
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

mylog::pass("$vm_name is fully booted and healthy");
exit 0;

