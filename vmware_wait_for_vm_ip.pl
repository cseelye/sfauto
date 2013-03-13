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
    timeout => {
        type => "=i",
        help => "How long to wait before aborting (minutes)",
        required => 0,
        default => 5,
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
   print "Get the IP address of a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();

Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $wait_timeout = Opts::get_option('timeout');
my $batch = Opts::get_option('batch');

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
        mylog::error("VMware Tools are not installed in this VM; cannot detect VM IP address");
        exit 1;
    }
    if ($vm->guest->toolsStatus->val eq "toolsNotRunning")
    {
        mylog::error("VMware Tools are not running in this VM; cannot detect VM IP address");
        exit 1;
    }

    # Try to find an IP address
    mylog::info("Looking for 192 IP address");
    my $start_time = time();
    while (1)
    {
        my $vm_ip;
        if (defined $vm->guest && defined $vm->guest->net)
        {
            foreach my $net (@{$vm->guest->net})
            {
                if (defined $net->ipAddress)
                {
                    foreach my $ip (@{$net->ipAddress})
                    {
                        if ($ip =~ /^192/)
                        {
                            $vm_ip = $ip;
                            last;
                        }
                    }
                }
                else
                {
                    mylog::debug("$vm_name ipAddress is undefined")
                }
                last if $vm_ip;
            }
            # Quit if we couldn't find an IP - either the VM doesn't have one, it's not fully booted, VMware Tools not running, etc.
            if (!$vm_ip)
            {
                mylog::debug("Cannot read the IP address of " . $vm->name);
            }
            mylog::info("$vm_name IP address is $vm_ip");
            print "$vm_ip\n" if $batch;
            last;
        }
        else
        {
            mylog::debug("$vm_name does not have a defined guest/net object");
        }
        
        if (time() - $start_time > $wait_timeout*60)
        {
            mylog::error("Timed out waiting for IP address");
            exit 1;
        }
        sleep 10;
        $vm->update_view_data();
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
