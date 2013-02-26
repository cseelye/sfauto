#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
    vm_prefix => {
        type => "=s",
        help => "The prefix of the virtual machines",
        required => 1,
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
   print "Pick the next number in a sequence of VM names with a given prefix. E.g. myvm-01, myvm-02, ...";
   Opts::usage();
   exit 1;
}
Opts::parse();
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_prefix = Opts::get_option('vm_prefix');
my $enable_debug = Opts::get_option('debug');
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
    mylog::info("Searching for VMs with prefix '$vm_prefix'");
    my $vm_list = Vim::find_entity_views(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_prefix/i});
    my $number = 0;
    foreach my $vm (@$vm_list)
    {
        my $name = $vm->name;
        mylog::debug("Found $name");
        if ($name =~ /^${vm_prefix}0*(\d+)$/i)
        {
            if (int($1) > $number)
            {
                $number = int($1);
            }
        }
    }
    $number++;
    mylog::info("The next VM number for $vm_prefix is $number");
    
    print "$number\n" if $batch;
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
