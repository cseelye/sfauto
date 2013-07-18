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
Opts::set_option("server", "192.168.144.20");

my %opts = (
    mgmt_server => {
        type => "=s",
        help => "The hostname/IP of the vCenter Server (replaces --server)",
        required => 0,
        default => Opts::get_option("server"),
    },
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to get the IP address",
        required => 1,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    memory => {
        type => "",
        help => "Add the memory size to the disk size",
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
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $memory = Opts::get_option('memory');
my $result_address = Opts::get_option('result_address');
Opts::validate();

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

my $diskSize;
my $memorySize;
eval
{
    # Find the source VM
    mylog::info("Searching for VM $vm_name");
    my $vm_views = Vim::find_entity_views(view_type => 'VirtualMachine', properties => ['name','config.hardware.device'], filter => {'name' => qr/^$vm_name$/i});  
    foreach my $vm_view(sort{$a->name cmp $b->name} @$vm_views) {  
        my $vmname = $vm_view->{'name'};  
        my $devices = $vm_view->{'config.hardware.device'};  
        foreach my $device (@$devices) {  
            if($device->isa('VirtualDisk')) {  
                if($device->backing->isa('VirtualDiskFlatVer2BackingInfo')) {   
                    $diskSize = $device->capacityInKB;  
                }
            }
        }
    }
    my $vm_views = Vim::find_entity_views(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});  
    foreach (@$vm_views){  
        my $vmname = $_;
        $memorySize = $vmname->summary->config->memorySizeMB;
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

my $total_size = $diskSize;
my $output_message = "";
if($memory){
    $memorySize = $memorySize * 1024;
    $total_size = $diskSize + $memorySize;
    $output_message = "with $memorySize KB memory"
}


# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => $total_size);
}

mylog::pass("The disk size is $total_size KB for $vm_name $output_message");
exit 0;
