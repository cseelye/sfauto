#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;

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
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to register",
        required => 1,
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
   #print "Relocate (Storage vMotion) a VM to a new datastore";
   #$mylog::info("");
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $result_address = Opts::get_option('result_address');
my $enable_debug = Opts::get_option('debug');
Opts::validate();



# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;

# Turn off cert validation so we can get away with self signed certs
mylog::debug("Disabling SSL cert verification");
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

# Connect to vSphere
mylog::info("Connecting to vSphere at $vsphere_server...");
eval{
   Util::connect();
};
if ($@){
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

eval {

    mylog::info("Destroying VM '$vm_name'");
    
    my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$vm){
        mylog::error("Could not find the VM '$vm_name'");
        exit 1;
    }

    #$vm->UnregisterVM;
    $vm->Destroy();


};
if ($@) {
    my $fault = $@;
    libvmware::DisplayFault("Failed destroying the VM ", $fault);

    exit 1;
}

mylog::pass("The $vm_name was destroyed");
# Send the info back to parent script if requested
if (defined $result_address){
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;
