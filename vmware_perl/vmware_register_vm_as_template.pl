#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_usr");
Opts::set_option("password", "solidfire");

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
        help => "The name of the virtual machine to register",
        required => 1,
    },
    vmhost => {
        type => "=s",
        help => "The name of the vm host",
        required => 1,
    },
    datastore_name => {
        type => "=s",
        help => "The name of the datastore to find the VM ",
        required => 1,
    },
    path => {
        type => "=s",
        help => "The path to the vmx or vmtx file. Ex: ubuntu-esx-gold/ubuntu-esx-gold.vmx",
        required => 1,
    },
    folder => {
        type => "=s",
        help => "The name of the folder to put the template in",
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
   print "Relocate (Storage vMotion) a VM to a new datastore";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $datastore_name = Opts::get_option('datastore_name');
my $host_name = Opts::get_option('vmhost');
my $path = Opts::get_option('path');
my $folder_name = Opts::get_option('folder');
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
my $new_vm;
eval {


    mylog::info("Registering VM $vm_name on $host_name in $folder_name");
    
    my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i});
    if (!$vmhost){
        mylog::error("Could not find host '$host_name'");
        exit 1;
    }

    my $datastore = Vim::find_entity_view(view_type => 'Datastore', filter => {'name' => qr/^$datastore_name$/i});
    if(!$datastore){
        mylog::error("Could not find the datastore '$datastore_name'");
        exit 1;
    }

    my $folder = Vim::find_entity_view(view_type => 'Folder', filter => {'name' => qr/^$folder_name$/i});
    if(!$folder){
        mylog::error("Could not find the folder '$folder_name'");
        exit 1;
    }

    my $new_path = "[$datastore_name] /$path";

    $new_vm = $folder->RegisterVM(path => $new_path, name => $vm_name, asTemplate => 1, host => $vmhost);

};
if ($@) {
    my $fault = $@;
    libsf::DisplayFault("Failed registering the VM ", $fault);

    exit 1;
}

mylog::pass("The $vm_name was registered on $host_name in $folder_name");
# Send the info back to parent script if requested
if (defined $result_address){
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;
