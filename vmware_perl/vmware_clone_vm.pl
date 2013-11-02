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
    source_vm => {
        type => "=s",
        help => "The name of the virtual machine to clone",
        required => 1,
    },
    clone_name => {
        type => "=s",
        help => "The name of the clone to create",
        required => 1,
    },
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to put the clone on",
        required => 1,
    },
    datastore => {
        type => "=s",
        help => "Name of the datastore to put the clone in",
        required => 1,
    },
    folder => {
        type => "=s",
        help => "Name of the folder to put the clone in",
        required => 0,
    },
    thin => {
        type => "",
        help => "Clone to a thin provisioned VMDK instead of thick provisioned",
        required => 0,
    },
    result_address => {
        type => "=s",
        help => "Address of a ZMQ server listening for results (when run as a child process)",
        required => 0,
    },
    template => {
        type => "",
        help => "Make clone a template",
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
   print "Clone a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $source_vm_name = Opts::get_option('source_vm');
my $clone_name = Opts::get_option('clone_name');
my $dest_datastore_name = Opts::get_option('datastore');
my $dest_host_name = Opts::get_option('vmhost');
my $dest_folder_name = Opts::get_option('folder');
my $enable_debug = Opts::get_option('debug');
my $template = Opts::get_option('template');
my $thin_prov = Opts::get_option('thin');
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
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

# Find the source VM
mylog::info("Searching for source VM $source_vm_name");
my $source_vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$source_vm_name$/i});
if (!$source_vm)
{
    mylog::error("Could not find source VM '$source_vm_name'");
    exit 1;
}

# Find the destination folder
my $dest_folder;
if ($dest_folder_name)
{
   mylog::info("Searching for destination folder $dest_folder_name");
   $dest_folder = Vim::find_entity_view(view_type => 'Folder', filter => {'name' => qr/^$dest_folder_name$/i});
   if (!$dest_folder)
   {
       mylog::error("Could not find destination folder '$dest_folder_name'");
       exit 1;
   }
}

# Find the destination datastore
mylog::info("Searching for destination datastore $dest_datastore_name");
my $dest_datastore = Vim::find_entity_view(view_type => 'Datastore', filter => {'name' => qr/^$dest_datastore_name/i});
if (!$dest_datastore)
{
    mylog::error("Could not find destination datastore '$dest_datastore_name'");
    exit 1;
}

# Make sure there is enough free space in the destination
my $ds_free = $dest_datastore->summary->freeSpace / 1024 / 1024;
my $vm_usage = $source_vm->summary->storage->committed / 1024 / 1024 + $source_vm->summary->storage->uncommitted / 1024 / 1024 + $source_vm->config->hardware->memoryMB;
if ($vm_usage > $ds_free)
{
    mylog::error("There is not enough free space on $dest_datastore_name ($ds_free MB are available but $vm_usage MB are needed)");
    exit 1;
}

# Find the destination host and resource pool
mylog::info("Searching for destination host $dest_host_name");
my $dest_host = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$dest_host_name$/i});
if (!$dest_host)
{
    mylog::error("Could not find host '$dest_host_name'");
    exit 1;
}
mylog::debug("Finding parent cluster and root resource pool");
my $cluster = Vim::get_view(mo_ref => $dest_host->parent);
my $root_pool = $cluster->resourcePool;

#print Dumper($dest_datastore) . "\n";

# Start the clone
my $relocate_spec;
if ($thin_prov)
{
    $relocate_spec = VirtualMachineRelocateSpec->new(
                                datastore => $dest_datastore,
                                host => $dest_host,
                                pool => $root_pool,
    );
}
else
{
    $relocate_spec = VirtualMachineRelocateSpec->new(
                                datastore => $dest_datastore,
                                host => $dest_host,
                                pool => $root_pool,
                                transform => VirtualMachineRelocateTransformation->new('flat'),
    );
}
my $clone_spec;
if($template)
{

    $clone_spec = VirtualMachineCloneSpec->new(
                                powerOn => 0,
                                template => 1,
                                location => $relocate_spec
    );
}
else
{
    $clone_spec = VirtualMachineCloneSpec->new(
                            powerOn => 0,
                            template => 0,
                            location => $relocate_spec
    );
}
mylog::info("Starting clone...");
eval
{
    $source_vm->CloneVM(
                    folder => $dest_folder,
                    name => $clone_name,
                    spec => $clone_spec,
    );
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

<<<<<<< HEAD
mylog::pass("Successfully cloned $source_vm_name to $clone_name");
=======
mylog::pass("Sucessfully cloned $source_vm_name to $clone_name");

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
>>>>>>> ec1c333... vmware_clone_vm.pl: added result address option for ZMQ server, added option for cloning as a template
exit 0;
