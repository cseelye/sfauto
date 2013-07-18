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
        help => "The name of the virtual machine that will be moved/cloned to the datastore",
        required => 1,
    },
    csv => {
        type => "",
        help => "Display a minimal output that is formatted as a comma separated list",
        required => 0,
    },
    bash => {
        type => "",
        help => "Display a minimal output that is formatted as a space separated list",
        required => 0,
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
   print "Select a datastore for a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $result_address = Opts::get_option('result_address');
Opts::validate();

# Turn on debug events if requested
$mylog::DisplayDebug = 1 if $enable_debug;
$mylog::Silent = 1 if ($bash || $csv);

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
    my $source_vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$source_vm)
    {
        mylog::error("Could not find VM '$vm_name'");
        exit 1;
    }
    $vm_name = $source_vm->name;

    # Assume the host the source VM is on has all of the datastores we care about
    # Make a list of SF disks by device ID
    mylog::info("Searching for datastores");
    my $host = $source_vm->runtime->host;
    $host = Vim::get_view(mo_ref => $host);
    my $luns = $host->config->storageDevice->scsiLun;
    my %sf_disks;
    for my $lun (@$luns)
    {
        if ($lun->vendor =~ /SolidFir/)
        {
            my $device_name = $lun->canonicalName;
            $sf_disks{$device_name} = 1;
        }
    }


    # Determine how much space the VM needs - committed disk + uncommitted disk + memory size
    my $vm_usage = $source_vm->summary->storage->committed / 1024 / 1024 + $source_vm->summary->storage->uncommitted / 1024 / 1024 + $source_vm->config->hardware->memoryMB;

    # Get a list of datastores
    my $datastores = Vim::find_entity_views(view_type => 'Datastore');

    # Find the first SF datastore with enough free space
    # We enough space for the VM + at least 80GB free in the datastore
    my $selected_datastore;
    foreach my $ds (@$datastores)
    {
        if ($ds->info->can("vmfs"))
        {
            my $device = $ds->info->vmfs->extent->[0]->diskName;
            if (!exists($sf_disks{$device}))
            {
                mylog::debug($ds->name . " is not a SolidFire device");
                next;
            }

            my $ds_free = $ds->summary->freeSpace / 1024 / 1024;
            mylog::debug($ds->name . " has " . $ds_free . " MB free");
            if ($ds_free - 80 * 1024 - $vm_usage > 0)
            {
                $selected_datastore = $ds;
                last;
            }
        }
        else
        {
            mylog::debug("Skipping " . $ds->name)
        }
    }
    if (!$selected_datastore)
    {
        mylog::error("Could not find a SolidFire datastore with enough free space");
        exit 1;
    }
    my $ds_name = $selected_datastore->name;
    if ($csv || $bash)
    {
        print "$ds_name\n";
    }
    else
    {
        mylog::info("Selected datastore $ds_name");
    }
    # Send the info back to parent script if requested
    if (defined $result_address)
    {
        libsf::SendResultToParent(result_address => $result_address, result => $ds_name);
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
