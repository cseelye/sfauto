#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "script_user");
Opts::set_option("password", "solidfire");

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
        help => "The name of the virtual machine to relocate",
        required => 1,
    },
    key => {
        type => "=s",
        help => "The name of the parameter to set",
        required => 1,
    },
    value => {
        type => "=s",
        help => "The value to set the parameter to",
        required => 0,
        default => "same",
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
   print "Set a VM parameter (VMX file param)";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_name = Opts::get_option('vm_name');
my $key = Opts::get_option('key');
my $value = Opts::get_option('value');
my $enable_debug = Opts::get_option('debug');
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

# Find the VM
mylog::info("Searching for VM $vm_name");
my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i}, properties => []);
if (!$vm)
{
    mylog::error("Could not find VM '$vm_name'");
    exit 1;
}

my $extra_conf = OptionValue->new(key => $key, value => $value);
my $spec = VirtualMachineConfigSpec->new(extraConfig => [$extra_conf]);
mylog::info("Setting $key = $value on VM $vm_name");
eval
{
    my $task_ref = $vm->ReconfigVM_Task(spec => $spec);
    while (1)
    {
        my $task = Vim::get_view(mo_ref => $task_ref);
        my $state = $task->info->state->val;
        if ($state eq 'success')
        {
            last;
        }
        elsif ($state eq 'error')
        {
            my $soap_fault = SoapFault->new;
            $soap_fault->name($task->info->error->fault);
            $soap_fault->detail($task->info->error->fault);
            $soap_fault->fault_string($task->info->error->localizedMessage);
            libsf::DisplayFault("$vm_name reconfig failed", $soap_fault);
        }
        sleep 1;
    }
};
if ($@)
{
    my $fault = $@;
    libsf::DisplayFault("$vm_name reconfig failed", $fault);
    exit 1;
}



mylog::pass("Successfully set parameter on $vm_name");

# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;
