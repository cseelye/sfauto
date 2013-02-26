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
        help => "The name of the virtual machine to kill",
        required => 1,
    },
    ssh_user => {
        type => "=s",
        help => "The SSH username for the ESX host",
        required => 1,
    },
    ssh_pass => {
        type => "=s",
        help => "The SSH password for the ESX host",
        required => 1,
    },
    debug => {
        type => "",
        help => "Display more verbose messages",
        required => 0,
    },
);

Opts::add_options(%opts);

Opts::set_option("ssh_user", "root");
Opts::set_option("ssh_pass", "password");

if (scalar(@ARGV) < 1)
{
   print "Power off a virtual machine.";
   Opts::usage();
   exit 1;
}
Opts::parse();
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $ssh_user = Opts::get_option('ssh_user');
my $ssh_pass = Opts::get_option('ssh_pass');

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


my $vm_host;
eval
{
    # Find the source VM
    mylog::info("Searching for $vm_name...");
    my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$vm)
    {
        mylog::error("Could not find VM '$vm_name'");
        exit 1;
    }
    $vm_name = $vm->name;
    
    # Skip if it's not powered on
    if ($vm->runtime->powerState->val eq "poweredOff")
    {
        mylog::pass("$vm_name is already powered off");
        exit 0;
    }

    mylog::info("Finding the host $vm_name is running on...");
    my $host = Vim::get_view(mo_ref => $vm->runtime->host);
    $vm_host = $host->name;
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
eval
{
   mylog::debug("Disconnecting from vSphere");
   Util::disconnect();
};

if (!$vm_host)
{
    mylog::error("Could not find host for $vm_name");
    exit 1;
}

mylog::info("Killing " . $vm_name);

mylog::debug("Searching for $vm_name world ID on $vm_host");
my $result = `expect ssh.exp $vm_host $ssh_user $ssh_pass "esxcli vm process list"; echo \$?`;
my @lines = split (/\n/, $result);
my $ret = pop (@lines);
if ($ret != 0)
{
    mylog::error("Could not connect to ESX host: " . join ("\n", @lines));
    exit 1;
}
my $found = 0;
my $world_id = -1;
foreach my $line (@lines)
{
    # Remove trailing whitespace
    $line =~ s/\s+$//g;
    
    if ($line =~ /^$vm_name/)
    {
        $found = 1;
    }
    if ($found && $line =~ /World ID:\s+(\d+)/)
    {
        $world_id = $1;
        last;
    }
}
if ($world_id < 0)
{
    mylog::error("Could not find world ID for $vm_name");
    exit 1;
}
mylog::debug("Killing world ID $world_id");
$result = `expect ssh.exp $vm_host $ssh_user $ssh_pass "esxcli vm process kill -t force -w $world_id"; echo \$?`;
@lines = split (/\n/, $result);
$ret = pop (@lines);
if ($ret != 0)
{
    mylog::error("Could not kill $vm_name: " . join ("\n", @lines));
    exit 1;
}









#my $ssh = Net::OpenSSH->new($vm_host, user => "root", password => "password");
#if ($ssh->error)
#{
#    mylog::error($ssh->error);
#    exit 1;
#}
#my @lines = $ssh->capture("esxcli vm process list");
#my $found = 0;
#my $world_id = -1;
#for my $line (@lines)
#{
#    if ($line =~ /^$vm_name/)
#    {
#        $found = 1;
#    }
#    if ($found && $line =~ /World ID:\s+(\d+)/)
#    {
#        $world_id = $1;
#        last;
#    }
#}
#if ($world_id < 0)
#{
#    mylog::error("Could not find world ID for $vm_name");
#    exit 1;
#}
#
#if (!$ssh->system("esxcli vm process kill -t force -w $world_id"))
#{
#    mylog::error("Failed to kill $vm_name: " . $ssh->error);
#    exit 1;
#}




#my $ssh = Net::SSH::Perl->new($vm_host, port => 22, debug => 1);
#$ssh->login("root", "password");
#my ($stdout, $stderr, $ret) = $ssh->cmd("esxcli vm process list");
#if ($ret != 0)
#{
#    mylog::error("Could not list VMs on $vm_host: $stderr");
#    exit 1;
#}
#my $found = 0;
#my $world_id = -1;
#for my $line (split /\n/, $stdout)
#{
#    if ($line =~ /^$vm_name/)
#    {
#        $found = 1;
#    }
#    if ($found && $line =~ /World ID:\s+(\d+)/)
#    {
#        $world_id = $1;
#        last;
#    }
#}
#if ($world_id < 0)
#{
#    mylog::error("Could not find world ID for $vm_name");
#    exit 1;
#}
#($stdout, $stderr, $ret) = $ssh->cmd("esxcli vm process kill -t force -w $world_id");
#if ($ret != 0)
#{
#    mylog::error("Failed to kill $vm_name: $stderr");
#    exit 1;
#}

mylog::pass("Successfully killed $vm_name");
exit 0;
