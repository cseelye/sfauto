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
    vm_prefix => {
        type => "=s",
        help => "The prefix of the virtual machines",
        required => 1,
    },
    fill => {
        type => "",
        help => "Find the first gap in the sequence instead of the highest number",
        required => 0,
    },
    count => {
        type => "=i",
        help => "Get the next 'count' numbers in the sequence",
        required => 0,
        default => 1,
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
   print "Pick the next number in a sequence of VM names with a given prefix. E.g. myvm-01, myvm-02, ...";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $vm_prefix = Opts::get_option('vm_prefix');
my $fill_gap = Opts::get_option('fill');
my $count = Opts::get_option('count');
my $enable_debug = Opts::get_option('debug');
my $csv = Opts::get_option('csv');
my $bash = Opts::get_option('bash');
my $result_address = Opts::get_option('result_address');
Opts::validate();

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
    mylog::info("Searching for VMs with prefix '$vm_prefix'");
    my $vm_list = Vim::find_entity_views(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_prefix/i});
    my $highest = 0;
    my %found_numbers;
    foreach my $vm (@$vm_list)
    {
        my $name = $vm->name;
        mylog::debug("Found $name");
        if ($name =~ /^${vm_prefix}0*(\d+)$/i)
        {
            my $found = int($1);
            $found_numbers{$found} = 1;
            if ($found > $highest)
            {
                $highest = int($1);
            }
        }
    }
    
    my @seq;
    
    # Find the gaps in the sequence
    if ($fill_gap)
    {
        my $gap;
        my $previous;
        my @found = sort { $a <=> $b } keys %found_numbers;
        for (my $i = 1; $i <= scalar(@found); $i++)
        {
            if (!$previous)
            {
                $previous = $found[$i];
                next
            }
            if ($found[$i] != $previous + 1)
            {
                push(@seq, $previous+1..$found[$i]-1);
                $previous = $seq[-1] + 1;
                if ($previous  > $highest)
                {
                    $highest = $previous;
                }
            }
            else
            {
                $previous = $found[$i]
            }
        }
        if (scalar(@seq) <= 0)
        {
            mylog::info("There are no gaps in $vm_prefix");
            push (@seq, $highest+1);
            $highest++;
        }
        if (scalar(@seq) < $count)
        {
            push(@seq, $highest+1..$highest+1+$count-scalar(@seq));
        }
    }
    else
    {
        @seq = $highest+1..$highest+1+$count;
    }
    # Make sure the list is only the requested length
    $#seq = $count - 1;
    
    # Show the results
    if ($bash || $csv)
    {
        my $sep = " ";
        $sep = "," if $csv;
        print join($sep, @seq);
    }
    else
    {
        mylog::info("The next $count VM numbers for $vm_prefix are " . join(",", @seq));
    }
    # Send the info back to parent script if requested
    if (defined $result_address)
    {
        libsf::SendResultToParent(result_address => $result_address, result => @seq);
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
