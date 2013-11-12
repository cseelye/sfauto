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

Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $dc_name = Opts::get_option("datacenter");
my $cluster_name = Opts::get_option('cluster_name');
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
   mylog::error("Could not connect to $vsphere_server: $!");
   exit 1;
}

my $service_content = Vim::get_service_content();
my @previous_tasks;
while (1)
{
    my $task_manager = Vim::get_view(mo_ref => $service_content->taskManager);
    if (!$task_manager->recentTask)
    {
        mylog::pass("There are no more running tasks");
        last;
    }
    my @incomplete_tasks;
    for my $task_ref (@{$task_manager->recentTask})
    {
        my $task = Vim::get_view(mo_ref => $task_ref);
        if (!$task->info->completeTime)
        {
            my $desc;
            for my $task_desc (@{$task_manager->description->methodInfo})
            {
                if ($task_desc->key eq $task->info->descriptionId)
                {
                    $desc = $task_desc->label;
                    last;
                }
            }
            push(@incomplete_tasks, $desc . " " . $task->info->entityName)
        }
    }
    
    if (scalar(@incomplete_tasks) <= 0)
    {
        mylog::pass("There are no more running tasks");
        last;
    }
    
    @incomplete_tasks = sort(@incomplete_tasks);
    if ("@incomplete_tasks" ne "@previous_tasks")
    {
        mylog::info("Waiting for " . scalar(@incomplete_tasks) . " tasks to complete");
        for my $t (@incomplete_tasks)
        {
            mylog::info("  " . $t);
        }
        sleep 10;
        next;
    }
    @previous_tasks = @incomplete_tasks;
}


# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => 1);
}
exit 0;
