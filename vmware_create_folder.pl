#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
    folder => {
        type => "=s",
        help => "The name of the new folder to create",
        required => 1,
    },
    parent => {
        type => "=s",
        help => "The name of the parent folder to create the new folder in",
        required => 1,
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
   print "Wait for a vSphere folder.";
   Opts::usage();
   exit 1;
}
Opts::parse();
Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $folder_name = Opts::get_option('folder');
my $parent_name = Opts::get_option('parent');
my $enable_debug = Opts::get_option('debug');

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


eval
{
    # Find the source VM
    mylog::info("Searching for parent folder");
    my $parent = Vim::find_entity_view(view_type => 'Folder', filter => {'name' => qr/^$parent_name$/i});
    if (!$parent)
    {
        mylog::error("Could not find folder '$parent'");
        exit 1;
    }

    # See if the folder already exists in the requested location
    foreach my $child_mor (@{$parent->childEntity})
    {
        my $child = Vim::get_view(mo_ref => $child_mor, properties => ['name']);
        if ($child->isa("Folder") && $child->name eq $folder_name)
        {
            mylog::pass("$folder_name already exists in $parent_name");
            exit 0;
        }
    }

    mylog::info("Creating $folder_name");
    $parent->CreateFolder(name => $folder_name);
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

mylog::pass("Created folder $folder_name");
exit 0;
