#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

my %opts = (
      datacenter => {
         type => "=s",
         help => "Name of the datacenter to search",
         required => 1,
      },
      folder => {
         type => "=s",
         help => "Name of vm folder to search",
         required => 0,
      },
      pool => {
         type => "=s",
         help => "Name of resource pool to search",
         required => 0,
      },
      cluster => {
         type => "=s",
         help => "Name of ESX cluster to search",
         required => 0,
      },
      recurse => {
         type => "",
         help => "Include VMs in subfolders/pools",
         required => 0,
      },
      batch => {
         type => "",
         help => "Display a minimal output that is suited for piping to other programs",
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
   print "Get a list of powered-on VMs in the specified folder.";
   Opts::usage();
   exit 1;
}

Opts::parse();
my $vsphere_server = Opts::get_option("server");
my $dc_name = Opts::get_option("datacenter");
my $folder_name = Opts::get_option("folder");
my $pool_name = Opts::get_option('pool');
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');
my $batch = Opts::get_option('batch');
my $recurse = Opts::get_option('recurse');
if ($folder_name && $pool_name)
{
   print STDERR "Please specify only one of folder or pool\n";
   exit 1;
}
if (!$folder_name and !$pool_name)
{
   print STDERR "Please specify either folder or pool\n";
   exit 1;
}
if (($pool_name && !$cluster_name) || ($cluster_name && !$pool_name))
{
   print STDERR "Please specify both pool and cluster\n";
   exit 1;
}
Opts::validate();

$mylog::DisplayDebug = 1 if $enable_debug;
$mylog::Silent = 1 if $batch;

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

# Find the specified datacenter
my $dc = Vim::find_entity_view(
            view_type => 'Datacenter',
            filter => { 'name' => $dc_name }
);
if (!$dc)
{
    mylog::error("Cannot find datacenter $dc_name");
    exit 1;
}

# Find the specified folder or pool
my @vm_list;
if ($folder_name)
{
    mylog::info("Searching for powered on VMs in folder $folder_name...");
    mylog::debug("Getting a list of matching folders");
    my $folder_views = Vim::find_entity_views(
            view_type => 'Folder',
            filter => { 'name' => $folder_name },
            properties => ['parent', 'name']
    );
    if (@$folder_views <= 0)
    {
        mylog::error("Could not find folder '$folder_name'");
        exit 1;
    }
    
    # Make sure the folder is in the correct datacenter
    mylog::debug("Comparing datacenter");
    my $folder;
    foreach my $f (@{$folder_views})
    {
        my $parent_dc = GetParentDatacenterName($f);
        if ($parent_dc eq $dc_name)
        {
            $folder = $f;
            last;
        }
    }
   
   mylog::debug("Getting a list of VMs");
   @vm_list = GetVmsInFolder($folder->{mo_ref}, $recurse)
}
if ($pool_name)
{
   mylog::info("Searching for powered on VMs in pool $pool_name on cluster $cluster_name...");   
   my $pool_views = Vim::find_entity_views(
            view_type => 'ResourcePool',
            filter => { 'name' => $pool_name }
   );
   if (@$pool_views <= 0)
   {
      mylog::error("Could not find pool '$pool_name'");
      exit 1;
   }
   my $pool;
   foreach my $p (@$pool_views)
   {
      # Compare this pool's root with the cluster root
      my $my_cluster = GetParentClusterName($p);
      if ($my_cluster eq $cluster_name)
      {
         $pool = $p;
         last;
      }
   }
   if (!$pool)
   {
      mylog::error("Could not find pool '$pool_name' on cluster '$cluster_name'");
      exit 1;
   }

   @vm_list = @{$pool->vm};
}

# Find all the VMs in the folder
mylog::debug("Enumerating VMs");
my %vms;
my $vm_objs = Vim::get_views(mo_ref_array => \@vm_list, properties => ['name', 'runtime']);
foreach my $vm (@$vm_objs)
{
   # Skip if it's not a VM
   next if !$vm->isa("VirtualMachine");

   my $vm_name = $vm->name;

   # Skip if it's not powered on
   if ($vm->runtime->powerState->val !~ /poweredOn/)
   {
      mylog::debug("$vm_name is not powered on");
      next;
   }
   
   $vms{$vm_name} = 1;    
}

eval
{
   mylog::debug("Disconnecting from vSphere");
   Util::disconnect();
};

# Display the results
foreach my $vm_name (sort keys %vms)
{
   mylog::info("  $vm_name");
}
print join(",", sort keys %vms) . "\n" if $batch;
exit 0;




sub GetParentClusterName
{
   my $pool = shift;
   my $parent_mo = $pool->parent;
   my $parent = Vim::get_view(mo_ref => $parent_mo);
   while ($parent_mo->{type} ne 'ClusterComputeResource')
   {
      $parent_mo = $parent->parent;
      $parent = Vim::get_view(mo_ref => $parent_mo);
   }
   return $parent->name;
}

sub GetParentDatacenterName
{
    my $start = shift;
    my $parent_mo = $start->parent;
    my $parent = Vim::get_view(mo_ref => $parent_mo);
    while ($parent_mo->{type} ne 'Datacenter')
    {
        $parent_mo = $parent->parent;
      $parent = Vim::get_view(mo_ref => $parent_mo);
    }
    return $parent->name;
}

sub GetVmsInFolder
{
    my ($folder_mor, $search_recursive) = @_;
    my $folder = Vim::get_view(mo_ref => $folder_mor, properties => ['childEntity']);
    
    my @vm_list;
    if ($folder->childEntity)
    {
        my @mo_list = @{$folder->childEntity};
        foreach my $mo (@mo_list)
        {
            #if ($mo->isa("VirtualMachine"))
            if ($mo->type eq "VirtualMachine")
            {
                push(@vm_list, $mo)
            }
            #if ($search_recursive && $mo->isa("Folder"))
            if ($search_recursive && $mo->type eq "Folder")
            {
                my @more = GetVmsInFolder($mo);
                if (scalar(@more) > 0)
                {
                    push(@vm_list, @more)
                }
            }
        }
    }
    return @vm_list;
}
