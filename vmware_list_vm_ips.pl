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
   print "Get a list of powered-on VMs and their IP addresses in the specified folder.";
   Opts::usage();
   exit 1;
}

Opts::parse();
my $vsphere_server = Opts::get_option("server");
my $folder_name = Opts::get_option("folder");
my $pool_name = Opts::get_option('pool');
my $cluster_name = Opts::get_option('cluster');
my $enable_debug = Opts::get_option('debug');
my $batch = Opts::get_option('batch');
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

# Find the specified folder or pool
my @vm_list;
if ($folder_name)
{
   mylog::info("Searching for powered on VMs in folder $folder_name...");
   my $folder_views = Vim::find_entity_views(
            view_type => 'Folder',
            filter => { 'name' => $folder_name }
   );
   if (@$folder_views <= 0)
   {
      mylog::error("Could not find folder '$folder_name'");
      exit 1;
   }
   @vm_list = @{@{$folder_views}[0]->childEntity};
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

# Find all the VMs in the folder with IP addresses
my %vms;
foreach my $vm_mo (@vm_list)
{
   my $vm = Vim::get_view(mo_ref => $vm_mo);

   # Skip if it's not a VM
   next if !$vm->isa("VirtualMachine");

   my $vm_name = $vm->name;

   # Skip if it's not powered on
   if ($vm->runtime->powerState->val !~ /poweredOn/)
   {
      mylog::debug("$vm_name is not powered on");
      next;
   }
   
   # Try to find an IP address
   my $vm_ip;
   if (defined $vm->guest && defined $vm->guest->net)
   {
      foreach my $net (@{$vm->guest->net})
      {
        if (defined $net->ipAddress)
        {
         foreach my $ip (@{$net->ipAddress})
         {
            if ($ip =~ /^172/)
            {
               $vm_ip = $ip;
               last;
            }
         }
         last if $vm_ip;
        }
        else
        {
           mylog::debug("$vm_name does not have a defined ipAddress object on " . $net->macAddress . " (" . $net->network . ")");
        }
      }
      # Skip if we couldn't find an IP - either the VM doesn't have one, it's not fully booted, VMware Tools not running, etc.
      if (!$vm_ip)
      {
         mylog::warn("$vm_name is powered on but has no 172 IP address");
         next;
      }
      $vms{$vm_name} = $vm_ip;
   }
   else
   {
      mylog::debug("$vm_name does not have a defined guest/net object");
   }
}

eval
{
   mylog::debug("Disconnecting from vSphere");
   Util::disconnect();
};

my $first = 1;
foreach my $vm_name (sort keys %vms)
{
   my $vm_ip = $vms{$vm_name};
   mylog::info("  VM,$vm_name,$vm_ip");
   if ($batch)
   {
      print "," if !$first;
      print $vm_ip;
   }
   $first = 0;
}
print "\n";
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

