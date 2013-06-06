#!/usr/bin/perl -w
use strict;
use VMware::VIRuntime;

package VMwareError;
{
    use overload ('""' => 'stringify');

    sub new
    {
        my $class = shift;
        my ( $message ) = @_;

        my $self = {};
        $self->{message} = $message;

        bless $self, $class;
        return $self;
    }

    sub stringify
    {
        my ($self) = @_;
        my $class = ref($self) || $self;
        return "$class - $self->{message}";
    }
    1;
}

package libvmware;

sub DisplayFault
{
    my ($message, $fault) = @_;
    if (ref($fault) ne 'SoapFault')
    {
        mylog::error("$message - " . $fault);
    }
    if (ref($fault->name))
    {
        mylog::error("$message - " . ref($fault->name) . ": " . $fault->fault_string);
    }
    else
    {
        mylog::error("$message - " . $fault->name . ": " . $fault->fault_string);
    }
}

#
# Return a list of VM mo_ref that match the specified inputs
#
sub SearchForVms
{
    my %params = (
        datacenter_name  => undef,
        cluster_name     => undef,
        pool_name        => 'Resources',
        folder_name      => undef,
        recurse          => undef,
        vm_name          => undef,
        vm_regex         => undef,
        vm_count         => 0,
        vm_powerstate    => undef,
        @_);

    my $dc;
    if ($params{datacenter_name})
    {
        mylog::info("Searching for datacenter $params{datacenter_name}");
        $dc = Vim::find_entity_view(view_type => 'Datacenter', filter => { 'name' => qr/^$params{datacenter_name}$/i }, properties => []);
        if (!$dc)
        {
            die VMwareError->new("Cannot find datacenter $params{datacenter_name}");
        }
    }

    my @vm_list;

    # Find VMs in a folder
    if ($params{folder_name})
    {
        mylog::info("Searching for folder $params{folder_name}...");
        my $folder;
        if ($dc)
        {
            $folder = Vim::find_entity_view(view_type => 'Folder', begin_entity => $dc, filter => { 'name' => qr/^$params{folder_name}/i }, properties => []);
        }
        else
        {
            $folder = Vim::find_entity_view(view_type => 'Folder', filter => { 'name' => qr/^$params{folder_name}/i }, properties => []);
        }
        if (!$folder)
        {
            die VMwareError->new("Could not find folder $params{folder_name}");
        }

        # Find a specifically named VM
        if ($params{vm_name})
        {
            $params{vm_regex} = '^' . $params{vm_name}. '$';
        }

        # Find matching VMs
        mylog::info("Searching for matching VMs in folder");
        my @temp_vm_list = GetVmsInFolder($folder->{mo_ref}, $params{recurse}, $params{vm_regex}, $params{vm_powerstate});
        my $count = 0;
        foreach my $vm (@temp_vm_list)
        {
            push(@vm_list, $vm);
            $count++;
            last if ($params{vm_count} > 0 && $count >= $params{vm_count});
        }
    }
    elsif ($params{cluster_name})
    {
        mylog::info("Searching for cluster $params{cluster_name}...");
        my $cluster;
        if ($dc)
        {
            $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', begin_entity => $dc, filter => { 'name' => qr/^$params{cluster_name}/i }, properties => []);
        }
        else
        {
            $cluster = Vim::find_entity_view(view_type => 'ClusterComputeResource', filter => { 'name' => qr/^$params{cluster_name}/i }, properties => []);
        }
        if (!$cluster)
        {
            die VMwareError->new("Could not find cluster '$params{cluster_name}'");
        }
        if (!$params{pool_name})
        {
            $params{pool_name} = "Resources";
            $params{recurse} = 1;
        }
        mylog::info("Searching for pool $params{pool_name}");
        my $pool = Vim::find_entity_view(view_type => 'ResourcePool', begin_entity => $cluster, filter => { 'name' => qr/^$params{pool_name}$/i }, properties => []);
        if (!$pool)
        {
            die VMwareError->new("Could not find pool '$params{pool_name}' on cluster '$params{cluster_name}'");
        }

        # Find a specifically named VM
        if ($params{vm_name})
        {
            $params{vm_regex} = '^' . $params{vm_name}. '$';
        }

        mylog::info("Searching for matching VMs in pool");
        my @temp_vm_list = GetVmsInPool($pool->{mo_ref}, $params{recurse}, $params{vm_regex}, $params{vm_powerstate});
        my $count = 0;
        foreach my $vm (@temp_vm_list)
        {
            push(@vm_list, $vm);
            $count++;
            last if ($params{vm_count} > 0 && $count >= $params{vm_count});
        }
    }
    else
    {
        mylog::info("Searching for matching VMs");
        # Find a specifically named VM
        if ($params{vm_name})
        {
            $params{vm_regex} = '^' . $params{vm_name}. '$';
        }
        my %filter;
        if ($params{vm_regex})
        {
            $filter{name} = qr/$params{vm_regex}/i;
        }
        if ($params{vm_powerstate})
        {
            $filter{'runtime.powerState'} = $params{vm_powerstate};
        }
        my $vm_matches;
        if ($dc)
        {
            $vm_matches = Vim::find_entity_views(view_type => 'VirtualMachine', begin_entity => $dc, filter => \%filter, properties => ['name']);
        }
        else
        {
            $vm_matches = Vim::find_entity_views(view_type => 'VirtualMachine', filter => \%filter, properties => ['name']);
        }
        my $count = 0;
        foreach my $vm (sort {$a->name cmp $b->name} @{$vm_matches})
        {
            push(@vm_list, $vm->{mo_ref});
            $count++;
            last if ($params{vm_count} > 0 && $count >= $params{vm_count});
        }
    }

    return @vm_list;
}

sub GetVmsInFolder
{
    my ($folder_mor, $search_recursive, $vm_regex, $vm_power) = @_;
    my $folder = Vim::get_view(mo_ref => $folder_mor, properties => ['childEntity']);

    my %filter;
    if ($vm_regex)
    {
        $filter{name} = qr/$vm_regex/i;
    }
    if ($vm_power)
    {
        $filter{'runtime.powerState'} = $vm_power;
    }
    my $vm_matches = Vim::find_entity_views(view_type => 'VirtualMachine', begin_entity => $folder_mor, filter => \%filter, properties => ['parent', 'name']);
    my @vm_list;
    foreach my $vm (sort {$a->name cmp $b->name} @{$vm_matches})
    {
        next if (!$search_recursive && $vm->parent->value ne $folder_mor->value);
        push(@vm_list, $vm->{mo_ref})
    }

    return @vm_list;
}

sub GetVmsInPool
{
    my ($pool_mor, $search_recursive, $vm_regex, $vm_power) = @_;
    my $pool = Vim::get_view(mo_ref => $pool_mor, properties => ['resourcePool', 'vm']);

    my %filter;
    if ($vm_regex)
    {
        $filter{name} = qr/$vm_regex/i;
    }
    if ($vm_power)
    {
        $filter{'runtime.powerState'} = $vm_power;
    }
    my $vm_matches = Vim::find_entity_views(view_type => 'VirtualMachine', begin_entity => $pool_mor, filter => \%filter, properties => ['resourcePool', 'name']);
    my @vm_list;
    foreach my $vm (sort {$a->name cmp $b->name} @{$vm_matches})
    {
        next if (!$search_recursive && $vm->resourcePool->value ne $pool_mor->value);
        push(@vm_list, $vm->{mo_ref});
    }

    return @vm_list;
}


1;
