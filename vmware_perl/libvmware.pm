#!/usr/bin/perl -w
use strict;
use constant { TRUE => 1, FALSE => 0 };
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

sub FaultToString
{
    my $fault = shift;
    my $fault_str = "";
    if (ref($fault) ne 'SoapFault')
    {
        $fault_str .= $fault;
    }
    elsif (ref($fault->name))
    {
        $fault_str .= ref($fault->name) . ": " . $fault->fault_string
    }
    else
    {
        $fault_str .= $fault->name . ": " . $fault->fault_string
    }
    $fault_str =~ s/(\s+$)//g;
    if ($fault->detail && $fault->detail->faultMessage)
    {
        if (ref($fault->detail->faultMessage) eq 'ARRAY')
        {
            $fault_str .= ". " . $fault->detail->faultMessage->[0]->message;
        }
    }
    return $fault_str;
}

sub DisplayFault
{
    my ($message, $fault) = @_;
    mylog::error("$message - " . FaultToString($fault));
}

sub WaitForTask
{
    my %params = (
        vim             => undef,
        task_ref        => undef,
    @_);

    if (!$params{vim})
    {
        $params{vim} = $Vim::vim_global;
    }

    while (1)
    {
        my $task = Vim::get_view($params{vim}, mo_ref => $params{task_ref});
        my $state = $task->info->state->val;
        if ($state eq 'success')
        {
            return;
        }
        elsif ($state eq 'error')
        {
            my $soap_fault = SoapFault->new;
            $soap_fault->name($task->info->error->fault);
            $soap_fault->detail($task->info->error->fault);
            $soap_fault->fault_string($task->info->error->localizedMessage);
            die FaultToString($soap_fault);
        }
        sleep 1;
    }
}

sub WaitForVmBooted
{
    my %params = (
        vim             => undef,
        vm_ref          => undef,
        timeout         => 180,
    @_);

    if (!$params{vim})
    {
        $params{vim} = $Vim::vim_global;
    }

    my $vm;
    eval
    {
        $vm = Vim::get_view($params{vim}, mo_ref => $params{vm_ref}, properties => ['name', 'runtime', 'guest', 'guestHeartbeatStatus']);
    };
    if ($@)
    {
        die "Could not look up VM ref " . $params{vm_ref} . " - " . FaultToString($@) . "\n";
    }
    eval
    {
        my $start_time = time();
        my $previous_status = "";
        my $status = "";

        # Wait for the VM to be powered on
        $status = $vm->runtime->powerState->val;
        $previous_status = "";
        while ($status ne "poweredOn")
        {
            if (time() - $start_time > $params{timeout})
            {
                die "Timeout waiting for VM to power on";
            }
            if ($status ne $previous_status)
            {
                mylog::info("  " . $vm->name . ": VM is " . $status);
                $previous_status = $status;
            }
            $vm->update_view_data();
            $status = $vm->runtime->powerState->val;
            sleep 5 if ($status ne "poweredOn");
        }
        mylog::info("  " . $vm->name . ": VM is poweredOn");

        # See if VMware tools are installed
        if ($vm->guest->toolsStatus->val eq "toolsNotInstalled")
        {
            die "VMware Tools are not installed in this VM; cannot detect VM boot/health";
        }

        $previous_status = "";
        $status = $vm->guestHeartbeatStatus->val;
        while ($status ne "green")
        {
            if (time() - $start_time > $params{timeout})
            {
                die "Timeout waiting for VM heartbeat";
            }
            if ($status ne $previous_status)
            {
                mylog::info("  " . $vm->name . ": VM heartbeat is " . $status);
                $previous_status = $status;
            }
            $vm->update_view_data();
            $status = $vm->guestHeartbeatStatus->val;
            sleep 5 if ($status ne "green");
        }
        mylog::info("  " . $vm->name . ": VM heartbeat is green");
    };
    if ($@)
    {
        die FaultToString($@) . "\n";
    }
}

sub WaitForVmDown
{
    my %params = (
        vim             => undef,
        vm_ref          => undef,
    @_);

    if (!$params{vim})
    {
        $params{vim} = $Vim::vim_global;
    }

    my $vm;
    eval
    {
        $vm = Vim::get_view($params{vim}, mo_ref => $params{vm_ref}, properties => ['name', 'runtime']);
    };
    if ($@)
    {
        die "Could not look up VM ref " . $params{vm_ref} . " - " . FaultToString($@) . "\n";
    }
    eval
    {
        my $previous_status = "";
        my $status = "";

        # Wait for the VM to be powered off
        $status = $vm->runtime->powerState->val;
        $previous_status = "";
        while ($status ne "poweredOff")
        {
            if ($status ne $previous_status)
            {
                mylog::info("  " . $vm->name . ": VM is " . $status);
                $previous_status = $status;
            }
            $vm->update_view_data();
            $status = $vm->runtime->powerState->val;
            sleep 5 if ($status ne "poweredOff");
        }
        mylog::info("  " . $vm->name . ": VM is poweredOff");
    };
    if ($@)
    {
        die FaultToString($@) . "\n";
    }
}

#
# Return a list of VM mo_ref that match the specified inputs
#
sub SearchForVms
{
    my %params = (
        vim              => undef,
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

    if (!$params{vim})
    {
        $params{vim} = $Vim::vim_global;
    }
    my $dc;
    if ($params{datacenter_name})
    {
        mylog::info("Searching for datacenter $params{datacenter_name}");
        $dc = Vim::find_entity_view($params{vim}, view_type => 'Datacenter', filter => { 'name' => qr/^$params{datacenter_name}$/i }, properties => []);
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
            $folder = Vim::find_entity_view($params{vim}, view_type => 'Folder', begin_entity => $dc, filter => { 'name' => qr/^$params{folder_name}/i }, properties => []);
        }
        else
        {
            $folder = Vim::find_entity_view($params{vim}, view_type => 'Folder', filter => { 'name' => qr/^$params{folder_name}/i }, properties => []);
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
        my @temp_vm_list = GetVmsInFolder($params{vim}, $folder->{mo_ref}, $params{recurse}, $params{vm_regex}, $params{vm_powerstate});
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
            $cluster = Vim::find_entity_view($params{vim}, view_type => 'ClusterComputeResource', begin_entity => $dc, filter => { 'name' => qr/^$params{cluster_name}/i }, properties => []);
        }
        else
        {
            $cluster = Vim::find_entity_view($params{vim}, view_type => 'ClusterComputeResource', filter => { 'name' => qr/^$params{cluster_name}/i }, properties => []);
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
        my $pool = Vim::find_entity_view($params{vim}, view_type => 'ResourcePool', begin_entity => $cluster, filter => { 'name' => qr/^$params{pool_name}$/i }, properties => []);
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
        my @temp_vm_list = GetVmsInPool($params{vim}, $pool->{mo_ref}, $params{recurse}, $params{vm_regex}, $params{vm_powerstate});
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
            $vm_matches = Vim::find_entity_views($params{vim}, view_type => 'VirtualMachine', begin_entity => $dc, filter => \%filter, properties => ['name']);
        }
        else
        {
            $vm_matches = Vim::find_entity_views($params{vim}, view_type => 'VirtualMachine', filter => \%filter, properties => ['name']);
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
    my ($vim, $folder_mor, $search_recursive, $vm_regex, $vm_power) = @_;
    my $folder = Vim::get_view($vim, mo_ref => $folder_mor, properties => ['childEntity']);

    my %filter;
    if ($vm_regex)
    {
        $filter{name} = qr/$vm_regex/i;
    }
    if ($vm_power)
    {
        $filter{'runtime.powerState'} = $vm_power;
    }
    my $vm_matches = Vim::find_entity_views($vim, view_type => 'VirtualMachine', begin_entity => $folder_mor, filter => \%filter, properties => ['parent', 'name']);
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
    my ($vim, $pool_mor, $search_recursive, $vm_regex, $vm_power) = @_;
    my $pool = Vim::get_view($vim, mo_ref => $pool_mor, properties => ['resourcePool', 'vm']);

    my %filter;
    if ($vm_regex)
    {
        $filter{name} = qr/$vm_regex/i;
    }
    if ($vm_power)
    {
        $filter{'runtime.powerState'} = $vm_power;
    }
    my $vm_matches = Vim::find_entity_views($vim, view_type => 'VirtualMachine', begin_entity => $pool_mor, filter => \%filter, properties => ['resourcePool', 'name']);
    my @vm_list;
    foreach my $vm (sort {$a->name cmp $b->name} @{$vm_matches})
    {
        next if (!$search_recursive && $vm->resourcePool->value ne $pool_mor->value);
        push(@vm_list, $vm->{mo_ref});
    }

    return @vm_list;
}

sub VMwareFindFCHbas
{
    my $vmhost = shift;
    my $host_name = $vmhost->name;
    my @fc_hbas = ();

    mylog::debug("Searching for FC adapter on $host_name");
    my $adapter_list = $vmhost->config->storageDevice->hostBusAdapter;
    foreach my $adapter (@{$adapter_list})
    {
        # if ($adapter =~ /HostInternetScsiHba/){
        #     print "Adapter $adapter\n";
        #     print Data::Dumper->Dump([$adapter], ['adapter']);
        # }
        if ($adapter =~ /FibreChannel/){
            mylog::debug("Found FC HBA " . $adapter->device);
            push(@fc_hbas, $adapter);
        }
    }
    if (@fc_hbas.length == 0){
        die "Could not find any FC HBAs on $host_name";
    }
    return @fc_hbas;
}

sub VMwareRescanFC
{
    my $vmhost = shift;
    my $host_name = $vmhost->name;

    my @fc_hbas = VMwareFindFCHbas($vmhost);
    # my $iscsi_hbas = VMwareFindIscsiHba($vmhost);
    mylog::info("Starting rescan for " . @fc_hbas.length . " HBAs");
    mylog::debug("Getting a reference to the storage manager");
    my $storage_manager = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);
    #mylog::info("  Rescan FC HBAs...");
    foreach my $fc_hba (@fc_hbas){
        mylog::info("  " . $fc_hba->device);
        $storage_manager->RescanHba(hbaDevice => $fc_hba->device);
    }
    mylog::info("  Rescan VMFS...");
    $storage_manager->RescanVmfs();
    mylog::info("  Refresh storage system...");
    $storage_manager->RefreshStorageSystem();
}


sub VMwareFindIscsiHba
{
    my $vmhost = shift;
    my $host_name = $vmhost->name;

    mylog::debug("Searching for iSCSI adapter on $host_name");
    my $adapter_list = $vmhost->config->storageDevice->hostBusAdapter;
    foreach my $adapter (@{$adapter_list})
    {
        if ($adapter->driver =~ /iscsi_vmk/)
        {
            mylog::debug("iSCSI HBA is " . $adapter->device);
            return $adapter;
        }
    }

    die "Could not find an iSCSI HBA on $host_name";
}

sub VMwareRescanIscsi
{
    my $vmhost = shift;
    my $host_name = $vmhost->name;

    my $iscsi_hba = VMwareFindIscsiHba($vmhost);

    mylog::info("Starting rescan");
    mylog::debug("Getting a reference to the storage manager");
    my $storage_manager = Vim::get_view(mo_ref => $vmhost->configManager->storageSystem);
    mylog::info("  Rescan HBA...");
    $storage_manager->RescanHba(hbaDevice => $iscsi_hba->device);
    mylog::info("  Rescan VMFS...");
    $storage_manager->RescanVmfs();
    mylog::info("  Refresh storage system...");
    $storage_manager->RefreshStorageSystem();
}

sub VMwareGetParentDatacenterName
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

1;
