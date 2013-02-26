#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use DBI;
use DBD::mysql;
use Net::SSH::Perl;
use libsf;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "eng\\script_user");
Opts::set_option("password", "password");

# SLIMS info
my $slims_ip = "192.168.144.4";
my $slims_user = "root";
my $slims_pass = "bluemoon";
my $slims_db = "lits";

# VMware networks
my $management_net = "192.168.128.0 VM Network";
my $storage_net = "10.10.0.0 VM Network";

my %opts = (
    vm_name => {
        type => "=s",
        help => "The name of the virtual machine to add to SLIMS",
        required => 1,
    },
    device_type => {
        type => "=s",
        help => "The device type to use for this VM (Vdbench Master, Virtual Client, etc)",
        required => 1,
    },
    owner => {
        type => "=s",
        help => "The owner to assign to this VM (QA, Development, Shared, etc)",
        required => 0,
        default => "Shared",
    },
    borrower => {
        type => "=s",
        help => "The borrower to assign to this VM (firstname.lastname)",
        required => 0,
        default => "Available",
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
   print "Add a virtual machine to SLIMS.";
   Opts::usage();
   exit 1;
}
Opts::parse();

Opts::validate();

my $vsphere_server = Opts::get_option("server");
my $vm_name = Opts::get_option('vm_name');
my $enable_debug = Opts::get_option('debug');
my $type = Opts::get_option('device_type');
my $owner = Opts::get_option('owner');
my $borrower = Opts::get_option('borrower');

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
    mylog::info("Searching for VM");
    my $vm = Vim::find_entity_view(view_type => 'VirtualMachine', filter => {'name' => qr/^$vm_name$/i});
    if (!$vm)
    {
        mylog::error("Could not find VM '$vm_name'");
        exit 1;
    }
    $vm_name = $vm->name;
    
    # Skip if it's not powered on
    if ($vm->runtime->powerState->val !~ /poweredOn/)
    {
        mylog::error("$vm_name is not powered on");
        exit 1;
    }
    
    # Quit if VMware tools are not installed and running
    if ($vm->guest->toolsStatus->val eq "toolsNotInstalled")
    {
        mylog::error("VMware Tools are not installed in this VM; cannot detect VM MAC address");
        exit 1;
    }
    if ($vm->guest->toolsStatus->val eq "toolsNotRunning")
    {
        mylog::error("VMware Tools are not running in this VM; cannot detect VM MAC address");
        exit 1;
    }

    # Try to find an IP address
    mylog::info("Getting network information");
    my %networks;
    if (defined $vm->guest && defined $vm->guest->net)
    {
        foreach my $net (@{$vm->guest->net})
        {
            my $net_name = $net->network;
            my $net_mac = $net->macAddress;
            my $net_ip;
            if (defined $net->ipAddress)
            {
                foreach my $ip (@{$net->ipAddress})
                {
                    if ($ip !~ /:/)
                    {
                        $net_ip = $ip;
                        last;
                    }
                }
            }
            else
            {
                mylog::warning("ipAddress is undefined for the interface on $net_name")
            }
            $networks{$net_name}{"name"} = $net_name;
            $networks{$net_name}{"mac"} = $net_mac;
            $networks{$net_name}{"ip"} = $net_ip;
            $networks{$net_name}{"dhcp"} = 1;
        }
    }
    else
    {
        mylog::error("$vm_name does not have a defined guest/net object.  VMware tools may be broken on this VM");
        exit 1;
    }
    mylog::info("Getting CPU/mem info");
    my $cpu_count = $vm->config->hardware->numCPU;
    my $mem_gb = $vm->config->hardware->memoryMB / 1024;
    $mem_gb .= "GB";
    
    mylog::info("Getting OS info");
    my $os;
    my $mip;
    if (exists $networks{$management_net})
    {
        $mip = $networks{$management_net}{"ip"};
        $os = `python ../get_client_os_version.py --client_ip=$mip 2>/dev/null`;
        $os = undef if $? != 0;
    }
    if (!$os)
    {
        $os = $vm->config->guestFullName;
    }
    chomp($os);
    
    mylog::info("Getting DHCP info");
    if ($mip)
    {
        # Getting DHCP info
        for my $network (keys %networks)
        {
            my $mac = $networks{$network}{"mac"};
            my $dhcp = `python ../get_client_dhcp_enabled.py --client_ip=$mip --interface_mac=$mac`;
            chomp($dhcp);
            if ($dhcp eq "true")
            {
                $networks{$network}{"dhcp"} = 1;
            }
            else
            {
                $networks{$network}{"dhcp"} = 0;
            }
        }
    }
    
    for my $net (keys %networks)
    {
        mylog::debug($net);
        for my $key (sort keys %{$networks{$net}})
        {
            mylog::debug("  $key => " . $networks{$net}{$key});
        }
    }
    
    mylog::info("The following info will be added to SLIMS:");
    mylog::info("  Name: " . $vm_name);
    mylog::info("  CPU: " . $cpu_count);
    mylog::info("  Memory: " . $mem_gb);
    if (!$networks{$management_net}{"dhcp"})
    {
        mylog::info("  Management IP: " . $networks{$management_net}{"ip"});
    }
    mylog::info("  Management MAC: " . $networks{$management_net}{"mac"});
    if (!$networks{$storage_net}{"dhcp"})
    {
        mylog::info("  Storage IP: " . $networks{$storage_net}{"ip"});
    }
    mylog::info("  Storage MAC: " . $networks{$storage_net}{"mac"});
    mylog::info("  OS: " . $os);
    mylog::info("  Owner: " . $owner);
    mylog::info("  Borrower: " . $borrower);
    mylog::info("  Type: " . $type);

    mylog::info("Connecting to SLIMS");
    my $dsn = "dbi:mysql:database=$slims_db;host=$slims_ip";
    my $dbh;
    eval
    {
        $dbh = DBI->connect($dsn, $slims_user, $slims_pass, {PrintError => 0, RaiseError => 1});
    };
    if ($@)
    {
        mylog::error("Could not connect to SLIMS: $@");
        exit 1;
    }

    # Create date/time stamps for DB
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
    $mon += 1;
    $year += 1900;
    my $datestamp = sprintf("%04d", $year) . "-" . sprintf("%02d", $mon) . "-" . sprintf("%02d", $mday);
    my $timestamp = sprintf("%02d", $hour) . ":" . sprintf("%02d", $min) . ":" . sprintf("%02d", $sec);

    # Prepare some SQL statements
    my $find_sth = $dbh->prepare("SELECT ID FROM computers WHERE name=?");
    my $server_update_sth = $dbh->prepare("UPDATE computers SET testring_id='0',name=?,type=?,location='Lab',cachecard='None',cachecardfirm='',drivetype='Other',drivefirm='',storage='None',storagefirm='',serial='N/A',console='',ip=?,mac=?,sip=?,smac=?,borrower=?,owner=?,comments=?,cpu=?,ram=?,duedate='$datestamp',date_mod='$datestamp $timestamp' WHERE ID=?");
    my $server_insert_sth = $dbh->prepare("INSERT INTO computers (ID, testring_id, name, type, location, cachecard, cachecardfirm, drivetype, drivefirm, storage, storagefirm, serial, console, ip, mac, sip, smac, borrower, owner, comments, cpu, ram, duedate, date_mod) VALUES (?,0,?,?,'Lab','None','','Other','','None','','N/A','',?,?,?,?,?,?,?,?,?,'$datestamp','$datestamp $timestamp')");
    my $selectid_sth = $dbh->prepare("SELECT sequence FROM computers__ID");
    my $updateid_sth = $dbh->prepare("UPDATE computers__ID SET sequence=sequence+1");

    my $device_id;
    $find_sth->execute($vm_name);
    my $result = $find_sth->fetchrow_hashref();
    if ($result)
    {
        # Update existing row
        $device_id = $result->{ID};
        mylog::info("Updating $vm_name");
        
        if ($networks{$storage_net}{"dhcp"} && $networks{$management_net}{"dhpc"})
        {
            $server_update_sth->execute($vm_name, $type, '', $networks{$management_net}{"mac"}, '', $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb, $device_id);
        }
        if ($networks{$storage_net}{"dhcp"} && !$networks{$management_net}{"dhpc"})
        {
            $server_update_sth->execute($vm_name, $type, $networks{$management_net}{"ip"}, $networks{$management_net}{"mac"}, '', $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb, $device_id);
        }
        if (!$networks{$storage_net}{"dhcp"} && $networks{$management_net}{"dhpc"})
        {
            $server_update_sth->execute($vm_name, $type, '', $networks{$management_net}{"mac"}, $networks{$storage_net}{"ip"}, $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb, $device_id);
        }
        if (!$networks{$storage_net}{"dhcp"} && !$networks{$management_net}{"dhpc"})
        {
            $server_update_sth->execute($vm_name, $type, $networks{$management_net}{"ip"}, $networks{$management_net}{"mac"}, $networks{$storage_net}{"ip"}, $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb, $device_id);
        }
    }
    else
    {
        # Insert a new row
        # Get the next ID
        $selectid_sth->execute();
        $result = $selectid_sth->fetchrow_hashref();
        $device_id = $result->{sequence};
        $updateid_sth->execute();
        
        mylog::info("Inserting $vm_name");
        if ($networks{$storage_net}{"dhcp"} && $networks{$management_net}{"dhpc"})
        {
            $server_insert_sth->execute($device_id, $vm_name, $type, '', $networks{$management_net}{"mac"}, '', $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb)
        }
        if ($networks{$storage_net}{"dhcp"} && !$networks{$management_net}{"dhpc"})
        {
            $server_insert_sth->execute($device_id, $vm_name, $type, $networks{$management_net}{"ip"}, $networks{$management_net}{"mac"}, '', $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb)
        }
        if (!$networks{$storage_net}{"dhcp"} && $networks{$management_net}{"dhpc"})
        {
            $server_insert_sth->execute($device_id, $vm_name, $type, '', $networks{$management_net}{"mac"}, $networks{$storage_net}{"ip"}, $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb)
        }
        if (!$networks{$storage_net}{"dhcp"} && !$networks{$management_net}{"dhpc"})
        {
            $server_insert_sth->execute($device_id, $vm_name, $type, $networks{$management_net}{"ip"}, $networks{$management_net}{"mac"}, $networks{$storage_net}{"ip"}, $networks{$storage_net}{"mac"}, $borrower, $owner, $os, $cpu_count, $mem_gb)
        }
    }


    #$dbh->disconnect();
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
