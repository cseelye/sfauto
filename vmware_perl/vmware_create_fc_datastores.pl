#!/usr/bin/perl
use strict;
use VMware::VIRuntime;
use libsf;
use libvmware;
use JSON::XS;

use Data::Dumper;

# Set default username/password to use
# These can be overridden via --username and --password command line options
Opts::set_option("username", "administrator");
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
    vmhost => {
        type => "=s",
        help => "The hostname/IP of the host to create datastores on",
        required => 1,
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


# my $client = new JSON::RPC::Client;
# my $url = 'https://172.26.64.93/json-rpc/7.0?';

# my $callobj = {
#         method => 'GetApi',
#         params => ["admin","admin"] };

# print Dumper(ref($callobj));

# my $sessionid = $client->call($url, $callobj);

# die;

Opts::add_options(%opts);
if (scalar(@ARGV) < 1)
{
   print "Find new iSCSI volumes and create datastores on them";
   Opts::usage();
   exit 1;
}
Opts::parse();
my $vsphere_server = Opts::get_option("mgmt_server");
Opts::set_option("server", $vsphere_server);
my $host_name = Opts::get_option('vmhost');
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

# Find the host
mylog::info("Searching for host $host_name");
my $vmhost = Vim::find_entity_view(view_type => 'HostSystem', filter => {'name' => qr/^$host_name$/i});
if (!$vmhost)
{
    mylog::error("Could not find host '$host_name'");
    exit 1;
}

# Rescan the host
eval
{
    libvmware::VMwareRescanFC($vmhost);
};
if ($@)
{
    my $fault = $@;
    libvmware::DisplayFault("Rescan failed", $fault);
    exit 1;
}

#trying to get the fc volumes on the cluster but have to explore the esx datatype more
#testing
# my $cluster_user = "admin";
# my $cluster_pass = "admin";
# my $cluster_account = "esx";
# my $mvip = "172.26.64.93";

# #hack
# my $account_res = `curl -s --insecure --user $cluster_user:$cluster_pass \'https://$mvip/json-rpc/7.0\' --data \'{"method":"ListAccounts"}\' 2>/dev/null`;
# my $test = `curl -s --insecure --user $cluster_user:$cluster_pass \'https://$mvip/json-rpc/7.0\' --data \'{"method":"ListActiveVolumes"}\' 2>/dev/null`;

# my $account_decoded = JSON::XS::decode_json($account_res);
# my $found_accounts = $account_decoded->{result}->{accounts};
# my $account_id;

# foreach my $found_account (@{$found_accounts}){
#     if($found_account->{username} eq $cluster_account){
#         $account_id = $found_account->{accountID};
#     }
# }
# my $decoded = JSON::XS::decode_json($test);
# my $volumes = $decoded->{result}->{volumes};

# Find the iSCSI adapter in this host
#mylog::info("Searching for FC adapters on $host_name");
#my @fc_hbas = libvmware::VMwareFindFCHbas($vmhost);
# foreach my $adapter (@fc_hbas){
#     my $wwn = libvmware::TurnDecWWNToHexWWN($adapter->portWorldWideName);
#     print "WWN: $wwn";
# }

# Find the SolidFire disks without datastores and create them
my @return_disk_list;
eval
{
    mylog::debug("Getting a list of available disks");
    my $datastore_manager = Vim::get_view(mo_ref => $vmhost->configManager->datastoreSystem);
    my $disk_list = $datastore_manager->QueryAvailableDisksForVmfs();
    foreach my $disk (@{$disk_list})
    {
        # Skip non-SF devices
        if ($disk->canonicalName !~ /f47acc/)
        {
            mylog::debug("Skipping " . $disk->devicePath);
            next;
        }
        mylog::debug("querying " . $disk->devicePath);
        #foreach my $vol (@{$volumes}){

            my $options_list = $datastore_manager->QueryVmfsDatastoreCreateOptions(devicePath => $disk->devicePath);
            my $create_option = $options_list->[0];

            my @pieces = split(/\./, $disk->canonicalName);
            my $disk_serial = pop @pieces;
            my $cluster_id = substr($disk_serial, 16, 8);
            $cluster_id =~ s/([a-fA-F0-9][a-fA-F0-9])/chr(hex($1))/eg;
            my $volume_id = substr($disk_serial, 24, 8);
            $volume_id =~ s/^0+//;
            $volume_id = hex($volume_id);

            my $datastore_name = "volume-$volume_id";

            mylog::info("Creating datastore $datastore_name on disk $disk->{canonicalName}...");
            push (@return_disk_list, $datastore_name);
            $create_option->spec->vmfs->volumeName($datastore_name);
            my $newDatastore;
            eval
            {
                $newDatastore = $datastore_manager->CreateVmfsDatastore(spec => $create_option->spec);
            };
            if ($@)
            {
                my $fault = $@;
                if ($fault->detail && $fault->detail->faultMessage->[0]->message =~ /active VMKernel file system detected/)
                {
                    mylog::error($disk->{canonicalName} . " has an existing datastore");
                }
                else
                {
                    libvmware::DisplayFault("Creating datastore failed", $fault);
                    exit 1;
                }
            }
            # if($account_id eq $vol->{accountID}){
            #     print $vol->{scsiEUIDeviceID}; # you actually want scsiNAADeviceID here to compare the to the VMware "cannonical name"
            #     print "\n";
            #     print $canonical_name;
            #     print "\n";
            #     print Data::Dumper->Dump([$disk], ['adapter']);
            #     if($vol->{scsiEUIDeviceID} eq $canonical_name){
            #         my $datastore_name = $vol->{name};
            #         mylog::info("Creating datastore $datastore_name on disk $canonical_name...");
            #         push (@return_disk_list, $datastore_name);
            #         $create_option->spec->vmfs->volumeName($datastore_name);
            #         my $newDatastore = $datastore_manager->CreateVmfsDatastore(spec => $create_option->spec);
            #     }
            # }
        #}
    }
};
if ($@)
{
    my $fault = $@;
    #print Dumper($fault);
    libvmware::DisplayFault("Creating datastore failed", $fault);
    exit 1;
}



mylog::pass("Sucessfully created datastores on $host_name");
# Send the info back to parent script if requested
if (defined $result_address)
{
    libsf::SendResultToParent(result_address => $result_address, result => \@return_disk_list);
}
exit 0;
