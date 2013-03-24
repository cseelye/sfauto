use strict;

package libsf;
{
    use VMware::VIRuntime;
    use DateTime;
    use Time::HiRes qw/ gettimeofday /;
    use IPC::Open3;

    sub SshCommand
    {
        # There is no good perl module for SSH that isn't hideously difficult to install, so we'll use python instead
        my %params = (
                client_ip   => undef,
                client_user => "root",
                client_pass => "solidfire",
                command     => undef,
                @_);
        mylog::debug("Executing " . $params{command} . " on " . $params{client_ip});
        my $command = "python execute_client_command.py --client_ip=$params{client_ip} --client_user=$params{client_user} --client_pass=$params{client_pass} --command=\"$params{command}\" --bash";
        my $pid = open3(\*CHLD_IN, \*CHLD_OUT, \*CHLD_ERR, $command);
        my @outlines = <CHLD_OUT>;
        my @errlines = <CHLD_ERR>;
        close CHLD_OUT;
        close CHLD_ERR;
        close CHLD_IN;
        waitpid ($pid, 0);
        my $return_code = $? >> 8;

        return ($return_code, join("\n", @outlines));

        #if (lc($^O) =~ /win/)
        #{
        #    $command .= " & echo %ERRORLEVEL%";
        #}
        #else
        #{
        #    $command .= "; echo $?"
        #}
        #my $result = `$command`;
        #mylog::debug($result);
        #my @lines = split (/\n/, $result);
        #my $return_code = pop (@lines);
        #return ($return_code, join("\n", @lines));
    }

    sub GetCurrentDateString
    {
        my ( $s, $us ) = gettimeofday;
        my $dt = DateTime->from_epoch( epoch => $s + 1.0e-6 * $us, time_zone => 'local' );
        return $dt->ymd . " " . $dt->hms . "," . sprintf("%03d", $dt->microsecond/1000);
    }

    sub SecondsToElapsed
    {
        # From perlmonks
        # http://www.perlmonks.org/?node_id=110550

        my( $weeks, $days, $hours, $minutes, $seconds, $sign, $res ) = qw/0 0 0 0 0/;

        $seconds = shift;
        $sign    = $seconds == abs $seconds ? '' : '-';
        $seconds = abs $seconds;

        ($seconds, $minutes) = ($seconds % 60, int($seconds / 60)) if $seconds;
        ($minutes, $hours  ) = ($minutes % 60, int($minutes / 60)) if $minutes;
        ($hours,   $days   ) = ($hours   % 24, int($hours   / 24)) if $hours;
        ($days,    $weeks  ) = ($days    %  7, int($days    /  7)) if $days;

        $res = sprintf '%ds',     $seconds;
        $res = sprintf "%dm$res", $minutes if $minutes or $hours or $days or $weeks;
        $res = sprintf "%dh$res", $hours   if             $hours or $days or $weeks;
        $res = sprintf "%dd$res", $days    if                       $days or $weeks;
        $res = sprintf "%dw$res", $weeks   if                                $weeks;

        return "$sign$res";
    }
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
}


package mylog;
{
    use Sys::Syslog qw/:standard :macros/;
    use Term::ANSIScreen qw/:constants/;
    $Term::ANSIScreen::AUTORESET = 1;

    # Load ANSI emulation if we are running on Windows
    BEGIN
    {
        if ($^O =~ 'MSWin')
        {
            require Win32::Console::ANSI;
            Win32::Console::ANSI->import;
        }
    }

    our $Silent = 0;        # Enable to silence logging to the screen
    our $DisplayDebug = 0;  # Enable to display debug messages to the screen

    # Determine if we have a real STDOUT or are being redirected
    my $redirected = 0;
    if ( ! -t STDOUT )
    {
    	$redirected = 1;
    }

    openlog("sftest", "ndelay", LOG_LOCAL0);

    END
    {
        closelog();
    }

    sub error
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_ERR, $message);

        return if $Silent;
        if ($redirected)
        {
	        print "$timestamp: ERROR  $message\n";
        }
        else
        {
        	print BOLD RED ON BLACK "$timestamp: ERROR  $message\n";
        }
    }

    sub warn
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_WARNING, $message);

        return if $Silent;

        if ($redirected)
        {
        	print "$timestamp: WARN   $message\n";
        }
        else
        {
        	print BOLD YELLOW ON BLACK "$timestamp: WARN   $message\n";
        }
    }

    sub info
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_INFO, $message);

        return if $Silent;
        if ($redirected)
        {
	        print "$timestamp: INFO   $message\n";
        }
        else
        {
        	print BOLD WHITE ON BLACK "$timestamp: INFO   $message\n";
        }
    }

    sub debug
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_DEBUG, $message);

        return if $Silent;
        return if !$DisplayDebug;
        if ($redirected)
        {
        	print "$timestamp: DEBUG  $message\n";
        }
        else
        {
	        print WHITE ON BLACK "$timestamp: DEBUG  $message\n";
        }
    }

    sub pass
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_INFO, $message);

        return if $Silent;
        if ($redirected)
        {
        	print "$timestamp: PASS   $message\n";
        }
        else
        {
	        print BOLD GREEN ON BLACK "$timestamp: PASS   $message\n";
        }
    }
}







1;
