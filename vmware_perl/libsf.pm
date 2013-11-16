use strict;
use constant { TRUE => 1, FALSE => 0 };

package libsf;
{
    use VMware::VIRuntime;
    use DateTime;
    use DateTime::Format::Strptime;
    use Time::HiRes qw/ gettimeofday /;
    use IPC::Open3;

    sub SendResultToParent
    {
	my %params = (
	    result_address => undef,
	    result => undef,
	    @_
	);

	require JSON;
	require ZMQ;
	require ZMQ::Constants;

	my %result;
	$result{result} = $params{result};
        my $result_json = JSON::encode_json(\%result);
        mylog::debug("Sending result " . $result_json . " to " . $params{result_address});

	my $ctx = ZMQ::Context->new();
	my $zmq_sock = $ctx->socket($ZMQ::Constants::ZMQ_PAIR);
	$zmq_sock->connect($params{result_address});
	$zmq_sock->send(ZMQ::Message->new($result_json));
    }

    sub SshCommand
    {
        # There is no good perl module for SSH that isn't hideously difficult to install, so we'll use python instead
        my %params = (
                client_ip   => undef,
                client_user => "root",
                client_pass => "password",
                command     => undef,
                @_);
        mylog::debug("Executing " . $params{command} . " on " . $params{client_ip});
        my $command = "python ../execute_client_command.py --client_ip=$params{client_ip} --client_user=$params{client_user} --client_pass=$params{client_pass} --command=\"$params{command}\" --bash";
        if (lc($^O) !~ /win/)
        {
            $command =~ s/\$/\\\$/g;
        }
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

    sub DateStringToEpoch
    {
        # Currently this is constrained to the format used by VMware's API
        # 2013-11-14T16:12:11.081321Z

        my $time_str = shift;

        my $parser = DateTime::Format::Strptime->new(pattern => "%Y-%m-%dT%H:%M:%S.%6N", locale => "en_US", time_zone => "UTC");
        my $dt = $parser->parse_datetime($time_str);
        if (!$dt)
        {
            mylog::debug("Failed to parse '$time_str' to a DateTime");
            return 0;
        }
        return $dt->epoch;
    }
}


package mylog;
{
    use threads;
    use threads::shared;
    use IO::Handle;
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
    
    my $stdout_lock : shared;   # Only allow one thread to write to the screen at a time
                                # Mostly useful for Windows

    our $Silent = 0;        # Enable to silence logging to the screen
    our $DisplayDebug = 0;  # Enable to display debug messages to the screen

    # Determine if we have a real STDOUT or are being redirected
    my $redirected = 0;
    if ( ! -t STDOUT )
    {
    	$redirected = 1;
    }

    openlog("sftest-pl", "ndelay", LOG_LOCAL0);

    END
    {
        closelog();
    }

    sub error
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_ERR, "ERROR $message");

        return if $Silent;
        {
            lock $stdout_lock;
            if ($redirected)
            {
                    STDOUT->printflush("$timestamp: ERROR   $message\n");
            }
            else
            {
                    print BOLD RED ON BLACK "$timestamp: ERROR   $message\n";
            }
        }
    }

    sub warn
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_WARNING, " WARN  $message");

        return if $Silent;
        {
            lock $stdout_lock;
            if ($redirected)
            {
                    STDOUT->printflush("$timestamp: WARN    $message\n");
            }
            else
            {
                    print BOLD YELLOW ON BLACK "$timestamp: WARN    $message\n";
            }
        }
    }

    sub info
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_INFO, "INFO  $message");

        return if $Silent;
        {
            lock $stdout_lock;
            if ($redirected)
            {
                    STDOUT->printflush("$timestamp: INFO    $message\n");
            }
            else
            {
                    print BOLD WHITE ON BLACK "$timestamp: INFO    $message\n";
            }
        }
    }

    sub debug
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_DEBUG, "DEBUG $message");

        return if $Silent;
        return if !$DisplayDebug;
        {
            lock $stdout_lock;
            if ($redirected)
            {
                    STDOUT->printflush("$timestamp: DEBUG   $message\n");
            }
            else
            {
                    STDOUT->printflush(WHITE ON BLACK "$timestamp: DEBUG   $message\n");
            }
        }
    }

    sub pass
    {
        my $message = shift;
        my $timestamp = libsf::GetCurrentDateString();

        syslog(LOG_INFO, "PASS  $message");

        return if $Silent;
        {
            lock $stdout_lock;
            if ($redirected)
            {
                    STDOUT->printflush("$timestamp: PASS    $message\n");
            }
            else
            {
                    print BOLD GREEN ON BLACK "$timestamp: PASS    $message\n";
            }
        }
    }
}







1;
