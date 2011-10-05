#!/usr/bin/perl -W

use strict;

use FindBin;
use POSIX qw(setsid);
use lib "$FindBin::Bin";
use LockNLog::Mutex;

if(scalar(@ARGV)>1) {
	my $concurr = shift(@ARGV);
	$concurr = ($concurr =~ /^[1-9][0-9]*$/)?$concurr:5;
	# We have to ignore pleas from the children
	$SIG{CHLD}='IGNORE';

	my($pid)=fork();

	if(defined($pid)) {
		if($pid==0) {
			# As we are using dumb forks, we must close connections to the parent process
			close(STDIN);
			open(STDIN,'<','/dev/null');
			close(STDOUT);
			open(STDOUT,'>','/dev/null');
			close(STDERR);
			open(STDERR,'>','/dev/null');
			setsid();
			
			my($mutex)=LockNLog::Mutex->new($concurr);

			$mutex->mutex(
			sub {
				system(@ARGV);
			});
		} else {
			# Nothing
			exit 0;
		}
	} else {
		die "ERROR: Unable to fork process!!!!";
	}
}
