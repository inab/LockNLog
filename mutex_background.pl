#!/usr/bin/perl -W

use strict;

use LockNLog::Mutex;

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
		
		my($mutex)=LockNLog::Mutex->new(5);

		$mutex->mutex(
		sub {
			system(@ARGV);
		});
	} else {
		# Nothing
		exit 0;
	}
} else {
	die "ERROR: Unable to fork process!!!!"
}