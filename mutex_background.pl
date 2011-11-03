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

	# As we are using dumb forks, we must close connections to the parent process
	# close $_ for map { /^(?:ARGV|std(?:err|out|in)|STD(?:ERR|OUT|IN))$/ ? () : *{$::{$_}}{IO} || () } keys %::;
	foreach my $FH ( map { /^(?:ARGV|STDERR|stderr)$/ ? () : *{$::{$_}}{IO} || () } keys %::) {
	# foreach my $FH ( map { /^(?:ARGV)$/ ? () : *{$::{$_}}{IO} || () } keys %::) {
		close($FH);
	}
	
	open(STDIN,'<','/dev/null');
	open(STDOUT,'>','/dev/null');
	#open(STDERR,'>','/dev/null');
	my($pid)=fork();

	if(defined($pid)) {
		if($pid==0) {
			
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
} else {
	print STDERR <<EOF ;
Usage: $0 {mutex_size} {program to run in mutex}
EOF
}
