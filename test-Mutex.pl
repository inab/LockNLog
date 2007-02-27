#!/usr/bin/perl -W

use strict;

use LockNLog::Mutex;

my($mutex)=LockNLog::Mutex->new();

$mutex->mutex(
sub {
	$|=1;
	print "Open $$\n";
	sleep(10);
	print "Close $$\n";
});