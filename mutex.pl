#!/usr/bin/perl -W

use strict;

use LockNLog::Mutex;

my($mutex)=LockNLog::Mutex->new(5);

$mutex->mutex(
sub {
	system(@ARGV);
});
