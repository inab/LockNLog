#!/usr/bin/perl -W

use strict;

use Time::HiRes qw(time);
use LockNLog;

foreach my $counter (1..100) {
	my($time1)=time;
	LockNLog::logStartNDelay('coco',undef,'192.168.0.1');
	my($time2)=time;
	print STDOUT $counter,' ',($time2-$time1),"\n";
}
