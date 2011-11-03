#!/usr/bin/perl -W

use strict;

use FindBin;
use lib "$FindBin::Bin";
use LockNLog::MarxCabin;

if(scalar(@ARGV)>2) {
	my $concurr = shift(@ARGV);
	$concurr = ($concurr =~ /^[1-9][0-9]*$/)?$concurr:5;
	
	my $parent = getppid();
	
	my @PARAMS = @ARGV;
	
	my $marx = LockNLog::MarxCabin->Queue($parent,\@PARAMS,$concurr);
} else {
	print STDERR <<EOF ;
Usage: $0 {mutex_size} {program to run in mutex}
EOF
}


exit 0;