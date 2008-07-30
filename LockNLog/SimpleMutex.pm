#!/usr/bin/perl -W

package LockNLog::SimpleMutex;

use strict;
use Carp qw(croak);
use Fcntl qw(:flock SEEK_END O_RDWR O_CREAT);
use File::Path;

##############
# Prototypes #
##############
sub new($;$$);

sub mutex($&);

###############
# Constructor #
###############
sub new($;$$) {
	my($class,$lockfile,$lockmode)=@_;
	
	my($self)={lockfile=>$lockfile,lockmode=>$lockmode};
	
	return bless($self,$class);
}

sub mutex($&) {
	my($self,$block)=@_;
	
	my($FH);
	open($FH,'>',$self->{lockfile}) || croak("UNABLE TO START LOCKING");
	my($lockmode)=defined($self->{lockmode})?LOCK_EX:LOCK_SH;
	flock($FH,$lockmode);
	eval $block->();
	close($FH);
}

1;