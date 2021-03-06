#!/usr/bin/perl -W

package LockNLog::Mutex;

use strict;
use Carp qw(croak);
use Fcntl qw(:flock SEEK_END O_RDWR O_CREAT);
use File::Path;

use base qw(LockNLog::Semaphore);

##############
# Prototypes #
##############
sub new($;$$$);

sub mutex($&);

###############
# Constructor #
###############
sub new($;$$$) {
	my($class,$maxcount,$ejt,$prefix)=@_;
	
	my($self)=$class->SUPER::new($maxcount,$ejt,$prefix);
	
	return bless($self,$class);
}

sub mutex($&) {
	my($self,$block)=@_;
	$self->Wait();
	eval $block->();
	my($exp)=$@;
	$self->Signal();
	die "$exp\t...propagated"  if(defined($exp) && length($exp)>0);
}

1;
