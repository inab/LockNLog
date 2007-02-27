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
sub new($;$$);

sub mutex($&);

###############
# Constructor #
###############
sub new($;$$) {
	my($class,$maxcount,$ejt)=@_;
	
	my($self)=$class->SUPER::new($maxcount,$ejt);
	
	return bless($self,$class);
}

sub mutex($&) {
	my($self,$block)=@_;
	$self->Wait();
	eval $block->();
	$self->Signal();
}

1;