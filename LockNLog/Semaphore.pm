#!/usr/bin/perl -W

package LockNLog::Semaphore;

use strict;

use Date::Parse;
use Fcntl qw(:flock SEEK_END O_RDWR O_CREAT);
use File::Path;

use Carp qw(croak);

use FindBin;
use lib "$FindBin::Bin/..";
use LockNLog;

###################
# Local variables #
###################

my($LOCKMAXCOUNTFILE)='semmaxcount.bin';
my($LOCKCOUNTFILE)='semcount.bin';
my($LOCKPIDSFILE)='sempids.txt';
my($LOCKWAITFILE)='semwait.txt';
my($LOCKUPTIMEFILE)='semuptime.txt';

##############
# Prototypes #
##############
sub new($;$$$);

sub Init($$$);
sub Wait($);
sub Signal($);

sub startKnockers($@);
sub exlock($$);
sub shlock($$);
sub unlock($$);

###############
# Constructor #
###############
sub new($;$$$) {
	my($class,$maxcount,$ejt,$prefix)=@_;
	
	my($self)={};

	$prefix=''  unless(defined($prefix));
	
	# Creating lock files (if they do not exist)
	my(@knockers)=startKnockers($self,$prefix.$LOCKMAXCOUNTFILE,$prefix.$LOCKCOUNTFILE,$prefix.$LOCKPIDSFILE,$prefix.$LOCKWAITFILE,$prefix.$LOCKUPTIMEFILE);
	
	croak("Unable to create lock files!!!!")  if(scalar(@knockers) eq 0);
	($self->{'MCFILE'},$self->{'CFILE'},$self->{'PFILE'},$self->{'WFILE'},$self->{'UFILE'})=@knockers;
	
	bless($self,$class);
	
	# Now
	return $self->Init($maxcount,$ejt);
}

sub Init($$$) {
	my($self,$maxcount,$ejt)=@_;
	
	$maxcount=10  unless(defined($maxcount));
	croak("Too high monitor value $maxcount")  if($maxcount>255);
	
	$ejt=5  unless(defined($ejt));
	croak("Too high estimated job time $ejt")  if($ejt>10);
	croak("Too low estimated job time $ejt")  if($ejt<1);
	
	$self->{maxcount}=$maxcount;
	$self->{sleeptime}=$ejt;
	
	my($LOCKUPTIME);
	my($LOCKMAXCOUNT);
	my($LOCKWAIT);
	my($LOCKCOUNT);
	my($LOCKPIDS);
	
	# Files to be locked down
	my($lockuptime)=$self->exlock('UFILE');
	my($lockmaxcount)=$self->exlock('MCFILE');
	my($lockwait)=$self->exlock('WFILE');
	my($lockcount)=$self->exlock('CFILE');
	my($lockpids)=$self->exlock('PFILE');
	
	my($newlock)=undef;
	my($uptime)=uptime();
	
	# First, reboot detection
	if(-e $lockuptime) {
		open($LOCKUPTIME,'<',$lockuptime) || die "Can't get lock!!!";
		my($storeduptime)=<$LOCKUPTIME>;
		close($LOCKUPTIME);
		
		unless(($storeduptime-5)<=$uptime && $uptime<=($storeduptime+5)) {
			$newlock=1;
		}
	} else {
		$newlock=1;
	}
	
	# Second, file corruption detection
	if(!defined($newlock) && -e $lockcount && ((-s $lockcount) == 2) && -e $lockmaxcount && ((-s $lockmaxcount) == 2)) {
		my($storedcount)=undef;
		my($storedmaxcount)=undef;
		my($runcount)=undef;
		
		open($LOCKMAXCOUNT,'+<',$lockmaxcount) || die "Can't get lock!!!";
		my($lect)='';
		read($LOCKMAXCOUNT,$lect,2);
		seek($LOCKMAXCOUNT,0,0);
		$storedmaxcount=unpack('S',$lect);
		
		open($LOCKCOUNT,'+<',$lockcount) || die "Can't get lock!!!";
		$lect='';
		read($LOCKCOUNT,$lect,2);
		seek($LOCKCOUNT,0,0);
		$storedcount=unpack('S',$lect);
		
		# First checks
		if($storedcount > $storedmaxcount) {
			# At this point we cannot reuse the infrastructure, so new lock infrastructure
			$newlock=1;
		} else {
			# let's count running instances
			open($LOCKPIDS,'<',$lockpids) || die "Can't get lock!!!";
			my($line)=<$LOCKPIDS>;
			close($LOCKPIDS);

			$line=''  unless(defined($line));
			my($zombie)=0;
			my(@alive)=();
			foreach my $pid (split(/ /,$line)) {
				if(kill(0,$pid)) {
					push(@alive,$pid);
				} else {
					$zombie++;
				}
			}

			# There were running zombies, after all
			if($zombie>0) {
				# Let's write
				open($LOCKPIDS,'>',$lockpids) || die "Can't update lock!!!";
				print $LOCKPIDS join(' ',@alive);
				close($LOCKPIDS);
				# $storedcount+=$zombie;
				$storedcount = $maxcount - scalar(@alive);
				print STDERR "STOREDAFTER ",$storedcount,"\n";

				# Let's write again
				#open($LOCKCOUNT,'>',$lockcount) || die "Can't update lock!!!";
				print $LOCKCOUNT pack('S',$storedcount);
				seek($LOCKCOUNT,0,0);
			}

			# The ones which are waiting
			open($LOCKWAIT,'<',$lockwait) || die "Can't get lock!!!";
			$line=<$LOCKWAIT>;
			close($LOCKWAIT);

			$line=''  unless(defined($line));
			$zombie=0;
			@alive=();
			foreach my $pid (split(/ /,$line)) {
				if(kill(0,$pid)) {
					push(@alive,$pid);
				} else {
					$zombie++;
				}
			}

			# There were waiting zombies, after all
			if($zombie>0) {
				# Let's write
				open($LOCKWAIT,'>',$lockwait) || die "Can't update lock!!!";
				print $LOCKWAIT join(' ',@alive);
				close($LOCKWAIT);
			}
			
			# Corrupted counter due reboot perhaps, so fix it!
			#if((scalar(@alive)+$storedcount)!=$maxcount) {
			#	print $LOCKCOUNT pack('S',($maxcount-scalar(@alive)));
			#}
			
			if($storedmaxcount != $maxcount) {
				print $LOCKMAXCOUNT pack('S',$maxcount);
			}
			# And some alive ones could be signaled!
			kill(18,@alive)  if($storedcount>0);
		}
		close($LOCKCOUNT);
		close($LOCKMAXCOUNT);
	} else {
		$newlock=1;
	}
	
	if(defined($newlock)) {
		open($LOCKUPTIME,'>',$lockuptime) || die "Can't create lock!!!";
		print $LOCKUPTIME $uptime;
		close($LOCKUPTIME);
		
		open($LOCKMAXCOUNT,'>',$lockmaxcount) || die "Can't create lock!!!";
		print $LOCKMAXCOUNT pack('S',$maxcount);
		close($LOCKMAXCOUNT);
		
		open($LOCKCOUNT,'>',$lockcount) || die "Can't create lock!!!";
		print $LOCKCOUNT pack('S',$maxcount);
		close($LOCKCOUNT);
		
		open($LOCKPIDS,'>',$lockpids) || die "Can't create lock!!!";
		close($LOCKPIDS);
		
		open($LOCKWAIT,'>',$lockwait) || die "Can't create lock!!!";
		close($LOCKWAIT);
	}
	$self->unlock('PFILE');
	$self->unlock('CFILE');
	$self->unlock('WFILE');
	$self->unlock('MCFILE');
	$self->unlock('UFILE');
	
	return $self;
}

sub Wait($) {
	my($self)=@_;
	
	# Guilty until the opposite...
	my($LOCKWAIT);
	my($lockwait)=$self->exlock('WFILE');
	open($LOCKWAIT,'+<',$lockwait) || die "Can't wait lock!!!";
	my($line)=<$LOCKWAIT>;
	seek($LOCKWAIT,0,0);
	$line=''  unless(defined($line));
	my(@wpids)=split(/ /,$line);
	push(@wpids,$$);
	print $LOCKWAIT join(' ',@wpids);
	close($LOCKWAIT);
	$self->unlock('WFILE');
	
	# Now, the loop where we are waiting our turn
	my($LOCKCOUNT);
	my($lockcount)=$self->{'CFILE'};
	open($LOCKCOUNT,'+<',$lockcount) || die "Can't wait lock!!!";
	my($storedcount)=undef;
	for(;;) {
		my($lect);
		$self->exlock('CFILE');
		read($LOCKCOUNT,$lect,2);
		seek($LOCKCOUNT,0,0);
		$storedcount=unpack('S',$lect);

		last if($storedcount>0);
		$self->unlock('CFILE');

		# We are going to sleep at most 5 minutes
		sleep($self->{sleeptime});
	}

	$storedcount--;

	print $LOCKCOUNT pack('S',$storedcount);
	close($LOCKCOUNT);
	
	# Now we have the turn, it is time to mark our presence
	my($LOCKPIDS);
	my($lockpids)=$self->exlock('PFILE');
	open($LOCKPIDS,'+<',$lockpids) || die "Can't wait lock!!!";
	$line=<$LOCKPIDS>;
	seek($LOCKPIDS,0,0);
	$line=''  unless(defined($line));
	my(@ppids)=split(/ /,$line);
	push(@ppids,$$);
	print $LOCKPIDS join(' ',@ppids);
	close($LOCKPIDS);
	$self->unlock('PFILE');
	
	# count is redundant related to pids, but count will guard any pids modification
	$self->unlock('CFILE');
	
	# And to erase ourselves from the list
	$lockwait=$self->exlock('WFILE');
	open($LOCKWAIT,'<',$lockwait) || die "Can't wait lock!!!";
	$line=<$LOCKWAIT>;
	close($LOCKWAIT);

	$line=''  unless(defined($line));
	my(@alive)=();
	foreach my $pid (split(/ /,$line)) {
		next if($pid eq $$);
		push(@alive,$pid);
	}
	open($LOCKWAIT,'>',$lockwait) || die "Can't wait lock!!!";
	print $LOCKWAIT join(' ',@alive);
	close($LOCKWAIT);
	$self->unlock('WFILE');
}

sub Signal($) {
	my($self)=@_;

	# Updating counter
	my($LOCKCOUNT);
	my($lockcount)=$self->exlock('CFILE');
	open($LOCKCOUNT,'+<',$lockcount) || die "Can't signal lock!!!";
	my($lect);
	read($LOCKCOUNT,$lect,2);
	seek($LOCKCOUNT,0,0);
	my($storedcount)=unpack('S',$lect);
	$storedcount++;
	print $LOCKCOUNT pack('S',$storedcount);
	close($LOCKCOUNT);
	
	# Then, removing ourselves from the list
	my($LOCKPIDS);
	my($lockpids)=$self->exlock('PFILE');
	open($LOCKPIDS,'<',$lockpids) || die "Can't signal lock!!!";
	my($line)=<$LOCKPIDS>;
	close($LOCKPIDS);

	$line=''  unless(defined($line));
	my(@alive)=();
	foreach my $pid (split(/ /,$line)) {
		next if($pid == $$);
		push(@alive,$pid);
	}
	open($LOCKPIDS,'>',$lockpids) || die "Can't signal lock!!!";
	print $LOCKPIDS join(' ',@alive);
	close($LOCKPIDS);
	$self->unlock('PFILE');
	
	# count is redundant related to pids, but count will guard any pids modification
	$self->unlock('CFILE');
	
	# And last, waking up others!
	my($LOCKWAIT);
	my($lockwait)=$self->shlock('WFILE');
	open($LOCKWAIT,'<',$lockwait) || die "Can't signal lock!!!";
	$line=<$LOCKWAIT>;
	close($LOCKWAIT);
	$self->unlock('WFILE');
	
	$line=''  unless(defined($line));
	my(@waiters)=split(/ /,$line);
	if(scalar(@waiters)>0) {
		# Give a chance to the first in the list!
		kill(18,$waiters[0]);
		# And then, the others!
		if(scalar(@waiters)>1) {
			shift(@waiters);
			sleep(1);
			kill(18,@waiters);
		}
	}
}

# internal convenience methods
sub startKnockers($@) {
	my($self,@knocknames)=@_;
	
	my(@knockh)=();
	
	foreach my $kname (@knocknames) {
		$kname = LockNLog::doExt($kname,'','');
		my($lckname)=$kname.'.lck';
		unless(open($self->{$lckname},'>',$lckname)) {
			foreach my $khname (@knockh) {
				$khname.='.lck';
				close($self->{$khname});
				delete($self->{$khname});
			}
			return undef;
		}
		
		push(@knockh,$kname);
	}
	
	return @knockh;
}

sub exlock($$) {
	my($self,$lockname)=@_;
	$lockname = $self->{$lockname};
	flock($self->{$lockname.'.lck'},LOCK_EX);
	
	return $lockname;
}

sub shlock($$) {
	my($self,$lockname)=@_;
	$lockname = $self->{$lockname};
	flock($self->{$lockname.'.lck'},LOCK_SH);
	
	return $lockname;
}

sub unlock($$) {
	my($self,$lockname)=@_;
	$lockname = $self->{$lockname};
	flock($self->{$lockname.'.lck'},LOCK_UN);
	
	return $lockname;
}

sub uptime() {
	# Getting the ellapsed time of init process (1)
	my $elapStr = `LC_ALL=C ps -o etime -p 1 | tail -n 1`;
	my $time = time();
	# Trimming spaces
	$elapStr =~ s/^ +//;
	$elapStr =~ s/ +$//;
	if(length($elapStr)>0) {
		my $elap = 0;
		my @elapTok = split(/-/,$elapStr,2);
		if(scalar(@elapTok)>1) {
			# Translating days to seconds
			$elap += shift(@elapTok)*86400;
		}

		my @hms = split(/:/,$elapTok[0],3);
		unshift(@hms,'0')  if(scalar(@hms)==1);
		unshift(@hms,'0')  if(scalar(@hms)==2);
		$elap += int($hms[0])*3600+int($hms[1])*60+int($hms[2]);
		
		return $time-$elap;
	} elsif(-f '/proc/uptime') {
		my($UPTIME);
		open($UPTIME,'/proc/uptime') || die "Can't get lock!!!";
		my(@statdata)=stat($UPTIME);
		my($uptime)=<$UPTIME>;
		close($UPTIME);
		$time=time();
		my(@upti)=split(/ /,$uptime,2);
		
		return $time-int($statdata[9]-$upti[0]);
	} else {
		my $now = time();
		my $rebootDateStr = `last | grep '^reboot' | head -n 1 | cut -b 37-`;
		$rebootDateStr = `last | tail -n 1 | cut -b 13-`  unless(defined($rebootDateStr) && length($rebootDateStr)>0);

		if(defined($rebootDateStr) && length($rebootDateStr)>0) {
			my $rebootDate = str2time($rebootDateStr);
			# This case is almost unneeded, because str2time has into account timestamps without years
			if($rebootDate>$now) {
				my @nowComp = localtime($now);
				$rebootDate = str2time($rebootDateStr.' '.($nowComp[5]+1900-1));
			}

			#return $now-$rebootDate;
			return $rebootDate;
		} else {
			return 0;
		}

	}
}

1;
