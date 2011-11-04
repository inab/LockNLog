#!/usr/bin/perl -W

package LockNLog::MarxCabin;

use strict;

use Data::Dumper;
use Date::Parse;
use Fcntl qw(:flock SEEK_END O_RDWR O_CREAT);
use File::Path;
use POSIX qw(setsid :sys_wait_h);
use Carp qw(croak);

use FindBin;
use lib "$FindBin::Bin/..";
use LockNLog;

###################
# Local variables #
###################

my $LOCKPIDFILE ='marxpid.txt';

my $LOCKWAITFILE ='marxwait.txt';

my $LOCKUPTIMEFILE ='marxuptime.txt';

my $MARXSTAT = 'marxStat';

##############
# Prototypes #
##############
sub Queue($$\@;$$$);

sub init($$$$$);
sub daemon($$);

sub startKnockers($@);
sub exlock($$);
sub shlock($$);
sub unlock($$);

###############
# Constructor #
###############
sub Queue($$\@;$$$) {
	my($class,$parent,$p_params,$maxcount,$ejt,$prefix)=@_;
	
	my($self)={};

	$prefix=''  unless(defined($prefix));

	# Creating lock files (if they do not exist)
	my(@knockers)=startKnockers($self,$prefix.$LOCKPIDFILE,$prefix.$LOCKWAITFILE,$prefix.$LOCKUPTIMEFILE);
	
	croak("Unable to create lock files!!!!")  if(scalar(@knockers) eq 0);
	($self->{'PFILE'},$self->{'WFILE'},$self->{'UFILE'})=@knockers;
	
	bless($self,$class);
	
	# Now
	return $self->init($parent,$p_params,$maxcount,$ejt);
}

sub init($$$$$) {
	my($self,$parent,$p_params,$maxcount,$ejt)=@_;
	
	$maxcount=10  unless(defined($maxcount));
	croak("Too high monitor value $maxcount")  if($maxcount>255);
	
	$ejt=5  unless(defined($ejt));
	croak("Too high estimated job time $ejt")  if($ejt>10);
	croak("Too low estimated job time $ejt")  if($ejt<1);
	
	$self->{maxcount}=$maxcount;
	$self->{sleeptime}=$ejt;
	
	my($LOCKUPTIME);
	my($LOCKPID);
	my($LOCKWAIT);
	
	# Files to be locked down
	my($lockuptime)=$self->exlock('UFILE');
	my($lockpid)=$self->exlock('PFILE');
	my($lockwait)=$self->exlock('WFILE');
	
	my($newdaemon)=undef;
	my($uptime)=uptime();
	
	# First, reboot detection
	if(-e $lockuptime) {
		open($LOCKUPTIME,'<',$lockuptime) || die "Can't get lock!!!";
		my($storeduptime)=<$LOCKUPTIME>;
		close($LOCKUPTIME);
		
		unless(($storeduptime-5)<=$uptime && $uptime<=($storeduptime+5)) {
			$newdaemon=1;
		}
	} else {
		$newdaemon=1;
	}
	
	# Second, daemon pid detection
	if(!defined($newdaemon)) {
		if(-e $lockpid) {
			open($LOCKPID,'<',$lockpid) || die "Can't get lock!!!";
			my($daemonpid)=<$LOCKPID>;
			close($LOCKPID);
			
			unless(defined($daemonpid) && $daemonpid =~ /^[1-9][0-9]*$/ && kill(0,$daemonpid)) {
				$newdaemon=1;
			}
		} else {
			$newdaemon=1;
		}
	}
	
	my $marxStatDir = $LockNLog::LOCKNLOGDIR.'/'.$MARXSTAT;
	if(defined($newdaemon)) {
		open($LOCKUPTIME,'>',$lockuptime) || die "Can't create lock!!!";
		print $LOCKUPTIME $uptime;
		close($LOCKUPTIME);
		
		open($LOCKPID,'>',$lockpid) || die "Can't create lock!!!";
		close($LOCKPID);
		
		open($LOCKWAIT,'>',$lockwait) || die "Can't create lock!!!";
		close($LOCKWAIT);
		
		# Cleaning previous stat dir
		eval {
			rmtree($marxStatDir,0,1);
		};
		
		# Creating current stat dir
		eval {
			mkpath($marxStatDir,0,0755);
		};
	}
	
	
	# Preparing the payload for the daemon
	my $PAR;
	if(open($PAR,'>',$marxStatDir.'/'.$parent.'.txt')) {
		print $PAR Data::Dumper->Dump([$p_params]);
		close($PAR);
		
		# Let's queue the new job
		open($LOCKWAIT,'>>',$lockwait) || die "Can't create lock!!!";
		print $LOCKWAIT ' ',$parent;
		close($LOCKWAIT);
	}
	
	# Starting the daemon (if needed)
	if(defined($newdaemon)) {
		# We have to ignore pleas from the children
		$SIG{CHLD}='IGNORE';

		# As we are using dumb forks, we must close connections to the parent process
		# close $_ for map { /^(?:ARGV|std(?:err|out|in)|STD(?:ERR|OUT|IN))$/ ? () : *{$::{$_}}{IO} || () } keys %::;
		foreach my $FH ( map { /^(?:ARGV|STDERR|stderr)$/ ? () : *{$::{$_}}{IO} || () } keys %::) {
			close($FH);
		}
		
		open(STDIN,'<','/dev/null');
		open(STDOUT,'>','/dev/null');
		#open(STDERR,'>','/dev/null');
		my($pid)=fork();

		if(defined($pid)) {
			if($pid==0) {
				# Born perhaps with undesired locks
				$self->unlock('WFILE');
				$self->unlock('PFILE');
				$self->unlock('UFILE');
				$lockuptime=$self->exlock('UFILE');
				$self->unlock('UFILE');
				
				setsid();
				
				exit $self->daemon($marxStatDir);
			} else {
				# As the parent, register the daemon
				open($LOCKPID,'>>',$lockpid) || die "Can't create lock!!!";
				print $LOCKPID $pid;
				close($LOCKPID);
			}
		} else {
			die "ERROR: Unable to fork process!!!!";
		}
	}
	
	$self->unlock('WFILE');
	$self->unlock('PFILE');
	$self->unlock('UFILE');
	
	return $self;
}

my $nChildren = undef;
my $maxChildren = undef;
my $ejt = undef;
my %children = ();

sub REAPER {
	# don't change $! and $? outside handler
	local ($!, $?);
	my $child;
	# If a second child dies while in the signal handler caused by the
	# first death, we won't get another signal. So must loop here else
	# we will leave the unreaped child as a zombie. And the next time
	# two children die we get another zombie. And so on.
	#print STDERR $nChildren,"\n";
	while(($child = waitpid(-1, WNOHANG))!=-1) {
	#return  if($child==-1);
		next  unless(exists($children{$child}));
		$nChildren--;
		delete($children{$child});
	}
}

sub daemon($$) {
	my($self,$marxStatDir)=@_;
	local $/;
	
	$nChildren = 0;
	$maxChildren = $self->{maxcount};
	$ejt = $self->{sleeptime};
	
	# The signal handler
	my $termHandler = sub {
		$SIG{CHLD}='IGNORE';
		
		kill(9,keys(%children));
		
		exit 1;
	};
	$SIG{INT}=$SIG{TERM}=$SIG{QUIT}=$termHandler;
	
	$SIG{CHLD}=\&REAPER;
	
	my $last = undef;
	do {
		# First, who is queued?
		my $lockpid = $self->exlock('PFILE');
		my($lockwait)=$self->exlock('WFILE');
		my($LOCKWAIT);
		my($LOCKPID);
		open($LOCKWAIT,'<',$lockwait) || die "Can't wait lock!!!";
		my($line)=<$LOCKWAIT>;
		close($LOCKWAIT);
		$line=''  unless(defined($line));
		my(@wpids)=split(/ /,$line);
		
		# Giving up!
		if(scalar(@wpids)==0) {
			# There are unfinished children. Wait for them!
			if($nChildren>0) {
				$self->unlock('WFILE');
				$self->unlock('PFILE');
				sleep($ejt);
			} else {
				open($LOCKPID,'>',$lockpid) || die "Can't wait lock!!!";
				close($LOCKPID);
				$self->unlock('WFILE');
				$self->unlock('PFILE');
				$last=0;
			}
		} else {
			# Look for a candidate
			my $wcount = 0;
			my $doFree = 1;
			foreach my $wpid (@wpids) {
				$wcount++;
				next if($wpid eq '');
				
				# Is the process still running?
				my $param_file = $marxStatDir.'/'.$wpid.'.txt';
				if(kill(0,$wpid)) {
					my $CLINE;
					
					# Skip those programs with no command line
					if(open($CLINE,'<',$param_file)) {
						my $line = <$CLINE>;
						
						my $VAR1;
						my $p_params = eval($line);
						
						close($CLINE);
						unlink($param_file);
						
						my @newpids = ();
						
						# Removing possible duplicates
						foreach my $npid (@wpids[$wcount..$#wpids]) {
							next  if($npid eq '' || $npid==$wpid);
							push(@newpids,$npid);
						}
						
						open($LOCKWAIT,'>',$lockwait) || die "Can't wait lock!!!";
						print $LOCKWAIT join(' ',@newpids);
						#print STDERR join(' ',@newpids),"\n";
						close($LOCKWAIT);
						$self->unlock('WFILE');
						$self->unlock('PFILE');
						$doFree=undef;
						
						# Are there free slots?
						while($nChildren >= $maxChildren) {
							print STDERR $nChildren,"\n";
							sleep($ejt);
						}
							print STDERR $nChildren,"\n";
						
						# And now, run the program!
						$nChildren++;
						my $pid = fork();
						if(defined($pid)) {
							if($pid==0) {
								# Becoming the program, of course!
								exec(@{$p_params});
							} else {
								# Let's store the PID in the shared array
								$children{$pid}=undef;
							}
						} else {
							# So low on resources that it cannot be spawned?!?!?!
							system(@{$p_params});
							$nChildren--;
						}
						
						last;
					}
				} elsif(-f $param_file) {
					# Erasing unwanted junk
					unlink($param_file);
				}
			}
			
			# Readcquiring locks
			if(defined($doFree)) {
				$self->unlock('WFILE');
				$self->unlock('PFILE');
			}
		}
	} until(defined($last));
	
	return $last;
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
