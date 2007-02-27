#!/usr/bin/perl -W

use strict;

package LockNLog;

use File::Path;
use Fcntl qw(:flock SEEK_END O_RDWR O_CREAT);
use Time::HiRes qw(sleep);
use DB_File;
use POSIX qw(strftime);

use vars qw($LOCKNLOGDIR $LOCKNLOGMASK);
use vars qw($LOGFILENAME $WHITELIST $GRAYLIST $GRAYGROUPLIST $BLACKLIST $LOCKFILE);
use vars qw($BASEDELAY $BLACKDELAY $WHITEDELAY $GRAYBASE $RENEWLEASE);

$LOCKNLOGMASK=0755;
$LOCKNLOGDIR='logs';
$LOGFILENAME='logfile';
$WHITELIST='whitelist';
$GRAYLIST='graylist';
$GRAYGROUPLIST='graygrouplist';
$BLACKLIST='blacklist';
$LOCKFILE='LockNLog_lockfile';

$BASEDELAY=0;
$BLACKDELAY=90;
$WHITEDELAY=0;
$GRAYBASE=10;
$RENEWLEASE=60*60*24;	# Just a 24h day

sub logStartNDelay($$;$$$$);
sub logDelay($;$);
sub logEntry($$$;$$$);
sub getPrintableNow();
sub doExt($$;$);
sub matchIP($$);
sub JobIdGenerator();

sub logStartNDelay($$;$$$$) {
	my($name,$jobid,$stage,$ip,$misc,$infix)=@_;
	my($delay)=$BASEDELAY;
	
	if(!defined($ip) && (exists($ENV{'REMOTE_ADDR'}) || exists($ENV{'HTTP_X_FORWARDED_FOR'}))) {
		$ip=(exists($ENV{'HTTP_X_FORWARDED_FOR'})?$ENV{'HTTP_X_FORWARDED_FOR'}:$ENV{'REMOTE_ADDR'});
	}
	
	if(defined($ip)) {
		$stage='start'  unless(defined($stage));
		logEntry($name,$jobid,$stage,$ip,$misc,$infix);
		my($ldelay)=logDelay($ip,$infix);
		$ldelay=$BLACKDELAY  unless(defined($ldelay));
		$delay+=$ldelay;
	}
	
	sleep($delay);
}

sub doExt($$;$) {
	my($name,$infix,$suffix)=@_;
	
	mkpath($LOCKNLOGDIR,1,$LOCKNLOGMASK);
	
	$infix=''  unless(defined($infix));
	$suffix='.txt'  unless(defined($suffix));
	
	return $LOCKNLOGDIR.'/'.$name.$infix.$suffix;
}

sub matchIP($$) {
	my($file,$ip)=@_;
	my($status)=undef;
	
	local(*LIST);
	
	if(open(LIST,'<',$file)) {
		my($line);
		while($line=<LIST>) {
			chomp($line);
			if($line eq $ip) {
				$status=1;
				last;
			}
		}
		close(LIST);
	}
	
	return $status;
}

sub logDelay($;$) {
	my($ip,$infix)=@_;
	my($delay)=undef;
	
	local(*LOCK);
	
	$infix=defined($infix)?('_'.$infix):'';
	
	my($lockfile)=doExt($LOCKFILE,$infix);
	
	if(open(LOCK,'>',$lockfile)) {
		# It is like a semaphore
		flock(LOCK,LOCK_EX);
		
		my($justnow)=time;
		
		# Getting filenames
		my($whitelist)=doExt($WHITELIST,$infix);
		my($graylist)=doExt($GRAYLIST,$infix);
		my($graygrouplist)=doExt($GRAYGROUPLIST,$infix);
		my($blacklist)=doExt($BLACKLIST,$infix);
		
		# It is not an error that the file does not exist
		$delay=$BLACKDELAY  if(matchIP($blacklist,$ip));
		
		$delay=$WHITEDELAY  if(!defined($delay) && matchIP($whitelist,$ip));
		
		# Now, the gray lists!
		unless(defined($delay)) {
			my(%IP);
			my(%IPGROUP);
			
			tie %IP,'DB_File',$graylist,O_RDWR|O_CREAT, 0666;
			tie %IPGROUP,'DB_File',$graygrouplist,O_RDWR|O_CREAT, 0666;
#			tie %IP,'NDBM_File',$graylist, O_RDWR|O_CREAT, 0666;
#			tie %IPGROUP,'NDBM_File',$graygrouplist, O_RDWR|O_CREAT, 0666;
			
			my($ipgroup)=substr($ip,0,rindex($ip,'.'));
			my($ipkey)='first_'.$ip;
			my($ipgroupkey)='first_'.$ipgroup;
			
			# Tracking first occurrences
			unless($IP{$ip}) {
				$IP{$ip}=0;
				$IP{$ipkey}=$justnow;
			}
			unless($IPGROUP{$ipgroup}) {
				$IPGROUP{$ipgroup}=0;
				$IPGROUP{$ipgroupkey}=$justnow;
			}
			
			# Clearing 'strange' dates
			$IP{$ipkey}=$justnow  if($IP{$ipkey}<$justnow);
			$IPGROUP{$ipgroupkey}=$justnow  if($IPGROUP{$ipgroupkey}<$justnow);
			
			# And now, clearing old dates assigned counts
			$IP{$ip}=0  if($IP{$ipkey}+$RENEWLEASE < $justnow);
			$IPGROUP{$ipgroup}=0  if($IPGROUP{$ipgroupkey}+$RENEWLEASE < $justnow);
			
			$IP{$ip}=$IP{$ip}+1;
			$IPGROUP{$ipgroup}=$IPGROUP{$ipgroup}+1;
			
			my($expdelay)=$IP{$ip};
			
			$expdelay=$IPGROUP{$ipgroup}  if($expdelay<$IPGROUP{$ipgroup});
			$delay=log($expdelay)/log($GRAYBASE);
			
			untie %IPGROUP;
			untie %IP;
		}

		close(LOCK);
	}
	
	return $delay;
}

sub logEntry($$$;$$$) {
	my($name,$jobid,$stage,$ip,$misc,$infix)=@_;
	
	if(!defined($ip) && (exists($ENV{'REMOTE_ADDR'}) || exists($ENV{'HTTP_X_FORWARDED_FOR'}))) {
		$ip=(exists($ENV{'HTTP_X_FORWARDED_FOR'})?$ENV{'HTTP_X_FORWARDED_FOR'}:$ENV{'REMOTE_ADDR'});
	}
	
	if(defined($ip)) {
		my($now)=getPrintableNow();
		
		$infix=defined($infix)?('_'.$infix):'';
		
		$name='undefined'  unless(defined($name));
		$stage='unknown'  unless(defined($stage));
		$misc=''  unless(defined($misc));
		
		$name =~ tr/\n\t/ /s;
		$stage =~ tr/\n\t/ /s;
		$misc =~ tr/\n\t/ /s;
	
		local(*LOGFILE);
		
		my($logfilename)=doExt($LOGFILENAME,$infix);
		
		if(open(LOGFILE,'>>',$logfilename)) {
			flock(LOGFILE,LOCK_EX);
			print LOGFILE $now,"\t",$ip,"\t",$name,"\t",$jobid,"\t",$stage,"\t",$misc,"\n";
			close(LOGFILE);
		}
	}
}

sub getPrintableNow() {
	my $now = time();

	# We need to munge the timezone indicator to add a colon between the hour and minute part
	my $tz = strftime("%z", localtime($now));
	$tz =~ s/(\d{2})(\d{2})/$1:$2/;

	# ISO8601
	return strftime("%Y-%m-%dT%H:%M:%S", localtime($now)) . $tz;
}

sub JobIdGenerator()
{
	my($subjobid)=@_;
	
	return time.'_'.$$;
}

1;
