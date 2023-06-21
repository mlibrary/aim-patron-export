#!/usr/bin/perl 

use strict; 
use File::Basename; 
use Getopt::Std; 
use XML::LibXML; 
use Data::Dumper; 
use FileHandle;
use Class::Date qw(:errors date localdate gmdate now -DateParse);
use Text::CSV_XS;
my  $csv = Text::CSV_XS->new ( { binary => 1, eol => "\n", always_quote => 1 } )  # should set binary attribute.
                 or die "Cannot use CSV: ".Text::CSV_XS->error_diag ();

my $prgname = basename($0);
our ($opt_i, $opt_d, $opt_o);
getopts('i:d:o:');
$opt_d or die usage("no input dir specified");
$opt_i or die usage("no input base specified");
$opt_o or die usage("no output base specified");
my $inbase = "$opt_i";
my $indir = "$opt_d";
my $outbase = "$opt_o";

my $fh_libauth = new FileHandle ">$opt_o.libauth" or die "can't open $opt_o.libauth for output: $!\n";
my $fh_illiad = new FileHandle ">$opt_o.illiad" or die "can't open $opt_o.illiad for output: $!\n";
my $fh_report = new FileHandle ">$opt_o.report" or die "can't open $opt_o.report for output: $!\n";
my $fh_metrics = new FileHandle ">$opt_o.metrics" or die "can't open $opt_o.metrics for output: $!\n";
binmode $fh_libauth, "utf8";
binmode $fh_illiad, "utf8";

print STDERR "$indir, $inbase\n";
my $parser = XML::LibXML->new();

#my $today = `date +"%Y%m%d"`;
my $today = localdate now;
print STDERR "today is $today\n";

my $patron_dir = $opt_d;
opendir my $dh, $indir  or die "can't open directory $indir: $!\n";

my @file_list = sort grep {$_ =~ /^$inbase/}  readdir($dh);
my $filecnt_total = scalar @file_list;
print STDERR "processing $filecnt_total files\n";

#EXPORT_USERS-15312374800006381-16267316634

my @libauth_bor_type_exclude = (
#'FA',	# Faculty                
#'GE',	# GEO                    
#'ST',	# Staff                  
#'GR',	# Graduate               
#'UN',	# Undergrad              
#'SA',	# Sponsored Affiliate    
#'VS',	# Visiting Scholar       
#'TS',	# Temp Staff             
#'CN',	# Contractor             
#'CD',	# Candidate              
#'DS',	# Detached Study         
#'AF',	# Adjunct Faculty        
'AL',	# Alumni-Fee Based       
'CA',	# Carrel                 
'CI',	# CIC                    
'D1',	# Dietetics Intern Med   
#'DB',	# Debater                
'DO',	# Docent                 
#'EM',	# Emeritus               
'GS',	# Guest                  
'HH',	# Howard Hughes Med Inst 
'IL',	# ILL                    
'JF',	# Journalism Fellow      
#'MI',	# Faculty from Michigan  
#'EU',	# EMU                    
#'MR',	# MRLT                   
'OT',	# Other                  
'PR',	# Proxy                  
'RC',	# Religious Counselor    
'RE',	# Reserve                
#'RF',	# Retired Faculty        
#'RS',	# Retired Staff          
#'SP',	# Spouse                 
'SU',	# Summer Program         
'WC',	# Wash Comm College      
'WD',	# William Davidson Inst  
  );

my @illiad_bor_type_exclude = (
  "AG", "AL", "CA", "CI",
  "DB", "DC", "EP", "G1", "G2", "IL",
  "MI", "MR", "NB", "OT", "RL", "SF",
  "TS", "SP",
  );

my $incnt = 0;
my $filecnt = 0;
my $filecnt_total = 0;
my $expired = 0;
my $libauth_no_umid = 0;
my $illiad_no_umid = 0;
my $illiad_user_group_skipped = 0;
my $illiad_statistic_category_skipped = 0;
my $libauth_statistic_category_skipped = 0;
my $out_illiad = 0;
my $out_libauth = 0;
my $no_campus_code = 0;
my $pushgateway_metrics = "";
my $job = "aim_patron_extract";

foreach my $file (@file_list) {
  next if $file =~ /^\./;
  $filecnt++;
  $filecnt % 100 == 0 and print STDERR "processing file $filecnt of $filecnt_total\n";
  process_file(join("/", $patron_dir, $file));
}
print $fh_report "$filecnt files processed\n";
print $fh_report "$incnt records read\n";
print $fh_report "$expired expired patrons skipped\n";
print $fh_report "$no_campus_code no campus code for record\n";
print $fh_report "$illiad_no_umid ILLIAD: patrons without umid skipped\n";
print $fh_report "$illiad_user_group_skipped ILLIAD: patrons skipped (user group)\n";
print $fh_report "$illiad_statistic_category_skipped ILLIAD: patrons skipped (statistic category)\n";
print $fh_report "$out_illiad illiad records written\n";
print $fh_report "$libauth_no_umid LIBAUTH patrons without umid--id generated)\n";
print $fh_report "$libauth_statistic_category_skipped LIBAUTH patrons skipped (statistic category)\n";
print $fh_report "$out_libauth libauth records written\n";

$filecnt_total = last_count($job, "files_processed_total") + $filecnt;

$pushgateway_metrics = <<"END_MESSAGE";
# HELP aim_patron_extract_files_processed_total Count of files exported from alma that have been processed by patron_extract script
# TYPE aim_patron_extract_files_processed_total counter
aim_patron_extract_files_processed_total $filecnt_total
END_MESSAGE

print $fh_metrics $pushgateway_metrics;


sub last_count {
 my $job = shift;
 my $metric = shift;
 return `/usr/local/bin/pushgateway_advanced -j $job -q ${job}_${metric}`;
}
 
sub process_file {
  my $filename = shift;
  my $fh = new FileHandle "<$filename" or die "can't create filehandle for $filename: $!\n";
  local $/=undef;
  my $data = <$fh>;
  my $dom = $parser->parse_string($data);

  NODE:foreach my $user ($dom->findnodes('/users/user')) {
    $incnt++;
    my $patron = process_node($user);
    #local $Class::Date::DATE_FORMAT="%Y-%m-%dZ";
    my $expire_date = localdate $patron->{expiry_date};
    $expire_date < $today and do {
      #print STDERR "patron expired, expire date is $expire_date\n";
      $expired++;
      next NODE;
    };
    write_libauth($patron, $fh_libauth);
    write_illiad($patron, $fh_illiad);
  }
}

sub process_node {
  my $user = shift;
  my $patron = {};
  $patron->{primary_id} = lc($user->findvalue('./primary_id'));
  $patron->{last_name} = $user->findvalue('./last_name');
  $patron->{first_name} = $user->findvalue('./first_name');
  $patron->{user_group} = $user->findvalue('./user_group'); 
  $patron->{statistic_category} = $user->findvalue('./user_statistics/user_statistic/statistic_category');
  $patron->{statistic_category_desc} = $user->findvalue('./user_statistics/user_statistic/statistic_category/@desc');
  $patron->{user_group_desc} = $user->findvalue('./user_group/@desc'); 
  $patron->{user_group_desc} =~ s/ Level//;
  $patron->{campus_code} = $user->findvalue('./campus_code');
  $patron->{umid} = ''; 
  $patron->{barcode} = ''; 
  ID:foreach my $user_identifier ($user->findnodes('./user_identifiers/user_identifier')) {
    $user_identifier->findvalue('./id_type') eq '01' and do {
      $patron->{barcode} = $user_identifier->findvalue('./value');
    };
    $user_identifier->findvalue('./id_type') eq '02' and do {
      $patron->{umid} = $user_identifier->findvalue('./value');
    };
  }
  $patron->{email_address} = '';
  EMAIL:foreach my $email ($user->findnodes('./contact_info/emails/email')) {
    $email->getAttribute('preferred') eq 'true' and do {
      $patron->{email_address} = $email->findvalue('./email_address');
      last EMAIL;
    };
  }
  $patron->{address1} = '';
  $patron->{address2} = '';
  $patron->{city} = '';
  $patron->{state} = '';
  $patron->{zip} = '';
  ADDRESS:foreach my $address ($user->findnodes('./contact_info/addresses/address')) {
    $address->getAttribute('preferred') eq 'true' and do {
      $patron->{address1} = $address->findvalue('./line1');
      $patron->{address2} = $address->findvalue('./line2');
      $patron->{city} = $address->findvalue('./city');
      $patron->{state} = $address->findvalue('./state_province');
      $patron->{zip} = $address->findvalue('./postal_code');
      last ADDRESS;
    };
  }
  #local $Class::Date::DATE_FORMAT="%Y-%m-%dZ";
  $patron->{expiry_date} = localdate $user->findvalue('./expiry_date');
  $patron->{job_description} = $user->findvalue('./job_description');
  #$patron->{phone_number} = $user->findvalue('./contact_info/phones/phone/phone_number');
  $patron->{phone_number} = $user->findvalue('./contact_info/phones/phone[@preferred=\'true\']/phone_number');
  $patron->{sms_phone_number} = $user->findvalue('./contact_info/phones/phone[@preferred_sms=\'true\']/phone_number');
  $patron->{print_id} = join("/", $patron->{primary_id}, $patron->{umid});

  $patron->{campus_code} or do {
    #print "$patron->{print_id}: no campus code, UMAA used\n";
    $no_campus_code++;
    $patron->{campus_code} = 'UMAA';
  };
  return $patron;
  #return clean_patron_hash($patron);
}

sub clean_patron_hash {
  my $patron = shift;
  foreach my $key (keys %$patron) {
    $patron->{$key} =~ s/\n\r/ /g;	# clean any embedded newlines/cr's
  }
  return $patron;
}

sub write_libauth {
  my $patron = shift;
  my $out_fh = shift;
  my ($dept_code) = $patron->{job_description} =~ /\((\d+)\)/;

  # check statistic category (bor_type)
  if ( $patron->{statistic_category} eq '' 						# no statistic category
    or grep /^$patron->{statistic_category}$/, @libauth_bor_type_exclude )		# cataegory to be excluded
    {
      print STDERR "$patron->{primary_id}: LIBAUTH patron skipped for statistic category $patron->{statistic_category}\n";
      $libauth_statistic_category_skipped++;
      return 0;
    }
  # check umid--if not present, generate one
  $patron->{umid} or do {
    $libauth_no_umid++;
    my $generated_umid = sprintf("%016X", rand(0xFFFFFFFFFFFFFF));
    $patron->{umid} = $generated_umid;
    print STDERR "LIBAUTH: $patron->{print_id}: no umid, bortype $patron->{statistic_category}: $generated_umid\n";
  };
  local $Class::Date::DATE_FORMAT="%Y-%m-%d";
  print $out_fh join("\t", 
      $patron->{primary_id},
      $patron->{last_name},
      $patron->{first_name},
      $dept_code,
      $patron->{user_group_desc},
      $patron->{campus_code},
      $patron->{umid},
      $patron->{email_address},
      $patron->{address1},
      $patron->{address2},
      $patron->{city},
      $patron->{state},
      $patron->{zip},
      $patron->{phone_number},
      $patron->{expiry_date},
    ), "\n";
  $out_libauth++;
}
#  # date format is YYYY-MM-DD
#  $fmt_expire_date = substr($expire_date,0,4) . '-' . substr($expire_date,4,2) . '-' . substr($expire_date,6,2);
#  print OUT "$uniqname\t$lastname\t$firstname\t$dept\t$patcat\t$campus\t$umid\t$email\t$address[0]\t$address[1]\t$city\t$state\t$zip\t$telephone\t$fmt_expire_date\n";

sub usage {
  my $msg = shift;
  $msg and $msg = " ($msg)";
  return "usage: $prgname -i inbase -d indir -o outfile $msg\n";
}

sub write_illiad {
  my $patron = shift;
  my $out_fh = shift;

  local $Class::Date::DATE_FORMAT="%m/%d/%Y";

  # require valid umid
  $patron->{umid} or do {
    print STDERR "$patron->{print_id}: ILLIAD: no umid\n";
    $illiad_no_umid++;
    return 0;
  };

  # check user group
  $patron->{user_group} =~ /(01|02|03|04|14|05)/ or do {
    #print STDERR "$patron->{primary_id}: ILLIAD: patron skipped for user group $patron->{user_group}\n";
    $illiad_user_group_skipped++;
    return 0;
  };
  # check statistic category (bor_type)
  grep /^$patron->{statistic_category}$/, @illiad_bor_type_exclude and do {
    #print STDERR "$patron->{primary_id}: ILLIAD: patron skipped for statistic category $patron->{statistic_category}\n";
    $illiad_statistic_category_skipped++;
    return 0;
  };

  # reformat the job_description to the old format used with Aleph:
  # Alma (current) job_description:  Library Info Tech - AIM (470430)
  # Desired format:  470430 - Library Info Tech - AIM
  #my ($dept_code) = $patron->{job_description} =~ /\((\d+)\)/;
  my $job_desc = $patron->{job_description};
  my $department = $job_desc;
  $job_desc and $job_desc ne 'UM (UNKNWN)' and do {
    if ($job_desc =~ /^(.*?) \((\d+)\)$/) {
      $department = join(' - ', $2, $1);
    } else {
      $patron->{user_group} =~ /(03|04)/ or print STDERR "$patron->{primary_id}: ILLIAD: can't parse job desc '$job_desc'\n";
      $department = $job_desc;
    }
  };

  my $cleared = '';
  my $illiad_status_desc = get_illiad_status_desc($patron);

  #print $out_fh join("\t", 
  my @fields = (
    $patron->{primary_id},	# 1
    val($patron->{last_name}, 40),	# 2
    val($patron->{first_name}, 40),	# 3
    $patron->{umid},	 	# 4
    join('', $patron->{user_group}, $patron->{statistic_category}),	# 5
    $patron->{email_address},	# 6
    $patron->{phone_number},	# 7
    $department,			# 8
    "ILL",			# 9
    "",                 # Password      10
    "",                 # NotificationMethod    11
    "",                 # DelMeth               12
    "",                 # LoanDeliveryMethod    13
    "",                 # LastChanged           14
    "",                 # Authorized Users      15
    "",                 # Staff                 16
    $cleared,           # Cleared (Y-no block or B-block)               17
    "",                 # Web           18
    val($patron->{address1},40),	# address1 19
    val($patron->{address2},40),	# address2 20
    $patron->{city},	# City 21
    val($patron->{state},2), 	# State 22
    val($patron->{zip},10),	# Zip 23
    "",                 # Site          24
    $patron->{expiry_date},   # ExpirationDate (mm/dd/yyyy)   25
    "",                 # Special       26
    $patron->{barcode},           # Number        27
    "",                 # UserRequestLimit              28
    "",                 # Organization  29
    "",                 # Fax           30
    "",                 # ShippingAcctNo                31
    "",                 # BillingCategory               32
    "",                 # Country               33
    "",                 # PasswordHint          34
    $illiad_status_desc,        # status description            35
    $patron->{sms_phone_number}, # SMS number            36
    );
  $csv->print($out_fh, \@fields);
  $out_illiad++;
  #print join("\t", @fields), "\n";
}

sub val {
  my $field = shift;
  my $length = shift;
  
  length($field) > $length and return substr($field, 0, $length);
  return $field;
}

sub get_illiad_status_desc {
  my $patron = shift;
  my $illiad_status_desc = $patron->{user_group_desc};
  my $campus_code = $patron->{campus_code};
  $patron->{statistic_category} =~ /(VS)/ and do {
    $illiad_status_desc = $patron->{statistic_category_desc};
  };
  return join("-", $illiad_status_desc, $campus_code);
}

sub usage {
  my $msg = shift;
  $msg and $msg = " ($msg)";
  return "usage: $prgname -i inbase -d indir -o outfile $msg\n";
}
