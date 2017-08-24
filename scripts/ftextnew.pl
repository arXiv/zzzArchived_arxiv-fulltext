#!/usr/bin/perl

#/users/e-prints/arXivLib/lib/arXiv/Converter/ExtractText.pm

if (-z $ARGV[0]) {
    print "not doing zero size file $ARGV[0]\n";
    exit;
}


open( IN,'<:encoding(UTF-8)',$ARGV[0]) || die "can't: $!";
open(OUT,'>:encoding(UTF-8)',$ARGV[1]) || die "can't: $!";

#open (IN,"<$ARGV[0]") || die "can't: $!";
#open(OUT,">$ARGV[1]") || die "can't: $!";

      my $look_for_stamp=1;
      my $stamp='';
      while (<IN>) {
          if ($look_for_stamp) {       
            # horizontal stamp: part of first line, date marks end
            if ($.==1 and s/^\s*arXiv:.{20,60}\s\d{1,2}\s[A-Z][a-z]{2}\s\d{4}//) {
	      $look_for_stamp=0;
            # vertical stamp: Discard up to 60 single char lines at start which are likely the arXiv stamp
            } elsif ($.<60 and length($_)<3) {
              chomp;
              $stamp.=$_;
              $look_for_stamp=($stamp!~/:viXra$/);
              $_=''; #discard
            } else {
              $look_for_stamp=0;
	    }
          }
  	  print OUT utf8_dumbdown($_);
      }

close OUT;
close IN;

print "$ARGV[1]\n";


sub utf8_dumbdown {
#  my $_ = @_[0];
#  print;
  # Discard non-characters
  $_=~s/[\x{FFFF}|\x{FFFE}]//g;
  # Ligatures
  # http://www.utf8-chartable.de/unicode-utf8-table.pl?start=64256&number=1024
  $_=~s/\x{FB00}/ff/g;
  $_=~s/\x{FB01}/fi/g;
  $_=~s/\x{FB02}/fl/g;
  $_=~s/\x{FB03}/ffi/g;
  $_=~s/\x{FB04}/ffl/g;
  $_=~s/\x{FB05}/st/g;
  $_=~s/\x{FB06}/st/g;
  # http://en.wikipedia.org/wiki/Typographic_ligature:
  # fs fz german ss
  $_=~s/(\B)\x{00DF}/${1}ss/g;  #careful, some use this for \beta
  # AE, ae U+00C6, U+00E6
  $_=~s/\x{00C6}/AE/g;
  $_=~s/\x{00E6}/ae/g;
  # OE, oe U+0152, U+0153
  $_=~s/\x{0152}/OE/g;
  $_=~s/\x{0153}/oe/g;
  # IJ, ij U+0132, U+0133
  $_=~s/\x{0132}/IJ/g;
  $_=~s/\x{0133}/ij/g;
  # ue U+1D6B
  $_=~s/\x{1D6B}/ue/g;
  # Ditch combining diacritics http://unicode.org/charts/PDF/U0300.pdf (0x300-0x36F)
  $_=~s/[\x{0300}-\x{036F}]//g;
  # Ditch other chars that sometimes (incorrectly?) appear as combining diacritics
  $_=~s/[\x{A8}|\x{02C0}-\x{02DF}]//g;
##additions
  $_=~s/\x{a0}/ /g;   #unicode space  u'\xa0'  (not \x{0c} = ^L keep!)
  $_=~s/[\x{2018}\x{2019}]/'/g;   #quote
  $_=~s/[\x{201c}\x{201d}]/"/g;   #doublequote
  $_=~s/[\x{ad}\x{2014}]/-/g;
  $_=~s/\x{b7}/*/g;   #asterisk
##
  return($_);
}

