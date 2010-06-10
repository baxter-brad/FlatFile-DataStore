use strict;
use warnings;

use Test::More 'no_plan';
use File::Path;

#---------------------------------------------------------------------
# tempfiles cleanup

sub delete_tempfiles {
    my( $dir ) = @_;
    for( glob "$dir/*" ) {
        if( -d $_ ) { rmtree( $_ ) }
        else        { unlink $_ or die "Can't delete $_: $!" }
    }
}

my $dir;
BEGIN { $dir = "./tempdir"       }
NOW:  { delete_tempfiles( $dir ) }
END   { delete_tempfiles( $dir ) }

#---------------------------------------------------------------------
BEGIN { use_ok('FlatFile::DataStore::Preamble') };

use FlatFile::DataStore;

my $name = "example";
my $desc = "Example FlatFile::DataStore";
my $recsep = "\x0A";
my $ds = FlatFile::DataStore->new(
    { dir  => $dir,
      name => $name,
      uri  => join( ";" =>
          "http://example.com?name=$name",
          "desc=$desc",
          "defaults=xsmall",
          "user=1-:",
          "recsep=$recsep" ),
    } );

ok( $ds, "FlatFile::DataStore->new()" );

{ # pod

my $indicator = '+';
my $transind  = '+';
my $date      = 'WQ6A';
my $transint  = 1;
my $keynum    = 0;
my $reclen    = 100;
my $fnum      = 1;
my $datapos   = 0;
my $prevfnum;  # not defined okay for create
my $prevseek;
my $nextfnum;
my $nextseek;
my $user_data = ':';


 use FlatFile::DataStore::Preamble;

 my $preamble = FlatFile::DataStore::Preamble->new( {
     datastore => $ds,         # FlatFile::DataStore object
     indicator => $indicator,  # single-character crud flag
     transind  => $transind,   # single-character crud flag
     date      => $date,       # pre-formatted date
     transnum  => $transint,   # transaction number (integer)
     keynum    => $keynum,     # record sequence number (integer)
     reclen    => $reclen,     # record length (integer)
     thisfnum  => $fnum,       # file number (in base format)
     thisseek  => $datapos,    # seek position (integer)
     prevfnum  => $prevfnum,   # ditto these ...
     prevseek  => $prevseek,
     nextfnum  => $nextfnum,
     nextseek  => $nextseek,
     user      => $user_data,  # pre-formatted user-defined data
     } );

ok( $preamble, "FF::DS::Preamble->new()" );

 my $string = $preamble->string();

is( $string, '++WQ6A01001c10000----------:', "string()" );

 my $clone = FlatFile::DataStore::Preamble->new( {
     datastore => $ds,
     string    => $string
     } );

ok( $clone );
is( $clone->string(), $preamble->string(), "clone" )

}
