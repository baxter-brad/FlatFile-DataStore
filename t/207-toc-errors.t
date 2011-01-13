use strict;
use warnings;

use Test::More 'no_plan';
use File::Path;
use URI::Escape;
use Data::Dumper;
$Data::Dumper::Terse    = 1;
$Data::Dumper::Indent   = 0;
$Data::Dumper::Sortkeys = 1;

#---------------------------------------------------------------------
# tempfiles cleanup

sub delete_tempfiles {
    my( $dir ) = @_;
    return unless $dir;

    for( glob "$dir/*" ) {
        if( -d $_ ) { rmtree( $_ ) }
        else        { unlink $_ or die "Can't delete $_: $!" }
    }
}

my $dir;
BEGIN {
    $dir  = "./tempdir";
    unless( -e $dir ) {
        mkdir $dir or die "Can't mkdir $dir: $!";
    }
}
NOW:  { delete_tempfiles( $dir ) }
END   { delete_tempfiles( $dir ) }

#---------------------------------------------------------------------
BEGIN { use_ok('FlatFile::DataStore') };

my $name   = "example";
my $desc   = "Example FlatFile::DataStore";
my $ok_uri = join( ';' =>
    qq'http://example.com?name=$name',
    qq'desc='.uri_escape($desc),
    qw(
        recsep=%0A
        indicator=1-%2B%23%3D%2A%2D
        transind=1-%2B%23%3D%2A%2D
        date=8-yyyymmdd
        transnum=2-10
        keynum=2-10
        reclen=2-10
        thisfnum=1-10 thisseek=4-10
        prevfnum=1-10 prevseek=4-10
        nextfnum=1-10 nextseek=4-10
        user=10-%20-%7E
    )
);

#---------------------------------------------------------------------
{  # new() with insufficient parms

    eval {
        my $toc =  FlatFile::DataStore::Preamble->new({ dummy => 1 });
    };
    like( $@, qr/Missing: datastore/, "new() with insufficient parms" );
}

__END__

