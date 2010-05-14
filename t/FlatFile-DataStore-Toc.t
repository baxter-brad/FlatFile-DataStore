use strict;
use warnings;

use Test::More 'no_plan';

BEGIN { use_ok('FlatFile::DataStore::Toc') };

# datastore set up
use FlatFile::DataStore;

sub delete_tempfiles {
    my( $dir ) = @_;
    for( glob "$dir/*" ) {
        unless( -d $_ ) {
            unlink $_ or die "Can't delete $_: $!";
        }
    }
}

my $dir  = "./tempdir";
NOW:{ delete_tempfiles( $dir ); }
END { delete_tempfiles( $dir ); }

my $name = "example";
my $desc = "Example+FlatFile::DataStore";
my $uri  = join ";",
    qq'http://example.com?name=$name',
    qq'desc=$desc',
    qw(
        datamax=9_000
        recsep=%0A
        indicator=1-%2B%23%3D%2A%2D
        date=8-yyyymmdd
        transnum=2-36
        keynum=2-36
        reclen=2-36
        thisfnum=1-36
        thisseek=4-36
        prevfnum=1-36
        prevseek=4-36
        nextfnum=1-36
        nextseek=4-36
        user=10-%20-%7E
    );

my $urifile = "$dir/$name.uri";
open my $fh, '>', $urifile or die qq/Can't open $urifile: $!/;
print $fh $uri;
close $fh;

my $datastore_obj = FlatFile::DataStore->new( {
    dir  => $dir,
    name => $name,
} );

ok( $datastore_obj );

{ # pod

 use FlatFile::DataStore::Toc;
 my $toc;

 $toc = FlatFile::DataStore::Toc->new( { int => 10,
     datastore => $datastore_obj } );

    is( $toc->datafnum, 10, "datafnum()" );

 # or

 $toc = FlatFile::DataStore::Toc->new( { num => "A",
     datastore => $datastore_obj } );

    is( $toc->datafnum, 10, "datafnum()" );

}

{ # accessors

    my $toc = FlatFile::DataStore::Toc->new( { int => 0,
        datastore => $datastore_obj } );

    is( $toc->datafnum, 0,  "datafnum()" );
    is( $toc->tocfnum,  1,  "tocfnum()"  );
    is( $toc->keyfnum,  0,  "keyfnum()"  );
    is( $toc->keynum,   -1, "keynum()"   );
    is( $toc->transnum, 0,  "transnum()" );
    is( $toc->create,   0,  "create()"   );
    is( $toc->oldupd,   0,  "oldupd()"   );
    is( $toc->update,   0,  "update()"   );
    is( $toc->olddel,   0,  "olddel()"   );
    is( $toc->delete,   0,  "delete()"   );

}


