use strict;
use warnings;

use Test::More 'no_plan';
use File::Temp qw/ tempfile tempdir /;
use Data::Dumper;
$Data::Dumper::Terse    = 1;
$Data::Dumper::Indent   = 0;
$Data::Dumper::Sortkeys = 1;

BEGIN { use_ok('FlatFile::DataStore') };

my $dir  = tempdir( CLEANUP => 1 );
my $name = "example";
my $desc = "Example+FlatFile::DataStore";

ACCESSORS: {

    my $uri = join ";",
        qq'http://example.com?name=$name',
        qq'desc=$desc',
        qw(
            maxfilesize=10_000
            recsep=%0A
            indicator=1-%2B%23%3D%2A%2D
            date=8-yyyymmdd
            transnum=2-10
            keynum=2-10
            reclen=2-10
            thisfilenum=1-10
            thisseekpos=4-10
            prevfilenum=1-10
            prevseekpos=4-10
            nextfilenum=1-10
            nextseekpos=4-10
            user=10-%20-%7E
        );

    my $urifile = "$dir/$name.uri";
    open my $fh, '>', $urifile or die qq/Can't open $urifile: $!/;
    print $fh $uri;
    close $fh;

    my $ds = FlatFile::DataStore->new( {
        dir  => $dir,
        name => $name,
    } );

    ok( $ds );

    is( $ds->dir,           $dir,         "dir()"         );
    is( $ds->name,          $name,        "name()"        );
    is( $ds->desc,          $desc,        "desc()"        );
    is( $ds->maxfilesize,   10_000,       "maxfilesize()" );
    is( $ds->recsep,        "\x0A",       "recseip()"     );
    is( $ds->indicator,     "1-+#=*-",    "indicator()"   );
    is( $ds->date,          "8-yyyymmdd", "date()"        );
    is( $ds->transnum,      "2-10",       "transnum()"    );
    is( $ds->keynum,        "2-10",       "keynum()"      );
    is( $ds->reclen,        "2-10",       "reclen()"      );
    is( $ds->thisfilenum,   "1-10",       "thisfilenum()" );
    is( $ds->thisseekpos,   "4-10",       "thisseekpos()" );
    is( $ds->prevfilenum,   "1-10",       "prevfilenum()" );
    is( $ds->prevseekpos,   "4-10",       "prevseekpos()" );
    is( $ds->nextfilenum,   "1-10",       "nextfilenum()" );
    is( $ds->nextseekpos,   "4-10",       "nextseekpos()" );
    is( $ds->user,          "10- -~",     "user()"        );
    is( $ds->dateformat,    "yyyymmdd",   "dateformat()"  );
    is( $ds->filenumbase,   "10",         "filenumbase()" );
    is( $ds->filenumlen,    1,            "filenumlen()"  ); # XXX 0+
    is( $ds->uri,           $uri,         "uri()"         );
    is( $ds->preamblelen,   40,           "preamblelen()" );
    is( $ds->translen,      2,            "translen()"    ); # XXX 0+
    is( $ds->transbase,     "10",         "transbase()"   );
    is( Dumper($ds->crud),
        "{'create' => '+','delete' => '-','olddel' => '*','oldupd' => '#','update' => '='}",
        "crud()" );
    is( Dumper($ds->regx),
        "qr/(?-xism:([+#=*-])([0-9]{8})([-0-9]{2})([-0-9]{2})([-0-9]{2})([-0-9])([-0-9]{4})([-0-9])([-0-9]{4})([-0-9])([-0-9]{4})([ -~]{10}))/",
        "regx()" );
    is( Dumper($ds->specs),
        "{'indicator' => [0,1,'+#=*-']}{'date' => [1,8,'yyyymmdd']}{'transnum' => [9,2,'10']}{'keynum' => [11,2,'10']}{'reclen' => [13,2,'10']}{'thisfilenum' => [15,1,'10']}{'thisseekpos' => [16,4,'10']}{'prevfilenum' => [20,1,'10']}{'prevseekpos' => [21,4,'10']}{'nextfilenum' => [25,1,'10']}{'nextseekpos' => [26,4,'10']}{'user' => [30,10,' -~']}",
        "specs()" );
}

CRUD: {
    my $ds = FlatFile::DataStore->new( {
        dir  => $dir,
        name => $name,
    } );

    ok( $ds );

    my $user_data   = "Testing1";
    #                  ----+----1----+----2---- (reclen:24)
    my $record_data = "This is testing record1.";

    my $record = $ds->create( $record_data, $user_data );
exit;
    is( Dumper($record),
        "xxx",
        "create()" );

}

