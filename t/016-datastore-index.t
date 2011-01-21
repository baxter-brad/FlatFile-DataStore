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
#END   { delete_tempfiles( $dir ) }

#---------------------------------------------------------------------
BEGIN { use_ok('FlatFile::DataStore::Index') };

my $name = "example";
my $desc = "Example FlatFile::DataStore";
my $uri  = join( ';' =>
    qq'http://example.com?name=$name',
    qq'desc='.uri_escape($desc),
    qw(
        recsep=%0A
        defaults=medium
        user=1-%20-%7E
    )
);

{

    my $index = FlatFile::DataStore::Index->new({
        name   => $name,
        dir    => $dir,
        uri    => $uri,
        config => {
            enc  => 'utf-8',
            tags => {
                ti => {
                    label => 'title',
                    eplen => 1,
                    eglen => 5,
                },
                au => {
                    label => 'author',
                },
                su => {
                    label => 'subject',
                },
            },
            eplen => 1,
            eglen => 8,
        },
    });

    for(
        [qw( ti willie   title 1 1 42 )],
        [qw( ti the      title 1 2 42 )],
        [qw( ti elephant title 1 3 42 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;
            
        $index->add_kw({
            tag   => $tag,
            kw    => $kw,
            field => $field,
            occ   => $occ,
            pos   => $pos,
            num   => $num,
            });
    }

    for(
        [qw( ti willie   title 1 1 256 )],
        [qw( ti the      title 1 2 999 )],
        [qw( ti elephant title 1 3 0 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;
            
        $index->add_kw({
            tag   => $tag,
            kw    => $kw,
            field => $field,
            occ   => $occ,
            pos   => $pos,
            num   => $num,
            });
    }

    for(
        [qw( ti war   title 12 13 1 )],
        [qw( ti and   title 13 22 1 )],
        [qw( ti peace title 14 31 1 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;
            
        $index->add_kw({
            tag   => $tag,
            kw    => $kw,
            field => $field,
            occ   => $occ,
            pos   => $pos,
            num   => $num,
            });
    }

    for(
        [qw( ti war      title 22 1  10  )],
        [qw( ti and      title 23 2  10  )],
        [qw( ti peace    title 24 3  10  )],
        [qw( ti willie   title 2  1  156 )],
        [qw( ti the      title 2  2  199 )],
        [qw( ti elephant title 2  3  44  )],
        [qw( ti elephants title 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;
            
        $index->add_kw({
            tag   => $tag,
            kw    => $kw,
            field => $field,
            occ   => $occ,
            pos   => $pos,
            num   => $num,
            });
    }

}

__END__

