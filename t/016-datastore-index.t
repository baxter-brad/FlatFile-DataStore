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

}

__END__
