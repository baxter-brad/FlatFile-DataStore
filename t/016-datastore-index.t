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
    my $title;
    my $debug;

    my $index = FlatFile::DataStore::Index->new({
        name   => $name,
        dir    => $dir,
        uri    => $uri,
        config => {
            encoding  => 'utf-8',
            kw => {
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
                    dt => {
                        label => 'date',
                    },
                },
                eplen => 1,
                eglen => 8,
            },
            ph => {
                tags => {
                    tp => {
                        label => 'title phrase',
                    },
                    ap => {
                        label => 'author phrase',
                    },
                    sp => {
                        label => 'subject phrase',
                    },
                    dp => {
                        label => 'date phrase',
                        eplen => 4,
                        eglen => 10,
                    },
                },
                eplen => 1,
                eglen => 25,
            },
            eplen => 1,
            eglen => 8,
        },
    });

    $title = "Adding ti willie the elephant";
    for(
        [qw( ti willie   title 1 1 42 )],
        [qw( ti the      title 1 2 42 )],
        [qw( ti elephant title 1 3 42 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->add_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding ti willie the elephant
ti         |        |      3= |      e t w |
ti e       |        |      1+ |            | ti e eleph |
ti e eleph |      3 |      1  |            |   ti t the |
ti t       |        |      1+ | ti e eleph |   ti t the |
ti t the   |      2 |      1  | ti e eleph | ti w willi |
ti w       |        |      1+ |   ti t the | ti w willi |
ti w willi |      1 |      1  |   ti t the |            |

_end_

    $title = "Adding ti willie the elephant";
    for(
        [qw( ti willie   title 1 1 256 )],
        [qw( ti the      title 1 2 999 )],
        [qw( ti elephant title 1 3 0 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->add_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

    # note: same as above, because only the bits change

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding ti willie the elephant
ti         |        |      3= |      e t w |
ti e       |        |      1+ |            | ti e eleph |
ti e eleph |      3 |      1  |            |   ti t the |
ti t       |        |      1+ | ti e eleph |   ti t the |
ti t the   |      2 |      1  | ti e eleph | ti w willi |
ti w       |        |      1+ |   ti t the | ti w willi |
ti w willi |      1 |      1  |   ti t the |            |

_end_


    $title = "Adding ti war and peace";
    for(
        [qw( ti war   title 12 13 1 )],
        [qw( ti and   title 13 22 1 )],
        [qw( ti peace title 14 31 1 )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->add_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding ti war and peace
ti         |        |      6= |  a e p t w |
ti a       |        |      1+ |            |   ti a and |
ti a and   |      5 |      1  |            | ti e eleph |
ti e       |        |      1+ |   ti a and | ti e eleph |
ti e eleph |      3 |      1  |   ti a and | ti p peace |
ti p       |        |      1+ | ti e eleph | ti p peace |
ti p peace |      6 |      1  | ti e eleph |   ti t the |
ti t       |        |      1+ | ti p peace |   ti t the |
ti t the   |      2 |      1  | ti p peace |   ti w war |
ti w       |        |      2+ |   ti t the |   ti w war |
ti w war   |      4 |      1  |   ti t the | ti w willi |
ti w willi |      1 |      1  |   ti w war |            |

_end_


    $title = "Adding ti war and peace, ti willie the elephant, ti elephants";
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
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding ti war and peace, ti willie the elephant, ti elephants
ti         |        |     13= |  a e p t w |
ti a       |        |      2+ |            |   ti a and |
ti a and   |      5 |      2  |            | ti e eleph |
ti e       |        |      3+ |   ti a and | ti e eleph |
ti e eleph |      3 |      3  |   ti a and | ti p peace |
ti p       |        |      2+ | ti e eleph | ti p peace |
ti p peace |      6 |      2  | ti e eleph |   ti t the |
ti t       |        |      2+ | ti p peace |   ti t the |
ti t the   |      2 |      2  | ti p peace |   ti w war |
ti w       |        |      4+ |   ti t the |   ti w war |
ti w war   |      4 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |            |

_end_

    $title = "Adding ti www";
    for(
        [qw( ti    www         title   3  1  11 )],
        # [   'ti', "\x{263a}", 'title', 3, 1, 11  ],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->add_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding ti www
ti         |        |     14= |  a e p t w |
ti a       |        |      2+ |            |   ti a and |
ti a and   |      5 |      2  |            | ti e eleph |
ti e       |        |      3+ |   ti a and | ti e eleph |
ti e eleph |      3 |      3  |   ti a and | ti p peace |
ti p       |        |      2+ | ti e eleph | ti p peace |
ti p peace |      6 |      2  | ti e eleph |   ti t the |
ti t       |        |      2+ | ti p peace |   ti t the |
ti t the   |      2 |      2  | ti p peace |   ti w war |
ti w       |        |      5+ |   ti t the |   ti w war |
ti w war   |      4 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |      7 |      1  | ti w willi |            |

_end_


    $title = "Adding su war and peace, su willie the elephant, su elephants";
    for(
        [qw( su war      subject 22 1  10  )],
        [qw( su and      subject 23 2  10  )],
        [qw( su peace    subject 24 3  10  )],
        [qw( su willie   subject 2  1  156 )],
        [qw( su the      subject 2  2  199 )],
        [qw( su elephant subject 2  3  44  )],
        [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->add_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding su war and peace, su willie the elephant, su elephants
su            |        |      7= |     a e p t w |
su a          |        |      1+ |               |      su a and |
su a and      |      9 |      1  |               | su e elephant |
su e          |        |      2+ |      su a and | su e elephant |
su e elephant |     13 |      2  |      su a and |    su p peace |
su p          |        |      1+ | su e elephant |    su p peace |
su p peace    |     10 |      1  | su e elephant |      su t the |
su t          |        |      1+ |    su p peace |      su t the |
su t the      |     12 |      1  |    su p peace |      su w war |
su w          |        |      2+ |      su t the |      su w war |
su w war      |      8 |      1  |      su t the |   su w willie |
su w willie   |     11 |      1  |      su w war |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_


    $title = "Deleting su war";
    for(
        [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su war
su            |        |      6= |     a e p t w |
su a          |        |      1+ |               |      su a and |
su a and      |      9 |      1  |               | su e elephant |
su e          |        |      2+ |      su a and | su e elephant |
su e elephant |     13 |      2  |      su a and |    su p peace |
su p          |        |      1+ | su e elephant |    su p peace |
su p peace    |     10 |      1  | su e elephant |      su t the |
su t          |        |      1+ |    su p peace |      su t the |
su t the      |     12 |      1  |    su p peace |   su w willie |
su w          |        |      1+ |      su t the |   su w willie |
su w willie   |     11 |      1  |      su t the |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su and";
    for(
        # [qw( su war      subject 22 1  10  )],
        [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su and
su            |        |      5= |       e p t w |
su e          |        |      2+ |               | su e elephant |
su e elephant |     13 |      2  |               |    su p peace |
su p          |        |      1+ | su e elephant |    su p peace |
su p peace    |     10 |      1  | su e elephant |      su t the |
su t          |        |      1+ |    su p peace |      su t the |
su t the      |     12 |      1  |    su p peace |   su w willie |
su w          |        |      1+ |      su t the |   su w willie |
su w willie   |     11 |      1  |      su t the |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_


    $title = "Deleting su peace";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su peace
su            |        |      4= |         e t w |
su e          |        |      2+ |               | su e elephant |
su e elephant |     13 |      2  |               |      su t the |
su t          |        |      1+ | su e elephant |      su t the |
su t the      |     12 |      1  | su e elephant |   su w willie |
su w          |        |      1+ |      su t the |   su w willie |
su w willie   |     11 |      1  |      su t the |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_


    $title = "Deleting su willie";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su willie
su            |        |      3= |           e t |
su e          |        |      2+ |               | su e elephant |
su e elephant |     13 |      2  |               |      su t the |
su t          |        |      1+ | su e elephant |      su t the |
su t the      |     12 |      1  | su e elephant |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su the";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su the
su            |        |      2= |             e |
su e          |        |      2+ |               | su e elephant |
su e elephant |     13 |      2  |               |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su elephant";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        [qw( su elephant subject 2  3  44  )],
        # [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su elephant
su            |        |      1= |             e |
su e          |        |      1+ |               | su e elephant |
su e elephant |     13 |      1  |               |               |
ti            |        |     14= |     a e p t w |
ti a          |        |      2+ |               |      ti a and |
ti a and      |      5 |      2  |               |    ti e eleph |
ti e          |        |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      3 |      3  |      ti a and |    ti p peace |
ti p          |        |      2+ |    ti e eleph |    ti p peace |
ti p peace    |      6 |      2  |    ti e eleph |      ti t the |
ti t          |        |      2+ |    ti p peace |      ti t the |
ti t the      |      2 |      2  |    ti p peace |      ti w war |
ti w          |        |      5+ |      ti t the |      ti w war |
ti w war      |      4 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |      7 |      1  |    ti w willi |               |

_end_


    $title = "Deleting su elephants";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  156 )],
        # [qw( su the      subject 2  2  199 )],
        # [qw( su elephant subject 2  3  44  )],
        [qw( su elephants subject 1  1 44  )],
    ) {
        my( $tag, $kw, $field, $occ, $pos, $num ) = @$_;

        $index->delete_kw({
            tag     => $tag,
            keyword => $kw,
            field   => $field,
            occ     => $occ,
            pos     => $pos,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Deleting su elephants
ti         |        |     14= |  a e p t w |
ti a       |        |      2+ |            |   ti a and |
ti a and   |      5 |      2  |            | ti e eleph |
ti e       |        |      3+ |   ti a and | ti e eleph |
ti e eleph |      3 |      3  |   ti a and | ti p peace |
ti p       |        |      2+ | ti e eleph | ti p peace |
ti p peace |      6 |      2  | ti e eleph |   ti t the |
ti t       |        |      2+ | ti p peace |   ti t the |
ti t the   |      2 |      2  | ti p peace |   ti w war |
ti w       |        |      5+ |   ti t the |   ti w war |
ti w war   |      4 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |      7 |      1  | ti w willi |            |

_end_

}

__END__

