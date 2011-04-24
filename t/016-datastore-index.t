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
                eglen => 8,
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
ti         |      2 |      3= |      e t w |
ti e       |      7 |      1+ |            | ti e eleph |
ti e eleph |      6 |      1  |            |   ti t the |
ti t       |      5 |      1+ | ti e eleph |   ti t the |
ti t the   |      4 |      1  | ti e eleph | ti w willi |
ti w       |      3 |      1+ |   ti t the | ti w willi |
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
ti         |      2 |      3= |      e t w |
ti e       |      7 |      1+ |            | ti e eleph |
ti e eleph |      6 |      1  |            |   ti t the |
ti t       |      5 |      1+ | ti e eleph |   ti t the |
ti t the   |      4 |      1  | ti e eleph | ti w willi |
ti w       |      3 |      1+ |   ti t the | ti w willi |
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
ti         |      2 |      6= |  a e p t w |
ti a       |     10 |      1+ |            |   ti a and |
ti a and   |      9 |      1  |            | ti e eleph |
ti e       |      7 |      1+ |   ti a and | ti e eleph |
ti e eleph |      6 |      1  |   ti a and | ti p peace |
ti p       |     12 |      1+ | ti e eleph | ti p peace |
ti p peace |     11 |      1  | ti e eleph |   ti t the |
ti t       |      5 |      1+ | ti p peace |   ti t the |
ti t the   |      4 |      1  | ti p peace |   ti w war |
ti w       |      3 |      2+ |   ti t the |   ti w war |
ti w war   |      8 |      1  |   ti t the | ti w willi |
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
ti         |      2 |     13= |  a e p t w |
ti a       |     10 |      2+ |            |   ti a and |
ti a and   |      9 |      2  |            | ti e eleph |
ti e       |      7 |      3+ |   ti a and | ti e eleph |
ti e eleph |      6 |      3  |   ti a and | ti p peace |
ti p       |     12 |      2+ | ti e eleph | ti p peace |
ti p peace |     11 |      2  | ti e eleph |   ti t the |
ti t       |      5 |      2+ | ti p peace |   ti t the |
ti t the   |      4 |      2  | ti p peace |   ti w war |
ti w       |      3 |      4+ |   ti t the |   ti w war |
ti w war   |      8 |      2  |   ti t the | ti w willi |
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
ti         |      2 |     14= |  a e p t w |
ti a       |     10 |      2+ |            |   ti a and |
ti a and   |      9 |      2  |            | ti e eleph |
ti e       |      7 |      3+ |   ti a and | ti e eleph |
ti e eleph |      6 |      3  |   ti a and | ti p peace |
ti p       |     12 |      2+ | ti e eleph | ti p peace |
ti p peace |     11 |      2  | ti e eleph |   ti t the |
ti t       |      5 |      2+ | ti p peace |   ti t the |
ti t the   |      4 |      2  | ti p peace |   ti w war |
ti w       |      3 |      5+ |   ti t the |   ti w war |
ti w war   |      8 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |     13 |      1  | ti w willi |            |

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
su            |     15 |      7= |     a e p t w |
su a          |     18 |      1+ |               |      su a and |
su a and      |     17 |      1  |               | su e elephant |
su e          |     25 |      2+ |      su a and | su e elephant |
su e elephant |     24 |      2  |      su a and |    su p peace |
su p          |     20 |      1+ | su e elephant |    su p peace |
su p peace    |     19 |      1  | su e elephant |      su t the |
su t          |     23 |      1+ |    su p peace |      su t the |
su t the      |     22 |      1  |    su p peace |      su w war |
su w          |     16 |      2+ |      su t the |      su w war |
su w war      |     14 |      1  |      su t the |   su w willie |
su w willie   |     21 |      1  |      su w war |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      6= |     a e p t w |
su a          |     18 |      1+ |               |      su a and |
su a and      |     17 |      1  |               | su e elephant |
su e          |     25 |      2+ |      su a and | su e elephant |
su e elephant |     24 |      2  |      su a and |    su p peace |
su p          |     20 |      1+ | su e elephant |    su p peace |
su p peace    |     19 |      1  | su e elephant |      su t the |
su t          |     23 |      1+ |    su p peace |      su t the |
su t the      |     22 |      1  |    su p peace |   su w willie |
su w          |     16 |      1+ |      su t the |   su w willie |
su w willie   |     21 |      1  |      su t the |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      5= |       e p t w |
su e          |     25 |      2+ |               | su e elephant |
su e elephant |     24 |      2  |               |    su p peace |
su p          |     20 |      1+ | su e elephant |    su p peace |
su p peace    |     19 |      1  | su e elephant |      su t the |
su t          |     23 |      1+ |    su p peace |      su t the |
su t the      |     22 |      1  |    su p peace |   su w willie |
su w          |     16 |      1+ |      su t the |   su w willie |
su w willie   |     21 |      1  |      su t the |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      4= |         e t w |
su e          |     25 |      2+ |               | su e elephant |
su e elephant |     24 |      2  |               |      su t the |
su t          |     23 |      1+ | su e elephant |      su t the |
su t the      |     22 |      1  | su e elephant |   su w willie |
su w          |     16 |      1+ |      su t the |   su w willie |
su w willie   |     21 |      1  |      su t the |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      3= |           e t |
su e          |     25 |      2+ |               | su e elephant |
su e elephant |     24 |      2  |               |      su t the |
su t          |     23 |      1+ | su e elephant |      su t the |
su t the      |     22 |      1  | su e elephant |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      2= |             e |
su e          |     25 |      2+ |               | su e elephant |
su e elephant |     24 |      2  |               |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
su            |     15 |      1= |             e |
su e          |     25 |      1+ |               | su e elephant |
su e elephant |     24 |      1  |               |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |

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
ti         |      2 |     14= |  a e p t w |
ti a       |     10 |      2+ |            |   ti a and |
ti a and   |      9 |      2  |            | ti e eleph |
ti e       |      7 |      3+ |   ti a and | ti e eleph |
ti e eleph |      6 |      3  |   ti a and | ti p peace |
ti p       |     12 |      2+ | ti e eleph | ti p peace |
ti p peace |     11 |      2  | ti e eleph |   ti t the |
ti t       |      5 |      2+ | ti p peace |   ti t the |
ti t the   |      4 |      2  | ti p peace |   ti w war |
ti w       |      3 |      5+ |   ti t the |   ti w war |
ti w war   |      8 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |     13 |      1  | ti w willi |            |

_end_

    $title = "Adding tp willie the elephant";
    for(
        [ "tp", "willie the elephant", 42 ],
    ) {
        my( $tag, $ph, $num ) = @$_;

        $index->add_ph({
            tag     => $tag,
            phrase  => $ph,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding tp willie the elephant
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |
tp            |     27 |      1= |             w |
tp w          |     28 |      1+ |               | tp w willie t |
tp w willie t |     26 |      1  |               |               |

_end_

    $title = "Adding tp war and peace";
    for(
        [ "tp", "war and peace", 10 ],
    ) {
        my( $tag, $ph, $num ) = @$_;

        $index->add_ph({
            tag     => $tag,
            phrase  => $ph,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding tp war and peace
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |
tp            |     27 |      2= |             w |
tp w          |     28 |      2+ |               | tp w war and  |
tp w war and  |     29 |      1  |               | tp w willie t |
tp w willie t |     26 |      1  | tp w war and  |               |

_end_

    $title = "Adding sp www; sp little house on the prairie";
    for(
        [ "sp", "www", 30 ],
        [ "sp", "little house on the prairie", 20 ],
        [ "sp", "willie the elephant", 40 ],
    ) {
        my( $tag, $ph, $num ) = @$_;

        $index->add_ph({
            tag     => $tag,
            phrase  => $ph,
            num     => $num,
            });
    }

        $debug = $index->debug_kv( $title );
    is( $debug, <<_end_, $title );

Adding sp www; sp little house on the prairie
sp            |     31 |      3= |           l w |
sp l          |     34 |      1+ |               | sp l little h |
sp l little h |     33 |      1  |               | sp w willie t |
sp w          |     32 |      2+ | sp l little h | sp w willie t |
sp w willie t |     35 |      1  | sp l little h |      sp w www |
sp w www      |     30 |      1  | sp w willie t |               |
ti            |      2 |     14= |     a e p t w |
ti a          |     10 |      2+ |               |      ti a and |
ti a and      |      9 |      2  |               |    ti e eleph |
ti e          |      7 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      6 |      3  |      ti a and |    ti p peace |
ti p          |     12 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     11 |      2  |    ti e eleph |      ti t the |
ti t          |      5 |      2+ |    ti p peace |      ti t the |
ti t the      |      4 |      2  |    ti p peace |      ti w war |
ti w          |      3 |      5+ |      ti t the |      ti w war |
ti w war      |      8 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     13 |      1  |    ti w willi |               |
tp            |     27 |      2= |             w |
tp w          |     28 |      2+ |               | tp w war and  |
tp w war and  |     29 |      1  |               | tp w willie t |
tp w willie t |     26 |      1  | tp w war and  |               |

_end_

    my $bitstring;
    $bitstring = $index->get_ph_bitstring ({
        tag    => 'tp',
        phrase => 'war and peace',
        });

    is( $bitstring, '-A15', 'get_ph_bistring tp: war and peace' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'www',
        });

    is( $bitstring, '-U11', 'get_ph_bistring sp: www' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'little house on the prairie',
        });

    is( $bitstring, '-K13', 'get_ph_bistring sp: little house on the prairie' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'willie the elephant',
        });

    is( $bitstring, '-e17', 'get_ph_bistring sp: willie the elephant' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'w*',
        });

    is( $bitstring, '-U1917', 'get_ph_bistring sp: w*' );

}

__END__

