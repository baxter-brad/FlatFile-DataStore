use strict;
use warnings;

use Test::More 'no_plan';
use File::Path;
use URI::Escape;
# don't use Data::Dumper;

use Data::Bvec qw( :all );

# not intended to dump perl code ...
sub dump_kw_group {
    my( $kw_group ) = @_;
    my @ret;
    push @ret, "[\n";
    for( @$kw_group ) {
        push @ret, "    [";
            push @ret, join ',' => $_->[0], $_->[1], $_->[2];
                push @ret, ",[";
                    push @ret, join ',' => $_->[3][0], $_->[3][1];
                push @ret, "]";
        push @ret, "]\n";
    }
    push @ret, "]\n";
    join '' => @ret;
}

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
        user=20-%20-%7E
    )
);

# longest eplen is 1
# longest eglen is 10
# user data might look like this: "[eg:dp 2 2011-04-26]"
#                                  ----+----1----+----2
# so that's why "user=20-..." above
# a production index might let user be a single blank,
# but the above info if helpful for testing

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
*          |      2 |
ti         |      3 |      3= |      e t w |
ti e       |      8 |      1+ |            | ti e eleph |
ti e eleph |      7 |      1  |            |   ti t the |
ti t       |      6 |      1+ | ti e eleph |   ti t the |
ti t the   |      5 |      1  | ti e eleph | ti w willi |
ti w       |      4 |      1+ |   ti t the | ti w willi |
ti w willi |      1 |      1  |   ti t the |            |

_end_

    $title = "Adding ti willie the elephant";
    for(
        [qw( ti willie   title 1 1 314 )],
        [qw( ti the      title 1 2 314 )],
        [qw( ti elephant title 1 3 314 )],
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
*          |      2 |
ti         |      3 |      3= |      e t w |
ti e       |      8 |      1+ |            | ti e eleph |
ti e eleph |      7 |      1  |            |   ti t the |
ti t       |      6 |      1+ | ti e eleph |   ti t the |
ti t the   |      5 |      1  | ti e eleph | ti w willi |
ti w       |      4 |      1+ |   ti t the | ti w willi |
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
*          |      2 |
ti         |      3 |      6= |  a e p t w |
ti a       |     11 |      1+ |            |   ti a and |
ti a and   |     10 |      1  |            | ti e eleph |
ti e       |      8 |      1+ |   ti a and | ti e eleph |
ti e eleph |      7 |      1  |   ti a and | ti p peace |
ti p       |     13 |      1+ | ti e eleph | ti p peace |
ti p peace |     12 |      1  | ti e eleph |   ti t the |
ti t       |      6 |      1+ | ti p peace |   ti t the |
ti t the   |      5 |      1  | ti p peace |   ti w war |
ti w       |      4 |      2+ |   ti t the |   ti w war |
ti w war   |      9 |      1  |   ti t the | ti w willi |
ti w willi |      1 |      1  |   ti w war |            |

_end_


    $title = "Adding ti war and peace, ti willie the elephant, ti elephants";
    for(
        [qw( ti war       title 22 1 10 )],
        [qw( ti and       title 23 2 10 )],
        [qw( ti peace     title 24 3 10 )],
        [qw( ti willie    title 2  1 50 )],
        [qw( ti the       title 2  2 50 )],
        [qw( ti elephant  title 2  3 50 )],
        [qw( ti elephants title 1  1 44 )],
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
*          |      2 |
ti         |      3 |     13= |  a e p t w |
ti a       |     11 |      2+ |            |   ti a and |
ti a and   |     10 |      2  |            | ti e eleph |
ti e       |      8 |      3+ |   ti a and | ti e eleph |
ti e eleph |      7 |      3  |   ti a and | ti p peace |
ti p       |     13 |      2+ | ti e eleph | ti p peace |
ti p peace |     12 |      2  | ti e eleph |   ti t the |
ti t       |      6 |      2+ | ti p peace |   ti t the |
ti t the   |      5 |      2  | ti p peace |   ti w war |
ti w       |      4 |      4+ |   ti t the |   ti w war |
ti w war   |      9 |      2  |   ti t the | ti w willi |
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
*          |      2 |
ti         |      3 |     14= |  a e p t w |
ti a       |     11 |      2+ |            |   ti a and |
ti a and   |     10 |      2  |            | ti e eleph |
ti e       |      8 |      3+ |   ti a and | ti e eleph |
ti e eleph |      7 |      3  |   ti a and | ti p peace |
ti p       |     13 |      2+ | ti e eleph | ti p peace |
ti p peace |     12 |      2  | ti e eleph |   ti t the |
ti t       |      6 |      2+ | ti p peace |   ti t the |
ti t the   |      5 |      2  | ti p peace |   ti w war |
ti w       |      4 |      5+ |   ti t the |   ti w war |
ti w war   |      9 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |     14 |      1  | ti w willi |            |

_end_

    $title = "Adding su war and peace, su willie the elephant, su elephants";
    for(
        [qw( su war      subject 22 1  10  )],
        [qw( su and      subject 23 2  10  )],
        [qw( su peace    subject 24 3  10  )],
        [qw( su willie   subject 2  1  314 )],
        [qw( su the      subject 2  2  314 )],
        [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      7= |     a e p t w |
su a          |     19 |      1+ |               |      su a and |
su a and      |     18 |      1  |               | su e elephant |
su e          |     26 |      2+ |      su a and | su e elephant |
su e elephant |     25 |      2  |      su a and |    su p peace |
su p          |     21 |      1+ | su e elephant |    su p peace |
su p peace    |     20 |      1  | su e elephant |      su t the |
su t          |     24 |      1+ |    su p peace |      su t the |
su t the      |     23 |      1  |    su p peace |      su w war |
su w          |     17 |      2+ |      su t the |      su w war |
su w war      |     15 |      1  |      su t the |   su w willie |
su w willie   |     22 |      1  |      su w war |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su war";
    for(
        [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      6= |     a e p t w |
su a          |     19 |      1+ |               |      su a and |
su a and      |     18 |      1  |               | su e elephant |
su e          |     26 |      2+ |      su a and | su e elephant |
su e elephant |     25 |      2  |      su a and |    su p peace |
su p          |     21 |      1+ | su e elephant |    su p peace |
su p peace    |     20 |      1  | su e elephant |      su t the |
su t          |     24 |      1+ |    su p peace |      su t the |
su t the      |     23 |      1  |    su p peace |   su w willie |
su w          |     17 |      1+ |      su t the |   su w willie |
su w willie   |     22 |      1  |      su t the |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su and";
    for(
        # [qw( su war      subject 22 1  10  )],
        [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      5= |       e p t w |
su e          |     26 |      2+ |               | su e elephant |
su e elephant |     25 |      2  |               |    su p peace |
su p          |     21 |      1+ | su e elephant |    su p peace |
su p peace    |     20 |      1  | su e elephant |      su t the |
su t          |     24 |      1+ |    su p peace |      su t the |
su t the      |     23 |      1  |    su p peace |   su w willie |
su w          |     17 |      1+ |      su t the |   su w willie |
su w willie   |     22 |      1  |      su t the |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su peace";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      4= |         e t w |
su e          |     26 |      2+ |               | su e elephant |
su e elephant |     25 |      2  |               |      su t the |
su t          |     24 |      1+ | su e elephant |      su t the |
su t the      |     23 |      1  | su e elephant |   su w willie |
su w          |     17 |      1+ |      su t the |   su w willie |
su w willie   |     22 |      1  |      su t the |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su willie";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      3= |           e t |
su e          |     26 |      2+ |               | su e elephant |
su e elephant |     25 |      2  |               |      su t the |
su t          |     24 |      1+ | su e elephant |      su t the |
su t the      |     23 |      1  | su e elephant |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su the";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      2= |             e |
su e          |     26 |      2+ |               | su e elephant |
su e elephant |     25 |      2  |               |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su elephant";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        [qw( su elephant subject 2  3  314 )],
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
*             |      2 |
su            |     16 |      1= |             e |
su e          |     26 |      1+ |               | su e elephant |
su e elephant |     25 |      1  |               |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |

_end_

    $title = "Deleting su elephants";
    for(
        # [qw( su war      subject 22 1  10  )],
        # [qw( su and      subject 23 2  10  )],
        # [qw( su peace    subject 24 3  10  )],
        # [qw( su willie   subject 2  1  314 )],
        # [qw( su the      subject 2  2  314 )],
        # [qw( su elephant subject 2  3  314 )],
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
*          |      2 |
ti         |      3 |     14= |  a e p t w |
ti a       |     11 |      2+ |            |   ti a and |
ti a and   |     10 |      2  |            | ti e eleph |
ti e       |      8 |      3+ |   ti a and | ti e eleph |
ti e eleph |      7 |      3  |   ti a and | ti p peace |
ti p       |     13 |      2+ | ti e eleph | ti p peace |
ti p peace |     12 |      2  | ti e eleph |   ti t the |
ti t       |      6 |      2+ | ti p peace |   ti t the |
ti t the   |      5 |      2  | ti p peace |   ti w war |
ti w       |      4 |      5+ |   ti t the |   ti w war |
ti w war   |      9 |      2  |   ti t the | ti w willi |
ti w willi |      1 |      2  |   ti w war |   ti w www |
ti w www   |     14 |      1  | ti w willi |            |

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
*             |      2 |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |
tp            |     28 |      1= |             w |
tp w          |     29 |      1+ |               | tp w willie t |
tp w willie t |     27 |      1  |               |               |

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
*             |      2 |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |
tp            |     28 |      2= |             w |
tp w          |     29 |      2+ |               | tp w war and  |
tp w war and  |     30 |      1  |               | tp w willie t |
tp w willie t |     27 |      1  | tp w war and  |               |

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
*             |      2 |
sp            |     32 |      3= |           l w |
sp l          |     35 |      1+ |               | sp l little h |
sp l little h |     34 |      1  |               | sp w willie t |
sp w          |     33 |      2+ | sp l little h | sp w willie t |
sp w willie t |     36 |      1  | sp l little h |      sp w www |
sp w www      |     31 |      1  | sp w willie t |               |
ti            |      3 |     14= |     a e p t w |
ti a          |     11 |      2+ |               |      ti a and |
ti a and      |     10 |      2  |               |    ti e eleph |
ti e          |      8 |      3+ |      ti a and |    ti e eleph |
ti e eleph    |      7 |      3  |      ti a and |    ti p peace |
ti p          |     13 |      2+ |    ti e eleph |    ti p peace |
ti p peace    |     12 |      2  |    ti e eleph |      ti t the |
ti t          |      6 |      2+ |    ti p peace |      ti t the |
ti t the      |      5 |      2  |    ti p peace |      ti w war |
ti w          |      4 |      5+ |      ti t the |      ti w war |
ti w war      |      9 |      2  |      ti t the |    ti w willi |
ti w willi    |      1 |      2  |      ti w war |      ti w www |
ti w www      |     14 |      1  |    ti w willi |               |
tp            |     28 |      2= |             w |
tp w          |     29 |      2+ |               | tp w war and  |
tp w war and  |     30 |      1  |               | tp w willie t |
tp w willie t |     27 |      1  | tp w war and  |               |

_end_

    my $b2n = sub { join ' ' => bit2num str2bit uncompress $_[0] };

    my $bitstring;
    $bitstring = $index->get_ph_bitstring ({
        tag    => 'tp',
        phrase => 'war and peace',
        });
    is( $b2n->($bitstring), '10', 'get_ph_bistring tp: war and peace' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'www',
        });
    is( $b2n->($bitstring), '30', 'get_ph_bistring sp: www' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'little house on the prairie',
        });
    is( $b2n->($bitstring), '20', 'get_ph_bistring sp: little house on the prairie' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'willie the elephant',
        });
    is( $b2n->($bitstring), '40', 'get_ph_bistring sp: willie the elephant' );

    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => '*',
        });
    is( $b2n->($bitstring), '20 30 40', 'get_ph_bistring sp: *' );


    $bitstring = $index->get_ph_bitstring ({
        tag    => 'sp',
        phrase => 'w*',
        });
    is( $b2n->($bitstring), '30 40', 'get_ph_bistring sp: w*' );

    my $kw_group;

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => 'willie',
        });

    # tried to get "[['title',1,1,[1,0]],['title',2,1,[1,1]]]",
    # by saying 0+$2 and $2*1, but DD wouldn't cooperate

    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: willie' );
[
    [title,1,1,[1,0]]
    [title,2,1,[1,1]]
]
_end_

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => 'elephant',
        });
    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: elephant' );
[
    [title,1,3,[7,0]]
    [title,2,3,[7,1]]
]
_end_

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => 'elephants',
        });
    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: elephants' );
[
    [title,1,1,[7,2]]
]
_end_

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => 'elephant*',
        });
    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: elephant*' );
[
    [title,1,3,[7,0]]
    [title,2,3,[7,1]]
    [title,1,1,[7,2]]
]
_end_

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => 'e*',
        });
    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: e*' );
[
    [title,1,3,[7,0]]
    [title,2,3,[7,1]]
    [title,1,1,[7,2]]
]
_end_

    $kw_group = $index->get_kw_group ({
        tag     => 'ti',
        keyword => '*',
        });
    is( dump_kw_group($kw_group), <<'_end_', 'get_kw_group ti: *' );
[
    [title,13,22,[10,0]]
    [title,23,2,[10,1]]
    [title,1,3,[7,0]]
    [title,2,3,[7,1]]
    [title,1,1,[7,2]]
    [title,14,31,[12,0]]
    [title,24,3,[12,1]]
    [title,1,2,[5,0]]
    [title,2,2,[5,1]]
    [title,12,13,[9,0]]
    [title,22,1,[9,1]]
    [title,1,1,[1,0]]
    [title,2,1,[1,1]]
    [title,3,1,[14,0]]
]
_end_

}

__END__

