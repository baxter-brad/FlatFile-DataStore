use strict;
use warnings;

use Test::More 'no_plan';
use File::Path;
use URI::Escape;

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
BEGIN { use_ok('FlatFile::DataStore') };
BEGIN { use_ok('FlatFile::DataStore::Index') };

=for comment

datastores:
    records  -- this is independent from any indexing operations
    index    -- keywords, phrase, headings keywords indexes
    facets   -- facets records  (same keynums as records)
    sortkeys -- for each record (same keynums as records)
    headings -- needs its own datastore because we index the headings
                (keynums are not the same as the records)

=cut

my $records_ds;   # data records -- to be indexed
my $index_ds;     # keywords, phrase, headings keywords indexes
my $facets_ds;    # facets records (same keynums as data records)
my $sortkeys_ds;  # sortkeys for each record (same keynums as records)
my $headings_ds;  # we'll keyword-index the headings
                  # (headings keynums are not the same as records)

# available fields in the data records

my @fields = qw(
    dc_contributor
    dc_coverage_spatial
    dc_coverage_temporal
    dc_date
    dc_description
    dc_identifier
    dc_publisher
    dc_rights
    dc_source
    dc_subject
    dc_title
    dc_type
    id
    item
    upd
);

LOAD: {{

$records_ds = init_records_datastore();

my @files = glob( "../testdata/*" );
my @recs;
for my $file ( @files ) {
    local $/;  # slurp
    open my $fh, '<', $file or die "Can't open  $file: $!";
    my $contents = <$fh>;
    close $fh               or die "Can't close $file: $!";
    my $hash = eval $contents; die $@ if $@;
    my $item = $hash->{'item'}[0];
    my $rec  = $records_ds->create({ user => $item, data => $contents });
}

for my $i ( 0 .. $#recs ) {
    my $rec      = $records_ds->retrieve( $i );
    my $userdata = $rec->user;
    my $recdata  = $rec->data;
    my $hash     = eval $recdata; die $@ if $@;
    my $item     = $hash->{'item'}[0];
    is( $userdata, $item,           "userdata/item: $item" );
    is( $userdata, $recs[$i]->user, "userdata/user: $item" );
    is( $recdata,  $recs[$i]->data, "record data:   $item" );
}

}}

INDEX: {{

my %norms = (
    rm_punct_kw => \&FlatFile::DataStore::Index::rm_punct_kw,
    rm_punct_ph => \&FlatFile::DataStore::Index::rm_punct_ph,
);

my $specs = {

    kw_norm => $norms{'rm_punct_kw'},
    ph_norm => $norms{'rm_punct_ph'},

    kw => {  # hash of index tags
        au  => [ { tag => '01', field => 'dc_contributor'       }, ],  # array of fields
        dd  => [ { tag => '02', field => 'dc_date'              }, ],
        de  => [ { tag => '03', field => 'dc_description'       }, ],
        dt  => [ { tag => '04', field => 'dc_coverage_temporal' }, ],
        ge  => [ { tag => '05', field => 'dc_coverage_spatial'  }, ],
        id  => [ { tag => '06', field => 'id'                   }, ],
        it  => [ { tag => '07', field => 'item'                 }, ],
        pu  => [ { tag => '08', field => 'dc_publisher'         }, ],
        ri  => [ { tag => '09', field => 'dc_rights'            }, ],
        so  => [ { tag => '10', field => 'dc_source'            }, ],
        su  => [ { tag => '11', field => 'dc_subject'           }, ],
        ti  => [ { tag => '12', field => 'dc_title', norm => $norms{'rm_punct_kw'}, }, ],
        ty  => [ { tag => '13', field => 'dc_type'              }, ],
        up  => [ { tag => '14', field => 'upd'                  }, ],
        ur  => [ { tag => '15', field => 'dc_identifier'        }, ],
    },

    ph => {
        aup => [ { field => 'dc_contributor'       }, ],
        ddp => [ { field => 'dc_date'              }, ],
        dep => [ { field => 'dc_description'       }, ],
        dtp => [ { field => 'dc_coverage_temporal' }, ],
        gep => [ { field => 'dc_coverage_spatial'  }, ],
        idp => [ { field => 'id'                   }, ],
        itp => [ { field => 'item'                 }, ],
        pup => [ { field => 'dc_publisher'         }, ],
        rip => [ { field => 'dc_rights'            }, ],
        sop => [ { field => 'dc_source'            }, ],
        sup => [ { field => 'dc_subject'           }, ],
        tip => [ { field => 'dc_title', norm => $norms{'rm_punct_ph'}, }, ],
        typ => [ { field => 'dc_type'              }, ],
        upp => [ { field => 'upd'                  }, ],
        urp => [ { field => 'dc_identifier'        }, ],
    },

};

my $index = init_index();

print "\nIndexing ...\n";

my $count;

for my $keynum ( 0 .. $records_ds->lastkeynum ) {
    my $dsrec = $records_ds->retrieve( $keynum );
    my $oprec = eval $dsrec->data; die $@ if $@;

print "$keynum ";

    $index->index_rec( $specs, $keynum, $oprec );

# last if $count++ > 3;

}

print "\n";

}}

#---------------------------------------------------------------------
sub init_records_datastore {
    my $name = 'records';
    my $desc = 'Example Records FlatFile::DataStore';

    my $ds = FlatFile::DataStore->new(
        { dir  => $dir,
          name => $name,
          uri  => join( ';' =>
              "http://example.com?name=$name",
              'desc=' . uri_escape($desc),
              'defaults=small',
              'user=6-%20-%7E',
              'recsep=%0A',
              ),
        } );
}

#---------------------------------------------------------------------
sub init_index {

    my $name = 'index';
    my $desc = 'Example Index FlatFile::DataStore';

    my $uri  = join( ';' =>
        qq'http://example.com?name=$name',
        qq'desc='.uri_escape($desc),
        qw(
            recsep=%0A
            defaults=small_nohist
            user=20-%20-%7E
        )
    );

    my $index = FlatFile::DataStore::Index->new({
        name   => $name,
        dir    => $dir,
        uri    => $uri,
        config => {
            encoding  => 'utf-8',
            eplen     => 1,
            eglen     => 8,
            kw => {
                tags => {
                    au => { label => 'author',      },
                    dt => { label => 'date',        },
                    dd => { label => 'dc:date',     },
                    de => { label => 'description', },
                    ge => { label => 'location',    },
                    su => { label => 'subject',     },
                    ti => { label => 'title',       },
                    ur => { label => 'url',         },
                    pu => { label => 'publisher',   },
                    ri => { label => 'rights',      },
                    so => { label => 'source',      },
                    ty => { label => 'type',        },
                    id => { label => 'id',          },
                    it => { label => 'item',        },
                    up => { label => 'updated',     },
                },
            },
            ph => { eglen => 20,
                tags => {
                    aup => { label => 'author phrase',      },
                    ddp => { label => 'dc:date phrase', eplen => 4, eglen => 10, },
                    dep => { label => 'description phrase', },
                    dtp => { label => 'date phrase',    eplen => 4, eglen => 10, },
                    gep => { label => 'location phrase',    },
                    idp => { label => 'id phrase',          },
                    itp => { label => 'item phrase',    eglen => 6, },
                    pup => { label => 'publisher phrase',   },
                    rip => { label => 'rights phrase',      },
                    sop => { label => 'source phrase',      },
                    sup => { label => 'subject phrase',     },
                    tip => { label => 'title phrase',       },
                    typ => { label => 'type phrase',        },
                    upp => { label => 'updated phrase', eplen => 4, eglen => 10, },
                    urp => { label => 'url phrase',         },
                },
            },
        },
    });
}

#---------------------------------------------------------------------

sub init_facets_datastore {
    my $name = 'facets';
    my $desc = 'Example Facets FlatFile::DataStore';

    my $ds = FlatFile::DataStore->new(
        { dir  => $dir,
          name => $name,
          uri  => join( ';' =>
              "http://example.com?name=$name",
              'desc=' . uri_escape($desc),
              'defaults=small',
              'user=6-%20-%7E',
              'recsep=%0A',
              ),
        } );
}

#---------------------------------------------------------------------

sub init_sortkeys_datastore {
    my $name = 'sortkeys';
    my $desc = 'Example  Sortkeys FlatFile::DataStore';

    my $ds = FlatFile::DataStore->new(
        { dir  => $dir,
          name => $name,
          uri  => join( ';' =>
              "http://example.com?name=$name",
              'desc=' . uri_escape($desc),
              'defaults=small',
              'user=6-%20-%7E',
              'recsep=%0A',
              ),
        } );
}

#---------------------------------------------------------------------

sub init_headings_datastore {
    my $name = 'headings';
    my $desc = 'Example Headings FlatFile::DataStore';

    my $ds = FlatFile::DataStore->new(
        { dir  => $dir,
          name => $name,
          uri  => join( ';' =>
              "http://example.com?name=$name",
              'desc=' . uri_escape($desc),
              'defaults=small',
              'user=6-%20-%7E',
              'recsep=%0A',
              ),
        } );
}

__END__

