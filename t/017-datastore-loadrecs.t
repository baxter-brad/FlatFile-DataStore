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

=for comment

parms:

keynum -- integer, data record sequence number
oprec  -- hash ref, hash of arrays (hash is fields, array is occurrences)
specs  -- indexes, fields, normalization routines

specs:

index tags:                 e.g., ti, au, su, tip, aup, sup, etc.
fields         (per tag):   e.g., dc_title, dc_contributor, dc_subject, etc.
normalizations (per field): e.g., &rm_punct_kw, &rm_punct_ph,

norm parms:

field     -- string
callback  -- coderef
keynum    -- integer, which bit to affect
new_oprec -- href
old_oprec -- href

returns todo list  -- href of deletes and adds;

=cut

my $index = init_index();

package FlatFile::DataStore::Index;

my $specs = {

    kw_norm => \&rm_punct_kw,
    ph_norm => \&rm_punct_ph,

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
        ti  => [ { tag => '12', field => 'dc_title', norm => \&rm_punct_kw, }, ],
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
        tip => [ { field => 'dc_title', norm => \&rm_punct_ph, }, ],
        typ => [ { field => 'dc_type'              }, ],
        upp => [ { field => 'upd'                  }, ],
        urp => [ { field => 'dc_identifier'        }, ],
    },

};

print "\nIndexing ...\n";
for my $keynum ( 0 .. $records_ds->lastkeynum ) {
    my $dsrec = $records_ds->retrieve( $keynum );
    my $oprec = eval $dsrec->data; die $@ if $@;

print "$keynum ";

    $index->index_rec( $specs, $keynum, $oprec );
}
print "\n";

#---------------------------------------------------------------------
sub rm_punct_kw {
    my( $val ) = @_;

    for( $val ) {
        $_ = lc;
        s{\p{IsPunct}+}{ }g;
        s{^\s+}        {};
        s{\s+$}        {};
        s{\s\s+}       { }g;
    }

    return if $val eq '';
    split m{\s} => $val;  # returned
}

#---------------------------------------------------------------------
sub rm_punct_ph {
    my( $val ) = @_;

    for( $val ) {
        $_ = lc;
        s{\p{IsPunct}+}{ }g;
        s{^\s+}        {};
        s{\s+$}        {};
        s{\s\s+}       { }g;
    }

    return if $val eq '';
    $val;  # returned
}

#---------------------------------------------------------------------
sub kw_norm {
    my( $field, $callback, $keynum, $new_oprec, $old_oprec ) = @_;

    my %todo;  # to return
    my $todo = sub {
        my( $action, $value, $occ ) = @_;
        if( my @keywords = $callback->( $value ) ) {
            for my $j ( 0 .. $#keywords ) {
                my $pos = $j + 1;
                push @{$todo{ $action }}, [ $keywords[$j], $field, $occ, $pos, $keynum ];
            }
        }
    };

    my $new_vals = $new_oprec->{ $field };  # aref
    my $old_vals = $old_oprec->{ $field };  # aref

    # cases:
    #           4.      3.       2.       1.
    # new_vals  undef   true     undef    true 
    # old_vals  undef   undef    true     true
    # action    return  add_new  del_old  compare

    if( $new_vals and $old_vals ) {  # 1. compare

        my $max = $#$new_vals > $#$old_vals? $#$new_vals: $#$old_vals;

        for my $i ( 0 .. $max ) {

            my $occ     = $i + 1;
            my $new_val = $new_vals->[$i];
            my $old_val = $old_vals->[$i];

            # cases:
            #          1.4    1.3      1.2      1.1
            # new_val  undef  defined  undef    defined
            # old_val  undef  undef    defined  defined
            # action   skip   add new  del old  add & del

            if( defined $new_val and defined $old_val ) {  # 1.1 add & delete
                
                # cases:
                #
                # new_val eq old_val ... skip
                # new_val ne old_val ... delete old_val & add new_val

                if( $new_val ne $old_val ) {  # delete old & add new

                    $todo->( del => $old_val, $occ );
                    $todo->( add => $new_val, $occ );
                }
                else {  # skip
                }
            }
            elsif( defined $old_val ) {  # 1.2 delete old
                $todo->( del => $old_val, $occ );
            }
            elsif( defined $new_val ) {  # 1.3 add new
                $todo->( add => $new_val, $occ );
            }
            else {  # 1.4 skip
                # XXX both are undefined ... error?
            }
        }
    }
    elsif( $old_vals ) {  # 2. delete old
        for my $i ( 0 .. $#$old_vals ) {
            my $occ = $i + 1;
            $todo->( del => $old_vals->[$i], $occ );
        }
    }
    elsif( $new_vals ) {  # 3. add new
        for my $i ( 0 .. $#$new_vals ) {
            my $occ = $i + 1;
            $todo->( add => $new_vals->[$i], $occ );
        }
    }
    else {  # 4. return
        return;
    }

    \%todo;  # returned
}

#---------------------------------------------------------------------
sub ph_norm {
    my( $field, $callback, $keynum, $new_oprec, $old_oprec ) = @_;

    my %todo;  # to return
    my $todo = sub {
        my( $action, $value ) = @_;
        my $phrase = $callback->( $value );
        if( defined $phrase and $phrase ne '' ) {
            push @{$todo{ $action }}, [ $phrase, $keynum ];
        }
    };

    my $new_vals = $new_oprec->{ $field };  # aref
    my $old_vals = $old_oprec->{ $field };  # aref

    # cases:
    #           4.      3.       2.       1.
    # new_vals  undef   true     undef    true 
    # old_vals  undef   undef    true     true
    # action    return  add_new  del_old  compare

    if( $new_vals and $old_vals ) {  # 1. compare

        my $max = $#$new_vals > $#$old_vals? $#$new_vals: $#$old_vals;
        for my $i ( 0 .. $max ) {

            my $new_val = $new_vals->[$i];
            my $old_val = $old_vals->[$i];

            # cases:
            #          1.4    1.3      1.2      1.1
            # new_val  undef  defined  undef    defined
            # old_val  undef  undef    defined  defined
            # action   skip   add new  del old  add & del

            if( defined $new_val and defined $old_val ) {  # 1.1 add & delete
                
                # cases:
                #
                # new_val eq old_val ... skip
                # new_val ne old_val ... delete old_val & add new_val

                if( $new_val ne $old_val ) {  # delete old & add new
                    $todo->( del => $old_val );
                    $todo->( add => $new_val );
                }
                else {  # skip
                    # values are equal, no need to do anything
                }
            }
            elsif( defined $old_val ) {  # 1.2 delete old
                $todo->( del => $old_val );
            }
            elsif( defined $new_val ) {  # 1.3 add new
                $todo->( add => $new_val );
            }
            else {  # 1.4 skip
                # XXX both are undefined ... error?
            }
        }
    }
    elsif( $old_vals ) {  # 2. delete old
        $todo->( del => $_ ) for @$old_vals;
    }
    elsif( $new_vals ) {  # 3. add new
        $todo->( add => $_ ) for @$new_vals;
    }
    else {  # 4. return
        return;
    }

    \%todo;  # returned
}

sub index_rec {
    my( $self, $specs, $keynum, $new_oprec, $old_oprec ) = @_;

    # kw
    my $default_norm = $specs->{'kw_norm'};
    my $tspecs = $specs->{'kw'};
    for my $index ( sort keys %$tspecs ) {
        my $ispecs = $tspecs->{ $index };
        for my $fspecs ( @$ispecs ) {
            my $field = $fspecs->{'field'};
            my $ftag  = $fspecs->{'tag'};
            my $norm  = $fspecs->{'norm' }||$default_norm;
            my $todo = kw_norm( $field, $norm, $keynum, $new_oprec, $old_oprec );
            if( my $deletions = $todo->{'del'} ) {
                for my $del ( @$deletions ) {
                    my( $keyword, $field, $occ, $pos, $num ) = @$del;
                    croak qq/Missing keyword:$num:$field/ unless defined $keyword and $keyword ne '';
                    $self->delete_kw({
                        tag     => $index,
                        keyword => $keyword,
                        field   => $ftag,
                        occ     => $occ,
                        pos     => $pos,
                        num     => $num,
                        });
                }
            }
            if( my $additions = $todo->{'add'} ) {
                for my $add ( @$additions ) {
                    my( $keyword, $field, $occ, $pos, $num ) = @$add;
                    croak qq/Missing keyword:$num:$field/.Dumper($todo) unless defined $keyword and $keyword ne '';
                    $self->add_kw({
                        tag     => $index,
                        keyword => $keyword,
                        field   => $ftag,
                        occ     => $occ,
                        pos     => $pos,
                        num     => $num,
                        });
                }
            }
        }
    }

    # ph
    $default_norm = $specs->{'ph_norm'};
    $tspecs = $specs->{'ph'};
    for my $index ( sort keys %$tspecs ) {
        my $ispecs = $tspecs->{ $index };
        for my $fspecs ( @$ispecs ) {
            my $field = $fspecs->{'field'};
            my $norm  = $fspecs->{'norm' }||$default_norm;
            my $todo = ph_norm( $field, $norm, $keynum, $new_oprec, $old_oprec );
            if( my $deletions = $todo->{'del'} ) {
                for my $del ( @$deletions ) {
                    my( $phrase, $num ) = @$del;
                    croak qq/Missing phrase:$num:$field/ unless defined $phrase and $phrase ne '';
                    $self->delete_ph({
                        tag    => $index,
                        phrase => $phrase,
                        num    => $num,
                        });
                }
            }
            if( my $additions = $todo->{'add'} ) {
                for my $add ( @$additions ) {
                    my( $phrase, $num ) = @$add;
                    croak qq/Missing phrase:$num:$field/ unless defined $phrase and $phrase ne '';
                    $self->add_ph({
                        tag    => $index,
                        phrase => $phrase,
                        num    => $num,
                        });
                }
            }
        }
    }

}
package main;

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

