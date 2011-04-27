#---------------------------------------------------------------------
  package FlatFile::DataStore::Index;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Index - Perl module that implements a flatfile
datastore index.

=head1 SYNOPSYS

    use FlatFile::DataStore::Index;

    # initialize a new index

    my $index = FlatFile::DataStore::Index->new({
        name   => $name,
        dir    => $dir,
        uri    => $uri,
        config => {
            encoding => 'utf-8',             # index's character encoding
            kw => {                          # keyword index
                tags => {                    # index tags
                    ti => {                  # keys are actual tags
                        label => 'title',    # label is required
                        eplen => 1,          # will override the defaults
                        eglen => 5,          # ditto
                    },
                    au => {
                        label => 'author',
                    },
                    su => {
                        label => 'subject',
                    },
                    dt => {
                        label => 'date',
                        eplen => 1,
                        eglen => 10,
                    },
                },
                eplen => 1,  # keyword default for entry point key length
                eglen => 8,  # keyword default for entry group key length
            },
            ph => {                          # phrase index
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
                eplen => 1,   # phrase defaults ...
                eglen => 25,
            },
            eplen => 1,  # global defaults ...
            eglen => 8,
        },
    });

    # instantiate an existing index

    my $index = FlatFile::DataStore::Index->new({
        name = $name,
        dir  = $dir,
    });

    # add/delete a keyword item

    $index->add_kw({
        tag     => 'ti',
        keyword => $keyword,
        field   => $field,
        occ     => $occurrence,
        pos     => $position,
        num     => $keynum,
    });

    $index->delete_kw({
        tag     => 'ti',
        keyword => $keyword,
        field   => $field,
        occ     => $occurrence,
        pos     => $position,
        num     => $keynum,
    });

    # add/delete a phrase item

    $index->add_ph({
        tag    => 'tp',
        phrase => $phrase,
        num    => $keynum,
    });

    $index->delete_ph({
        tag    => 'ti',
        phrase => $phrase,
        num    => $keynum,
    });

    # get a bitstring group for a keyword

    my $kw_group = $index->get_kw_group({
        tag     => 'ti',
        keyword => $keyword,
    });

    # combine two groups into one group

    my $kw_group = $index->combine_kw_groups({
        group1 => $kw_group1,
        group2 => $kw_group2,
        op     => 'w1',  # "with 1" (i.e., adjacency -- the default)
    });

    # get a bitstring for a keyword, something like this
    # - kw_group  = get_kw_group(     { tag    => ..., keyword => ...            })
    # - kw_group  = combine_kw_groups({ group1 => kw_group1, group2 => kw_group2 })
    # - bitstring = get_kw_bitstring( { group  => kw_group                       })

    $bitstring = $index->get_kw_bitstring({
        group => $kw_group
    });

    # get a bitstring for a phrase

    $bitstring = $index->get_ph_bitstring
        tag    => 'tp',
        phrase => $phrase,
    });

    #-----------------------------------------------------------------
    # initial load of index data
    # (index must be empty -- one could use apply() on an empty index,
    # but initial_load() is (XXX should be) faster)

    # file name (will be opened using index's encoding)

    $index->initial_load({
        filename => $load_file,
    });

    # or specify the (input) encoding

    $index->initial_load({
        filename => $load_file,
        encoding => 'iso-8859-1',
    });

    # or file handle, already open

    $index->initial_load({
        fh => $load_fh,
    });

    # or string data (may be scalar ref)
    # will not be decoded by default

    $index->initial_load({
        string => $load_data,
    });

    # or specify an encoding if needed

    $index->initial_load({
        string => $load_data,
        encoding => 'utf-8',
    });

    #-----------------------------------------------------------------
    # apply a batch of index entries
    # (index may be empty or not)

    # file name (will be opened using index's encoding)

    $index->apply({
        filename => $load_file,
    });

    # or specify the (input) encoding
    # this will not change the index's (output) encoding

    $index->apply({
        filename => $load_file,
        encoding => 'iso-8859-1',
    });

    # or file handle, already open

    $index->apply({
        fh => $load_fh,
    });

    # or string data (may be scalar ref)
    # will not be decoded by default

    $index->apply({
        string => $load_data,
    });

    # or specify an (input) encoding if needed
    # this will not change the index's (output) encoding

    $index->apply({
        string => $load_data,
        encoding => 'utf-8',
    });

=head1 DESCRIPTION

FlatFile::DataStore::Index

=head1 VERSION

FlatFile::DataStore::Index version 1.03

=cut

our $VERSION = '1.03';

use 5.008003;
use strict;
use warnings;

use Encode;
use Fcntl qw(:DEFAULT :flock);
use Data::Dumper;

use Carp;

use FlatFile::DataStore;
use Data::Bvec qw( :all );

#---------------------------------------------------------------------
# globals

our $dbm_package  = "SDBM_File";
our $dbm_parms    = [ O_CREAT|O_RDWR, 0666 ];
our $dbm_lock_ext = ".dir";

my $Trunc = '*?'    ;  # truncation characters
my $Sp    = ' '     ;  # one-space separator
my $Sep   = $Sp x 2 ;  # two-space separator

my $default_eplen = 1;
my $default_eglen = 8;

#---------------------------------------------------------------------

=head1 Class Methods

=cut

#---------------------------------------------------------------------
# accessors
# the following are required attributes, so simple accessors are okay
# XXX are they required?
#
# Private methods.
#

sub config        {for($_[0]->{config       }){$_=$_[1]if@_>1;return$_}}
sub encoding      {for($_[0]->{encoding     }){$_=$_[1]if@_>1;return$_}}
sub dbm           {for($_[0]->{dbm          }){$_=$_[1]if@_>1;return$_}}
sub datastore     {for($_[0]->{datastore    }){$_=$_[1]if@_>1;return$_}}
sub dbm_lock_file {for($_[0]->{dbm_lock_file}){$_=$_[1]if@_>1;return$_}}
sub locked        {for($_[0]->{locked       }){$_=$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------

=head2 new()

Create a new index object.
Returns a reference to the FlatFile::DataStore::Index object.

The parms C<name> and C<dir> are always required.

If the index hasn't been initialized yet, C<uri> and C<config> are
also required.

=cut

sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self->init( $parms ) if $parms;

    return $self;
}

#---------------------------------------------------------------------
# init()

sub init {
    my ( $self, $parms ) = @_;

    eval qq{require $dbm_package; 1} or croak qq/Can't use $dbm_package: $@/;

    my $dir    = $parms->{'dir' };  # always required
    my $name   = $parms->{'name'};  # always required
    my $uri    = $parms->{'uri' };
    my $config = $parms->{'config'};

    # uri is required to initialize a new datastore

    my $ds_parms = { name => $name, dir => $dir };
       $ds_parms->{'uri'} = $uri if $uri;

    my $ds = FlatFile::DataStore->new( $ds_parms );
    my $nl = $ds->recsep;  # "newline" for datastore

    # config is required to configure a new index

    # 0th record is the config record
    # XXX please use ascii only in config?
    my     $config_rec;
    eval { $config_rec = $ds->retrieve( 0 ) };
    if( $@ ) {
        croak $@ unless $@ =~ /Record doesn't exist/;
    }

    # here is where we configure the index
    if( $config ) {
        croak qq/Index already configured, not allowed: config/ if $config_rec;

        # config a one-liner
        local $Data::Dumper::Quotekeys = 0;
        local $Data::Dumper::Pair      = '=>';
        local $Data::Dumper::Useqq     = 1;
        local $Data::Dumper::Terse  = 1;
        local $Data::Dumper::Indent = 0;

        $config_rec = $ds->create({
            data => Dumper( $config ),
            user => gen_userdata( '[', 'config:', ']', $ds->userlen ),
            });
    }

    # here is where we get the config of an existing index
    elsif( $config_rec ) {
        $config = eval ${$config_rec->data};
        croak qq/Something is wrong with index configuration: $@/ if $@;
    }

    else {
        croak qq/Index needs to be configured/;
    }

    # XXX validate config here XXX

    $self->config( $config );
    $self->encoding( $config->{'encoding'} );
    $self->datastore( $ds );
    $self->dbm_lock_file( "$dir/$name$dbm_lock_ext" );

    return $self;
}

#---------------------------------------------------------------------

=head1 Object Methods

=head2 add_kw()

add a keyword

    $index->add_kw({
        tag     => 'ti',
        keyword => $keyword,
        field   => $field,
        occ     => $occurrence,
        pos     => $position,
        num     => $keynum,
    });

=cut

sub add_kw {
    my( $self, $parms ) = @_;

    my $num = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    # returned:
    $self->add_item( $num, $self->get_kw_keys( $parms ) );
}

#---------------------------------------------------------------------

=head2 delete_kw()

delete a keyword

    $index->delete_kw({
        tag     => 'ti',
        keyword => $keyword,
        field   => $field,
        occ     => $occurrence,
        pos     => $position,
        num     => $keynum,
    });

=cut

sub delete_kw {
    my( $self, $parms ) = @_;

    my $num = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    # returned:
    $self->delete_item( $num, $self->get_kw_keys( $parms ) );
}


#---------------------------------------------------------------------

=head2 add_ph()

add a phrase

    $index->add_ph({
        tag    => 'tp',
        phrase => $phrase,
        num    => $keynum,
    });

=cut

sub add_ph {
    my( $self, $parms ) = @_;

    my $num = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    # returned:
    $self->add_item( $num, $self->get_ph_keys( $parms ) );
}

#---------------------------------------------------------------------

=head2 delete_ph()

delete a phrase

    $index->delete_ph({
        tag    => 'tp',
        phrase => $phrase,
        num    => $keynum,
    });

=cut

sub delete_ph {
    my( $self, $parms ) = @_;

    my $num = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    # returned:
    $self->delete_item( $num, $self->get_ph_keys( $parms ) );
}

#---------------------------------------------------------------------

=head2 get_kw_group()

get a bitstring group for a keyword

    my $group = $index->get_kw_group({
        tag     => 'ti',
        keyword => $keyword,
    });

=cut

sub get_kw_group {
    my( $self, $parms ) = @_;

    my $ds   = $self->datastore;
    my $dir  = $ds->dir;
    my $name = $ds->name;
    my $nl   = $ds->recsep;  # "newline" for datastore

    my $keys        = $self->get_kw_keys( $parms );
    my $entry_group = $keys->{'entry_group'};
    my $truncated   = $keys->{'truncated'};

    my $tag         = $parms->{'tag'};
    my $keyword     = $parms->{'keyword'};

    $self->readlock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};
    $self->dbm( \%dbm );

    my @matches;

    TRY: {
        my $index_key = $keys->{'index_key'};
        last TRY unless exists $dbm{ $index_key };

        if( $truncated ) {

            # we can't use the bitstrings in ik/ep for truncation
            # because we need field/occ/pos for adjacency

            # the ik/ep bitstrings are still needed for updating
            # the all star bitstring

            my $eglen = $keys->{'eglen'};

            $keyword =~ s{[$Trunc]$}{};  # remove truncation char

            # if keyword is shorter than our entry group length
            # then we know that every index entry in every matching
            # entry group will match our keyword

            if( length $keyword <= $eglen ) {

                # locate the starting entry point(s)
                my $eplist = $self->retrieve_index_key_kv( $index_key => 'eplist' );
                my @eps;
                if( length $keyword == 0 ) {
                    @eps = split $Sp => $eplist;
                }
                else {
                    for( split $Sp => $eplist ) { push @eps, $_ if $keyword =~ m{^$_} }
                }

                for my $ep ( @eps ) {
                    # start traversing entry groups
                    my $match_key  = qr{^$index_key$Sp$ep$Sp\Q$keyword};
                    my $next_group = $self->retrieve_entry_point_kv( "$index_key$Sp$ep" => 'next' );
                    my( $eg_keynum, $found );
                    while( $next_group ) {

                        if( $next_group =~ /$match_key/ ) {
                            $found++;
                            ( $eg_keynum, $next_group ) = $self->retrieve_entry_group_kv( $next_group => 'keynum', 'next' );
                            my $eg_rec = $ds->retrieve( $eg_keynum );

                            # loop through the index entries in this group
                            # we want every index entry, because we know
                            # they all match our keyword

                            # XXX would split be better ... ?
                            my $match_space = quotemeta $Sp;
                            my $match_sep   = quotemeta $Sep;
                            my $matchrx = qr{
                                $match_space ([a-zA-Z0-9]+)  # $1 field
                                $match_space ([0-9]+)        # $2 occ
                                $match_space ([0-9]+)        # $3 pos
                                $match_sep
                                }x;

                            my $ie_offset = 0;
                            for( split $nl => $eg_rec->data ) {
                                if( /$matchrx/ ) {
                                    push @matches, [ $1, $2, $3, [ $eg_keynum, $ie_offset ] ];
                                }
                                else {
                                    croak qq/Expected a match/;
                                }
                                $ie_offset++;
                            }
                        }
                        else {
                            $next_group = $self->retrieve_entry_group_kv( $next_group => 'next' );
                            last if $found;  # short circuit the rest
                        }

                    }
                }
            }

            # else keyword is longer than our entry group length,
            # so we need to get the one matching entry group and
            # find every matching index entry in it

            else {
                my $match_space    = quotemeta $Sp;
                my $match_sep      = quotemeta $Sep;
                my $match_tag      = quotemeta $tag;
                my $match_keyword  = quotemeta $keyword;
                   $match_keyword .= "[^$match_space]*";  # truncated

                my $matchrx = qr{^
                    $match_tag     $match_space  #    tag
                    $match_keyword $match_space  #    keyword
                    ([a-zA-Z0-9]+) $match_space  # $1 field
                    ([0-9]+)       $match_space  # $2 occ
                    ([0-9]+)       $match_sep    # $3 pos
                    }x;

                my $eg_keynum         = $self->retrieve_entry_group_kv( $entry_group => 'keynum' );
                my( $fh, $pos, $len ) = $ds->locate_record_data( $eg_keynum );

                my $found;
                my $got;
                local $/ = $nl;  # $fh is opened in binmode
                my $ie_offset = 0;
                while( <$fh> ) {
                    last if ($got += length) > $len;
                    chomp;
                    if( /$matchrx/ ) {
                        $found++;
                        push @matches, [ $1, $2, $3, [ $eg_keynum, $ie_offset ] ];
                    }
                    else {
                        last if $found;  # short circuit the rest
                    }
                    last if $got == $len;
                    $ie_offset++;
                }
                close $fh;
            }

        }  # if truncated

        else {
            if( my $eg_keynum = $self->retrieve_entry_group_kv( $entry_group => 'keynum' ) ) {

                my $match_space    = quotemeta $Sp;
                my $match_sep      = quotemeta $Sep;
                my $match_tag      = quotemeta $tag;
                my $match_keyword  = quotemeta $keyword;

                my $matchrx = qr{^
                    $match_tag     $match_space  #    tag
                    $match_keyword $match_space  #    keyword
                    ([a-zA-Z0-9]+) $match_space  # $1 field
                    ([0-9]+)       $match_space  # $2 occ
                    ([0-9]+)       $match_sep    # $3 pos
                    }x;

                my( $fh, $pos, $len ) = $ds->locate_record_data( $eg_keynum );

                my $got;
                local $/ = $nl;  # $fh is opened in binmode
                my $ie_offset = 0;
                while( <$fh> ) {
                    last if ($got += length) > $len;
                    chomp;
                    if( /$matchrx/ ) {
                        push @matches, [ $1, $2, $3, [ $eg_keynum, $ie_offset ] ];
                    }
                    last if $got == $len;
                    $ie_offset++;
                }
                close $fh;
            }
        }
    }  # TRY

    $self->dbm( '' );
    untie %dbm;
    $self->unlock;

    return unless @matches;
    return       \@matches;
}

#---------------------------------------------------------------------

=head2 combine_kw_groups()

combine a keyword groups into a single group

=for comment

1. compare each member of the second group to all the members of the
   first group

2. they "match" when the field name and occurrence number are the same
   and when the position number of the second group member is one more
   than that of the first.

3. when there's a match, we push the keynum(s) from the first group
   onto the keynum list of the second group and push the resulting
   member onto the third -- result -- group

4. after all the members of both groups are compared, the resulting
   group contains new members, each of which has the position numbers
   of the second group's members

5. this resulting group is pushed back onto the solution stack, ready
   to be processed by an immediately following adjacency operation if
   there is one

operators:

'w' for 'with' (within the same field, in the given order)
'n' for 'near' (within the same field, in any order)

intervals:

'0' (any number of terms in between)
'1' (no terms in between)
'2' (one term in between)
... (etc.)

examples:

term1 w1 term2 ("term1 term2" in a field)
term1 w2 term2 ("term1 (one term) term2" in a field)
term1 w0 term2 ("term1 (any terms) term2" in a field)

term1 n1 term2 ("term1 term2" or "term2 term1" in a field)
term1 n2 term2 ("term1 (one term) term2" or
                "term2 (one term) term1" in a field)
term1 n0 term2 ("term1 (any terms) term2" or
                "term2 (any terms) term1" in a field)

interval mnemonics:

'1' -- "limit to one word away"
'2' -- "limit to two words away"
'0' -- "no limits"

=cut

sub combine_kw_groups {
    my( $self, $parms ) = @_;

    my $group1 = $parms->{'group1'};
    my $group2 = $parms->{'group2'};
    croak qq/Bad call/ unless $group1 and $group2;

    my $op = $parms->{'op'}   || 'w1';  # "with 1" -- adjacent
    my $iv = substr( $op, 1 ) ||   1 ;  # 'w' == 'w1'; 'n' == 'n1'
    croak qq/Bad call/ unless $iv >= 0;

    my $result;

    # 'with'
    if( $op =~ /^w/ ) {
        if( $iv ) {
            for my $_2 ( @$group2 ) {
                for my $_1 ( @$group1 ) {
                    if(        $_2->[0]  eq $_1->[0]       &&  # field
                               $_2->[1]  eq $_1->[1]       &&  # occ
                               $_2->[2]  eq $_1->[2] + $iv ){  # pos
                        push @{$_2->[3]}, @{$_1->[3]};         # eg/ie
                        push @$result, $_2;
                    }
                }
            }
        }
        else {
            for my $_2 ( @$group2 ) {
                for my $_1 ( @$group1 ) {
                    if(        $_2->[0]  eq $_1->[0] &&  # field
                               $_2->[1]  eq $_1->[1] &&  # occ
                               $_2->[2]  gt $_1->[2] ){  # pos
                        push @{$_2->[3]}, @{$_1->[3]};   # eg/ie
                        push @$result, $_2;
                    }
                }
            }
        }
    }
    # 'near'
    elsif( $op =~ /^n/ ) {
        if( $iv ) {
            for my $_2 ( @$group2 ) {
                for my $_1 ( @$group1 ) {
                    if(        $_2->[0]  eq $_1->[0]          &&  # field
                               $_2->[1]  eq $_1->[1]          &&  # occ
                          abs( $_2->[2]  -  $_1->[2] ) <= $iv ){  # pos
                        push @{$_2->[3]}, @{$_1->[3]};            # eg/ie
                        push @$result, $_2;
                    }
                }
            }
        }
        else {
            for my $_2 ( @$group2 ) {
                for my $_1 ( @$group1 ) {
                    if(        $_2->[0]  eq $_1->[0] &&  # field
                               $_2->[1]  eq $_1->[1] ){  # occ
                                                         # any pos
                        push @{$_2->[3]}, @{$_1->[3]};   # eg/ie
                        push @$result, $_2;
                    }
                }
            }
        }
    }
    else {
        croak qq/Bad op: $op/;
    }

    return $result;
}

#---------------------------------------------------------------------

=head2 get_kw_bitstring()

get a bitstring for a keyword group

    $bitstring = $index->get_kw_bitstring({
        group => $kw_group,
    });

=for comment

1. for each member of the result group, the bitstrings referenced by
   the array of keynums are ANDed into one bitstring

2. the resulting bitstrings for each member are then ORed into one
   bitstring

3. this final bitstring represents all the records where those terms
   may be found adjacent to each other

4. this final bitstring is pushed on to the stack for the next
   operation or to be returned

=cut

sub get_kw_bitstring {
    my( $self, $parms ) = @_;

    my $ds = $self->datastore;
    my $nl = $ds->recsep;  # "newline" for datastore

    my $group = $parms->{'group'};
    return unless $group;

    my %egs;  # cache
    my @vecs;

    for( @$group ) {
        my $vec;
        my @tmp = @{$_->[3]};

        # while( my @a = splice @tmp, 0, 2 ) {
        #     my( $eg_keynum, $ie_offset ) = @a;
        while( my( $eg_keynum, $ie_offset ) = splice @tmp, 0, 2 ) {

            my $ies = $egs{ $eg_keynum };
            unless( $ies ) {
                my $eg_rec = $ds->retrieve( $eg_keynum );
                $ies = $egs{ $eg_keynum } = [ split $nl => $eg_rec->data ];
            }
            my $ie = $ies->[ $ie_offset ];
            chomp $ie;
            my $bitstring = (split $Sp => $ie)[-1];
            my $bvec = str2bit uncompress $bitstring;
            if( $vec ) { $vec &= $bvec }
            else       { $vec  = $bvec }
        }
        push @vecs, $vec;
    }

    my $vec  = '';
       $vec |= $_ for @vecs;

    return compress bit2str $vec;
}

#---------------------------------------------------------------------

=head2 get_ph_bitstring()

get a bitstring for a phrase

    $bitstring = $index->get_ph_bitstring
        tag    => 'tp',
        phrase => $phrase,
    });

=cut

=begin comment

given an index tag and a phrase, get entry group
locate the group record
find our phrase in the group
return the count and bitstring in array context
return the bitstring in scalar context

if truncation is present, we loop through all
matches. at the end, we 'or' the bitstrings and
get a resulting count.

=cut

sub get_ph_bitstring {
    my( $self, $parms ) = @_;

    my $ds   = $self->datastore;
    my $dir  = $ds->dir;
    my $name = $ds->name;
    my $nl   = $ds->recsep;  # "newline" for datastore

    my $keys        = $self->get_ph_keys( $parms );
    my $entry_group = $keys->{'entry_group'};
    my $truncated   = $keys->{'truncated'};

    my $tag         = $parms->{'tag'};
    my $phrase      = $parms->{'phrase'};

    $self->readlock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};
    $self->dbm( \%dbm );

    my @matches;

    TRY: {
        my $index_key = $keys->{'index_key'};
        last TRY unless exists $dbm{ $index_key };

        if( $truncated ) {
            my $eplen     = $keys->{'eplen'};
            my $eglen     = $keys->{'eglen'};

            $phrase =~ s{[$Trunc]$}{};  # remove truncation char

            # if phrase was "*", i.e., "find all"

            if( length $phrase == 0 ) {

                # get the index tag bitstring
                my $ik_keynum = $self->retrieve_index_key_kv( $index_key => 'keynum' );
                my $ik_rec = $ds->retrieve( $ik_keynum );
                push @matches, [ split $Sp => $ik_rec->data ];

            }

            # else if phrase was "z*", i.e., "find all beginning with z"
            # (or "zz*" if eplen == 2, etc.)

            elsif( length $phrase <= $eplen ) {

                # get entry point bitstring(s)
                for my $ep ( split $Sp => $self->retrieve_index_key_kv( $index_key => 'eplist' ) ) {
                    if( $ep =~ m{^$phrase} ) {
                        my $ep_keynum = $self->retrieve_entry_point_kv( "$index_key$Sp$ep" => 'keynum' );
                        my $ep_rec = $ds->retrieve( $ep_keynum );
                        push @matches, [ split $Sp => $ep_rec->data ];
                    }
                }

            }

            # else if phrase is shorter than our entry group length,
            # then we know that every index entry in every matching
            # entry group will match our phrase

            elsif( length $phrase <= $eglen ) {

                # locate the starting entry point
                my $ep;
                for( split $Sp => $self->retrieve_index_key_kv( $index_key => 'eplist' ) ) {
                    if( $phrase =~ m{^$_} ) {
                        $ep = $_;
                        last;
                    }
                }

                # start traversing entry groups
                my $match_key = join $Sp => $index_key, $ep, $phrase;
                my $found;

                my $this_group = $self->retrieve_entry_point_kv( "$index_key$Sp$ep" => 'next' );
                while( $this_group ) {

                    if( $match_key =~ m{^$this_group} ) {
                        $found++;
                        my( $eg_keynum, $this_group ) = $self->retrieve_entry_group_kv( $this_group, 'keynum', 'next' );
                        my $eg_rec = $ds->retrieve( $eg_keynum );

                        # loop through the index entries in this group
                        # we want every index entry, because we know
                        # they all match our phrase

                        for( split $nl => $eg_rec->data ) {
                            if( m{$Sep([0-9]+)$Sp(.*)$} ) {
                                push @matches, [ $1, $2 ];  # (count) (bitstring)
                            }
                        }
                    }
                    else {
                        last if $found;  # short circuit the rest
                    }

                }

            }

            # else phrase is longer than our entry group length,
            # so we need to get the one matching entry group and
            # find every matching index entry in it

            else {
                my $match_space   = quotemeta $Sp;
                my $match_sep     = quotemeta $Sep;
                my $match_tag     = quotemeta $tag;
                my $match_phrase  = quotemeta $phrase;
                   $match_phrase .= '.*';  # truncated

                my $matchrx = qr{^
                    $match_tag     $match_space  #    tag
                    $match_phrase                #    phrase
                    $match_sep
                    ([0-9]+)       $match_space  # $1 count
                    (.*)                         # $2 bitstring
                    $}x;

                my $eg_keynum         = $self->retrieve_entry_group_kv( $entry_group => 'keynum' );
                my( $fh, $pos, $len ) = $ds->locate_record_data( $eg_keynum );

                my $found;
                my $got;
                local $/ = $nl;  # $fh is opened in binmode
                while( <$fh> ) {
                    last if ($got += length) > $len;
                    chomp;
                    if( /$matchrx/ ) {
                        $found++;
                        push @matches, [ $1, $2 ];
                    }
                    else {
                        last if $found;  # short circuit the rest
                    }
                    last if $got == $len;
                }
                close $fh;
            }

        }  # if truncated

        else {
            if( my $eg_keynum = $self->retrieve_entry_group_kv( $entry_group => 'keynum' ) ) {

                my $match_space   = quotemeta $Sp;
                my $match_sep     = quotemeta $Sep;
                my $match_tag     = quotemeta $tag;
                my $match_phrase  = quotemeta $phrase;

                my $matchrx = qr{^
                    $match_tag     $match_space  #    tag
                    $match_phrase                #    phrase
                    $match_sep
                    ([0-9]+)       $match_space  # $1 count
                    (.*)                         # $2 bitstring
                    $}x;

                my( $fh, $pos, $len ) = $ds->locate_record_data( $eg_keynum );

                my $got;
                local $/ = $nl;  # $fh is opened in binmode
                while( <$fh> ) {
                    last if ($got += length) > $len;
                    chomp;
                    if( /$matchrx/ ) {
                        push @matches, [ $1, $2 ];
                        last;  # only one phrase will match
                    }
                    last if $got == $len;
                }
                close $fh;
            }
        }
    }  # TRY

    $self->dbm( '' );
    untie %dbm;
    $self->unlock;

    return unless @matches;

    my( $count, $bitstring );
    if( @matches == 1 ) {
        ( $count, $bitstring ) = @{$matches[0]};
    }
    else {
        my $vec = '';
        for( @matches ) { $vec |= str2bit uncompress $_->[1] }
        $count     = howmany          $vec;
        $bitstring = compress bit2str $vec;
    }

    return $count, $bitstring if wantarray;
    return $bitstring;
}

#---------------------------------------------------------------------

=head2 initial_load()

Initial load of index data. Index must be empty.

=cut

sub initial_load {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 apply()

Apply a batch of index entries. Index may be empty or not.

=cut

sub apply {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------
#
# =head2 get_kw_keys()
#
# Called by add_kw() and delete_kw() to parse out the key values needed
# by add_item() and delete_item()
#
#     $index->get_kw_keys( $parms );
#
# In this example, C<$parms> is the same hash ref that was passed to
# add_kw() or delete_kw().
#
# Private method.
#
# =cut
#

sub get_kw_keys {
    my( $self, $parms ) = @_;

    # return values:
    my $index_key;
    my $entry_point;
    my $entry_group;
    my $index_entry;
    my $truncated;
    my $eplen;
    my $eglen;

    my $config = $self->config;

    # ascii
    for( $parms->{'tag'} ) {

        croak qq/Missing: tag/ unless defined;

        my $taginfo = $config->{'kw'}{'tags'}{ $_ };

        croak qq/Unrecognized keyword index tag: $_/ unless $taginfo;

        $eplen = $taginfo->{'eplen'} ||
            $config->{'kw'}{'eplen'} ||
            $config->{'eplen'}       ||
            $default_eplen;
        $eglen = $taginfo->{'eglen'} ||
            $config->{'kw'}{'eglen'} ||
            $config->{'eglen'}       ||
            $default_eglen;

        $index_key = $_;
    }

    # should be decoded already (i.e., in perl's internal format)
    for( $parms->{'keyword'} ) {
        croak qq/Missing: keyword/ unless defined;
        croak qq/Keyword may not contain spaces: $_/ if /$Sp/;
        $truncated = 1 if m{[$Trunc]$};
        my $ep = substr $_, 0, $eplen;
        my $eg = substr $_, 0, $eglen;
        $entry_point = "$index_key $ep";
        $entry_group = "$index_key $ep $eg";
        $index_entry = "$index_key $_";
    }

    # ascii, maybe 0-9 even
    for( $parms->{'field'} ) {
        # croak qq/Missing: field/    unless defined;  # XXX optional ...
        last unless defined;
        croak qq/Invalid field: $_/ unless /^[0-9A-Za-z]+$/;
        $index_entry .= " $_";
    }

    # 0-9
    for( $parms->{'occ'} ) {
        # croak qq/Missing: occ/    unless defined;  # XXX optional ...
        last unless defined;
        croak qq/Invalid occ: $_/ unless /^[0-9]+$/;
        $index_entry .= " $_";
    }

    # 0-9
    for( $parms->{'pos'} ) {
        # croak qq/Missing: pos/    unless defined;  # XXX optional ...
        last unless defined;
        croak qq/Invalid pos: $_/ unless /^[0-9]+$/;
        $index_entry .= " $_";
    }

    return {
        index_key   => $index_key,
        entry_point => $entry_point,
        entry_group => $entry_group,
        index_entry => $index_entry,
        truncated   => $truncated,
        eplen       => $eplen,
        eglen       => $eglen,
    };
}

#---------------------------------------------------------------------
#
# =head2 get_ph_keys()
#
# Called by add_ph() and delete_ph() to parse out the key values needed
# by add_item() and delete_item()
#
#     $index->get_ph_keys( $parms );
#
# In this example, C<$parms> is the same hash ref that was passed to
# add_ph() or delete_ph().
#
# Private method.
#
# =cut
#

sub get_ph_keys {
    my( $self, $parms ) = @_;

    # return values:
    my $index_key;
    my $entry_point;
    my $entry_group;
    my $index_entry;
    my $truncated;
    my $eplen;
    my $eglen;

    my $config = $self->config;

    # ascii
    for( $parms->{'tag'} ) {

        croak qq/Missing: tag/ unless defined;

        my $taginfo = $config->{'ph'}{'tags'}{ $_ };

        croak qq/Unrecognized phrase index tag: $_/ unless $taginfo;

        $eplen = $taginfo->{'eplen'} ||
            $config->{'ph'}{'eplen'} ||
            $config->{'eplen'}       ||
            $default_eplen;
        $eglen = $taginfo->{'eglen'} ||
            $config->{'ph'}{'eglen'} ||
            $config->{'eglen'}       ||
            $default_eglen;

        $index_key = $_;
    }

    # should be decoded already (i.e., in perl's internal format)
    for( $parms->{'phrase'} ) {
        croak qq/Missing: phrase/ unless defined;
        croak qq/Phrase may not contain double spaces: $_/ if /$Sep/;
        $truncated = 1 if m{[$Trunc]$};
        my $ep = substr $_, 0, $eplen;
        my $eg = substr $_, 0, $eglen;
        $entry_point = "$index_key $ep";
        $entry_group = "$index_key $ep $eg";
        $index_entry = "$index_key $_";
    }

    return {
        index_key   => $index_key,
        entry_point => $entry_point,
        entry_group => $entry_group,
        index_entry => $index_entry,
        truncated   => $truncated,
        eplen       => $eplen,
        eglen       => $eglen,
    };
}

#---------------------------------------------------------------------
#
# =head2 add_item()
#
# Add a keyword or phrase bit to the index.
#
# Private method.
#
# =cut
#

=head2

=begin comment1

given: index key,   e.g., 'ti' xxx
       entry point, e.g., 'ti a'
       entry group, e.g., 'ti a apple'
       index entry, e.g., 'ti apple title 1 2 3 45'

if entry group exists
    - index key exists with at least one element in its eplist
    - entry point exists with at least this one entry group under it
    - an index entry record exists
    if it contains our index entry
        + we turn on our bit (e.g., number 45)
    else if it doesn't contain our index entry
        + we create a new index entry with our bit turned on
    + we get the diff between the entry count in the record before and after
        - the diff is either 0 or 1
    + we save the new index entry record
    if diff is 1
        + we up the entry group count by 1
        + we up the entry point count by 1
        + we up the index key count by 1
else if entry group doesn't exist
    - an index entry record doesn't exist
    + we create an index entry record with our index entry in it
    if index key exists
        if entry point is in the index key's eplist
            - entry point exists
        else if entry point isn't in the eplist
            - entry point doesn't exist
    else if index key doesn't exist
        - entry point doesn't exist

=cut

sub add_item {
    my( $self, $num, $keys ) = @_;

    my $index_key   = $keys->{'index_key'};
    my $entry_point = $keys->{'entry_point'};
    my $entry_group = $keys->{'entry_group'};
    my $index_entry = $keys->{'index_entry'};

    my $enc     = $self->encoding;
    my $ds      = $self->datastore;
    my $dir     = $ds->dir;
    my $name    = $ds->name;
    my $nl      = $ds->recsep;  # "newline" for datastore
    my $userlen = $ds->userlen;

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};
    $self->dbm( \%dbm );

    # if the entry group is already in the dbm file
    if( exists $dbm{ $entry_group } ) {

        my( $eg_keynum, $eg_count ) = $self->retrieve_entry_group_kv( $entry_group => 'keynum', 'count' );
        my $group_rec = $ds->retrieve( $eg_keynum );
        my $rec_data  = Encode::decode( $enc, $group_rec->data );

        # make group_rec data into a hash
        my %entries = map { split $Sep } split $nl => $rec_data;

        my $ie_changed;
        if( exists $entries{ $index_entry } ) {
            my( $ie_howmany, $ie_bitstring ) = split $Sp => $entries{ $index_entry };
            my $ie_vec = str2bit uncompress $ie_bitstring;
            set_bit( $ie_vec, $num );
            my $new_bitstring = compress bit2str $ie_vec;
            if( $new_bitstring ne $ie_bitstring ) {
                $entries{ $index_entry } = join $Sp => howmany( $ie_vec ), $new_bitstring;
                $ie_changed++;
            }
        }
        else {
            my $ie_vec = '';
            set_bit( $ie_vec, $num );
            $entries{ $index_entry } = join $Sp => howmany( $ie_vec ), compress bit2str $ie_vec;
            $ie_changed++;
        }

        if( $ie_changed ) {

            # recreate record data and update group counts
            my $newdata  = '';
            my $newcount = 0;
            for my $key ( sort keys %entries ) {
                my $val   = $entries{ $key };
                $newdata .= join( $Sep => $key, $val ) . $nl;
                $newcount++;
            }
            $newdata = Encode::encode( $enc, $newdata );
            $ds->update({ record => $group_rec, data => $newdata });

            # update the entry point count and index key count
            # (we may have added an index entry)
            if( my $diff = $newcount - $eg_count ) {

                # save the newcount in the entry group
                $self->update_entry_group_kv( $entry_group => { count => $newcount } );

                # add to the entry point count
                my $ep_count = $self->retrieve_entry_point_kv( $entry_point => 'count' );
                $self->update_entry_point_kv( $entry_point => { count => $ep_count + $diff } );

                # add to the index key count
                my $ik_count = $self->retrieve_index_key_kv( $index_key => 'count' );
                $self->update_index_key_kv( $index_key => { count => $ik_count + $diff } );
            }
        }
    }

    # else if that entry group isn't there

    else {

        # initialize a bit vector
        my $ie_vec = '';
        set_bit( $ie_vec, $num );

        # create a new datastore record
        my $newdata = join '' => $index_entry, $Sep,
            howmany( $ie_vec ), $Sp, compress( bit2str $ie_vec ), $nl;

        $newdata = Encode::encode( $enc, $newdata );
        my $eg_rec = $ds->create({
            data => $newdata,
            user => gen_userdata( '[', "eg:$entry_group", ']', $ds->userlen ),
            });

        # set up all star entry
        unless( exists $dbm{ '*' } ) {
            my $all_vec = '';
            set_bit( $all_vec, $num );
            my $all_rec = $ds->create({
                data => howmany( $all_vec ).$Sp.compress( bit2str $all_vec ),
                user => gen_userdata( '[', 'all:*', ']', $ds->userlen ),
                });
            $self->update_all_star_kv( $all_rec->keynum );
        }

        my( $ik_keynum, $ik_count, $eplist );

        if( exists $dbm{ $index_key } ) {
            ( $ik_keynum, $ik_count, $eplist ) = $self->retrieve_index_key_kv( $index_key );
        }
        else {  # set up new index key entry
            my $ik_vec = '';
            set_bit( $ik_vec, $num );
            my $ik_rec = $ds->create({
                data => howmany( $ik_vec ).$Sp.compress( bit2str $ik_vec ),
                user => gen_userdata( '[', "ik:$index_key", ']', $ds->userlen ),
                });
            $ik_keynum = $ik_rec->keynum;
            $ik_count  = 0;
            $eplist    = '';
        }

        my $ep = (split $Sp => $entry_point)[1];  # e.g., 'a' in 'ti a'

        # eplist is string of space-separated entry point characters
        # if entry point not in the list

        if( index( $eplist, $ep ) < 0 ) {

            # insert it in the list
            my @eps;
               @eps = split $Sp => $eplist if $eplist;
               @eps = sort $ep, @eps;

            $eplist = join $Sp => @eps;

            # get prev/next entry points
            my( $prev_ep, $next_ep );
            for my $i ( 0 .. $#eps ) {
                if( $eps[ $i ] eq $ep ) {
                    $prev_ep = $eps[ $i - 1 ] if $i;
                    $next_ep = $eps[ $i + 1 ] if $i < $#eps;
                    last;
                }
            }

            # set up entry point datastore record (bitstring)
            my $ep_vec = '';
            set_bit( $ep_vec, $num );
            my $ep_rec = $ds->create({
                data => howmany( $ep_vec ).$Sp.compress( bit2str $ep_vec ),
                user => gen_userdata( '[', "ep:$entry_point", ']', $ds->userlen ),
                });

            # start building new entry point and entry group entries
            # 1 is for our 1 new index entry
            my $new_ep = { keynum => $ep_rec->keynum, count => 1 };
            my $new_eg = { keynum => $eg_rec->keynum, count => 1 };

            # start getting entry points to insert between
            # if there's a previous entry point, we start there to find
            # its last group, which will be our prev_group

            if( $prev_ep ) {

                my $prev_ep_key = "$index_key $prev_ep";
                my $next_group  = $self->retrieve_entry_point_kv( $prev_ep_key => 'next' );

                my $this_group;

                while( $next_group =~ /^$prev_ep_key/ ) {
                    $this_group = $next_group;
                    $next_group = $self->retrieve_entry_group_kv( $this_group => 'next' );
                }

                # at this point, $this_group is the last group of the prev entry point
                # we want it as our prev group for both of these:

                $new_ep->{ prev } = $this_group;
                $new_eg->{ prev } = $this_group;

                # we also want to change its next group to our new group
                $self->update_entry_group_kv( $this_group => { next => $entry_group } );
            }

            # else if there's no previous entry point, there's also
            # no previous group

            else {
                $new_ep->{ prev } = '';
                $new_eg->{ prev } = '';
            }

            # set this entry point's next group to our new group
            $new_ep->{ next } = $entry_group;

            # if there's a next entry point, make our next group
            # the same as its next group

            if( $next_ep ) {

                my $next_ep_key = "$index_key $next_ep";
                my $next_group  = $self->retrieve_entry_point_kv( $next_ep_key => 'next' );

                # that's the next group we want for our group
                $new_eg->{ next } = $next_group;

                # now make its prev group our group
                $self->update_entry_point_kv( $next_ep_key => { prev => $entry_group } );

                # we also need to change the first group of the next entry point
                # make its prev group our group, too
                $self->update_entry_group_kv( $next_group => { prev => $entry_group } );
            }

            # else if there's no next entry point, there's also no next group

            else {
                $new_eg->{ next } = '';
            }

            # ready now to add these to the dbm file
            $self->update_entry_point_kv( $entry_point => $new_ep );
            $self->update_entry_group_kv( $entry_group => $new_eg );
        }

        # else if the entry point is already in the entry points list

        else {

            # locate groups to insert between
            my( $ep_keynum, $ep_count, $prev_group, $next_group ) = $self->retrieve_entry_point_kv( $entry_point );

            # see if we need to update the entry point bitstring
            my $ep_rec = $ds->retrieve( $ep_keynum );
            my( $ep_howmany, $ep_bitstring ) = split $Sp => $ep_rec->data;
            my $ep_vec = str2bit uncompress $ep_bitstring;
            set_bit( $ep_vec, $num );
            my $new_bitstring = compress bit2str $ep_vec;
            if( $new_bitstring ne $ep_bitstring ) {
                $ds->update({
                    record => $ep_rec,
                    data   => howmany( $ep_vec ).$Sp.$new_bitstring
                    });
            }

            # if we want to insert after the entry point (i.e., become first group)

            if( $next_group gt $entry_group ) {

                # make its next group our group
                # add 1 for the index entry we're adding
                $self->update_entry_point_kv( $entry_point => { count => $ep_count + 1, next => $entry_group } );

                # make its prev group our prev group and its old next group our next group
                # 1 is for our 1 index entry
                $self->update_entry_group_kv( $entry_group => {
                    keynum => $eg_rec->keynum,
                    count  => 1,
                    prev   => $prev_group,
                    next   => $next_group
                    } );

                # now get the entry point's prev group and make it point to us
                # change its next group to our group
                $self->update_entry_group_kv( $prev_group => { next => $entry_group } )
                    if $prev_group;

                # now get the next group (it's always under this entry point)
                # change its prev group to our group
                $self->update_entry_group_kv( $next_group => { prev => $entry_group } );
            }

            # else if we're not inserting after the entry point, find the group to insert after

            else {

                # go ahead and update the entry point (with above values)
                # add 1 for the index entry we're adding
                $self->update_entry_point_kv( $entry_point => { count => $ep_count + 1 } );

                # entry point's next group is never null
                my $this_group;;
                my $eg_next = $next_group;

                while( $eg_next lt $entry_group ) {
                    $this_group = $eg_next;
                    $eg_next = $self->retrieve_entry_group_kv( $this_group => 'next' );
                    last unless $eg_next;  # XXX need this above, too?
                }

                # at this point, $this_group is the group we want to insert after

                # change its next group to our group
                $self->update_entry_group_kv( $this_group => { next => $entry_group } );

                # make it our prev group and its old next group our next group
                # 1 is for our 1 index entry
                $self->update_entry_group_kv( $entry_group => {
                    keynum => $eg_rec->keynum,
                    count  => 1,
                    prev   => $this_group,
                    next   => $eg_next
                    } );

                if( $eg_next ) {

                    # the next group might be under another entry point

                    if( $eg_next !~ /^$entry_point/ ) {

                        my $other_ep = "$index_key " . (split $Sp => $eg_next)[1];
                        $eg_next = $self->retrieve_entry_point_kv( $other_ep => 'next' );

                        # make its prev_group our group
                        $self->update_entry_point_kv( $other_ep => { prev => $entry_group } );

                        # we also need to change the first group of the next entry point
                        # make its prev group our group, too
                        $self->update_entry_group_kv( $eg_next => { prev => $entry_group } );
                    }

                    # else the next group is still under our entry point

                    else {

                        # get the next group
                        # change its prev group to our group
                        $self->update_entry_group_kv( $eg_next => { prev => $entry_group } );
                    }
                }
            }
        }

        # update the index key count for our 1 index entry
        $self->update_index_key_kv( $index_key => {
            keynum => $ik_keynum,
            count  => $ik_count + 1,
            eplist => $eplist,
            } );
    }

    # set the bit in the 'ep', 'ik', and 'all' bitstrings
    # this logic is safe for 'add' (but not for 'delete') ...
    my $all_keynum = $self->retrieve_all_star_kv();
    my $ik_keynum  = $self->retrieve_index_key_kv( $index_key => 'keynum' );
    my $ep_keynum  = $self->retrieve_entry_point_kv( $entry_point => 'keynum' );

    for my $keynum ( $all_keynum, $ik_keynum, $ep_keynum ) {
        next unless $keynum;
        my $rec       = $ds->retrieve( $keynum );
        my $bitstring = (split $Sp => $rec->data)[-1];
        my $vec       = str2bit uncompress $bitstring;
        set_bit( $vec, $num );
        my $new_bitstring = compress bit2str $vec;
        if( $new_bitstring ne $bitstring ) {
            $ds->update({
                record => $rec,
                data   => howmany( $vec ) . $Sp . $new_bitstring
                });
        }
    }

    $self->dbm( '' );
    untie %dbm;
    $self->unlock;

    return( $entry_group, $entry_point, $index_key ) if wantarray;
    return  $entry_group;
}

#---------------------------------------------------------------------
#
# =head2 delete_item()
#
# Delete a keyword or phrase bit from the index.
#
# Private method.
#
# =cut
#

=begin comment2

given: index key,   e.g., 'ti'
       entry point, e.g., 'ti a'
       entry group, e.g., 'ti a apple'
       index entry, e.g., 'ti apple title 1 2 3 45'

(- assertion, + action)

if entry group exists in the dbm file
    - index key exists with at least one element in its eplist
    - entry point exists with at least this one entry group under it
    - an index entry record exists
    if the entry group contains our index entry
        + we turn off our bit (e.g., number 45)
        if the bit count is zero
            + we remove the index entry from the record
            if there are no more index entries in the record

{ inside delete_entry_group() ...

                + we delete the entry group from the dbm file
                  (no need to delete the record -- it's simply ignored)
                if there are no more entry groups under the entry point
                    + we delete the entry point from the dbm file
                    + we delete the entry point from the eplist in the index key
                    if there are no more entry points in the index key
                        + we delete the index key from the dbm file
                    else if there are still entry points
                        + we decrement the index key count (for the index entry we removed)
                else if there are still entry groups
                    + we decrement the entry point count (for the index entry we removed)
}
            else if there are still index entries in the record
                + we update the record with the remaining entries
                + we decrement the entry group count (for the index entry we removed)
                + we decrement the entry point count (for the index entry we removed)
                + we decrement the index key count (for the index entry we removed)
        else if the bit count isn't zero
            + we update the record with the updated index entry
    else if the entry group doesn't contain our index entry
        + error
else if entry group doesn't exist
    + error

=cut

sub delete_item {
    my( $self, $num, $keys ) = @_;

    my $index_key   = $keys->{'index_key'};
    my $entry_point = $keys->{'entry_point'};
    my $entry_group = $keys->{'entry_group'};
    my $index_entry = $keys->{'index_entry'};

    my $enc  = $self->encoding;
    my $ds   = $self->datastore;
    my $dir  = $ds->dir;
    my $name = $ds->name;
    my $nl   = $ds->recsep;  # "newline" for datastore
    my $err;

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};
    $self->dbm( \%dbm );

    # if entry group exists in the dbm file
    if( exists $dbm{ $entry_group } ) {

        # - index key exists with at least one element in its eplist
        # - entry point exists with at least this one entry group under it
        # - an index entry record exists

        my( $eg_keynum, $eg_count, $prev_group, $next_group ) = $self->retrieve_entry_group_kv( $entry_group );

        my $group_rec = $ds->retrieve( $eg_keynum );
        my $rec_data  = Encode::decode( $enc, $group_rec->data );

        # make group_rec data into a hash
        my %entries = map { split $Sep } split $nl => $rec_data;

        # if the entry group contains our index entry
        my $vec;
        if( exists $entries{ $index_entry } ) {
            my( $bit_count, $bstr ) = split $Sp => $entries{ $index_entry };
            $vec = str2bit uncompress $bstr;

            # + we turn off our bit (e.g., number 45)
            set_bit( $vec, $num, 0 );
            $bit_count = howmany( $vec );

            # if the bit count is zero
            if( $bit_count == 0 ) {

                # + we remove the index entry from the record
                delete $entries{ $index_entry };

                # if there are no more index entries in the record
                if( not %entries ) {

                    # + we delete the entry group from the dbm file
                    $self->delete_entry_group( $entry_group, $entry_point, $index_key );

                    # delete the record
                    $ds->delete({ record => $group_rec, data => '' });
                }

                # else if there are still index entries in the record
                else {

                    # + we update the record with the remaining entries
                    my $newdata  = '';
                    for my $key ( sort keys %entries ) {
                        my $val   = $entries{ $key };
                        $newdata .= join( $Sep => $key, $val ) . $nl;
                    }
                    $newdata = Encode::encode( $enc, $newdata );
                    $ds->update({ record => $group_rec, data => $newdata });

                    # + we decrement the entry group count (for the index entry we removed)
                    $self->update_entry_group_kv( $entry_group => { count => $eg_count -1 } );

                    # + we decrement the entry point count (for the index entry we removed)
                    my $ep_count = $self->retrieve_entry_point_kv( $entry_point => 'count' );
                    $self->update_entry_point_kv( $entry_point => { count => $ep_count - 1 } );

                    # + we decrement the index key count (for the index entry we removed)
                    my $ik_count = $self->retrieve_index_key_kv( $index_key => 'count' );
                    $self->update_index_key_kv( $index_key => { count => $ik_count - 1 } );

                }

            }

            # else if the bit count isn't zero
            else {

                # + we update the record with the updated index entry
                $entries{ $index_entry } = join $Sp => $bit_count, compress bit2str $vec;

                my $newdata  = '';
                for my $key ( sort keys %entries ) {
                    my $val   = $entries{ $key };
                    $newdata .= join( $Sep => $key, $val ) . $nl;
                }
                $newdata = Encode::encode( $enc, $newdata );
                $ds->update({ record => $group_rec, data => $newdata });

            }

        }

        # else if the entry group doesn't contain our index entry
        else {

            $err = qq/index entry not found: $index_entry/;
        }

    }

    # else if entry group doesn't exist
    else {

        $err = qq/entry group not found: $entry_group/;
    }

    # long way around to updating the collective bitstrings ...

    $self->update_entry_point_ds( $entry_point );
    $self->update_index_key_ds( $index_key );
    $self->update_all_star_ds();

    $self->dbm( '' );
    untie %dbm;
    $self->unlock;

    croak $err if $err;
}

#---------------------------------------------------------------------
#
# =head2 retrieve_all_star_kv()
#
# returns keynum from '*' key
#

sub retrieve_all_star_kv {
    my( $self ) = @_;

    my @vals = $self->get_vals( '*' );
    return $vals[0];

}

#---------------------------------------------------------------------
#
# =head2 retrieve_index_key_kv( $key, @fields )
#
# $key    is index key, e.g., 'ti', 'au', etc.
# @fields may contain 'keynum', 'count', and/or 'eplist'
#     if no @fields, returns all fields

sub retrieve_index_key_kv {
    my( $self, $key, @fields ) = @_;

    my @vals = $self->get_vals( $key );
    return @vals unless @fields;

    my @ret;
    for( @fields ) {
        m{^ keynum $}x and do{ push @ret, $vals[0];    next };
        m{^ count  $}x and do{ push @ret, $vals[1]||0; next };
        m{^ eplist $}x and do{ push @ret, $vals[2];    next };
        croak qq/Unrecognized field: $_/;
    }
    return @ret if wantarray;
    return $ret[0];
}

#---------------------------------------------------------------------
#
# =head2 update_all_star_kv( $keynum )
#
# update '*' keynum
#
sub update_all_star_kv {
    my( $self, $keynum ) = @_;

    $self->set_vals( '*' => $keynum );
}

#---------------------------------------------------------------------
#
# =head2 update_index_key_kv( $key, $fields )
#
# $key    is index key, e.g., 'ti', 'au', etc.
# $fields is href with keys 'keynum', 'count', and/or 'eplist'
sub update_index_key_kv {
    my( $self, $key, $fields ) = @_;

    my @vals = $self->get_vals( $key );

    while( my( $field, $val ) = each %$fields ) {
        $field =~ m{^ keynum $}x and do{ $vals[0] = $val; next };
        $field =~ m{^ count  $}x and do{ $vals[1] = $val; next };
        $field =~ m{^ eplist $}x and do{ $vals[2] = $val; next };
        croak qq/Unrecognized field: $field/;
    }

die "caller: ".(caller)[2] if $key eq '';  # XXX debug

    $self->set_vals( $key => @vals );
}

#---------------------------------------------------------------------
#
# =head2 retrieve_entry_point_kv( $key, @fields )
#
# $key   is entry point key, e.g., 'ti w', 'au s', etc.
# @fields may contain 'keynum', 'count', 'prev', and/or 'next'
#     if no @fields, returns all fields

sub retrieve_entry_point_kv {
    my( $self, $key, @fields ) = @_;

    my @vals = $self->get_vals( $key );
    while( @vals < 3 ) { push @vals, '' }
    return @vals unless @fields;

    my @ret;
    for( @fields ) {
        m{^ keynum $}x and do{ push @ret, $vals[0];    next };
        m{^ count  $}x and do{ push @ret, $vals[1]||0; next };
        m{^ prev   $}x and do{ push @ret, $vals[2];    next };
        m{^ next   $}x and do{ push @ret, $vals[3];    next };
        croak qq/Unrecognized field: $_/;
    }
    return @ret if wantarray;
    return $ret[0];
}

#---------------------------------------------------------------------
#
# =head2 update_entry_point_kv( $key, $field )
#
# $key    is entry point key, e.g., 'ti w', 'au s', etc.
# $fields is href with keys 'keynum', 'count', 'prev', and/or 'next'

sub update_entry_point_kv {
    my( $self, $key, $fields ) = @_;

    my @vals = $self->get_vals( $key );
    while( @vals < 4 ) { push @vals, '' }

    while( my( $field, $val ) = each %$fields ) {
        $field =~ m{^ keynum $}x and do{ $vals[0] = $val; next };
        $field =~ m{^ count  $}x and do{ $vals[1] = $val; next };
        $field =~ m{^ prev   $}x and do{ $vals[2] = $val; next };
        $field =~ m{^ next   $}x and do{ $vals[3] = $val; next };
        croak qq/Unrecognized field: $field/;
    }

die "caller: ".(caller)[2] if $key eq '';  # XXX debug

    $self->set_vals( $key => @vals );
}

#---------------------------------------------------------------------
#
# =head2 update_entry_point_ds( $ep_key )
#
#   for each entry group (eg)
#       for each index entry (ie)
#           ep_bitstring = ep_bitstring | ie_bitsting
#   write ep_rec => count ep_bitstring
#   return ep_rec
#

sub update_entry_point_ds {
    my( $self, $ep_key ) = @_;

    my( $ep_keynum, $next_group ) = $self->retrieve_entry_point_kv( $ep_key => 'keynum', 'next' );
    return unless $ep_keynum;

    my $ds = $self->datastore;
    my $nl = $ds->recsep;

    my $ep_vec = '';

    # for each entry group (eg)
    my $eg_keynum;
    while( $next_group ) {
        ( $eg_keynum, $next_group ) = $self->retrieve_entry_group_kv( $next_group => 'keynum', 'next' );

        my $eg_rec = $ds->retrieve( $eg_keynum );

        # for each index entry
        for my $index_entry ( split $nl => $eg_rec->data ) {
            my $ie_bitstring = (split $Sp => $index_entry)[-1];
            my $ie_vec = str2bit uncompress $ie_bitstring;
            $ep_vec = $ep_vec | $ie_vec;
        }
    }

    my $ep_rec        = $ds->retrieve( $ep_keynum );
    my $old_bitstring = (split $Sp => $ep_rec->data)[-1];
    my $new_bitstring = compress bit2str $ep_vec;
    if( $new_bitstring ne $old_bitstring ) {
        $ds->update({
            record => $ep_rec,
            data   => howmany( $ep_vec ) . $Sp . $new_bitstring
            });
    }

    return $ep_rec;
}

#---------------------------------------------------------------------
#
# -head2 update_index_key_ds( $ik_key )
#
#   for each entry point (ep)
#       ik_bitstring = ik_bitstring | ep_bitstring
#   write ik_rec => count ik_bitstring
#   return ik_rec
#

sub update_index_key_ds {
    my( $self, $ik_key ) = @_;

    my( $ik_keynum, $eplist ) = $self->retrieve_index_key_kv( $ik_key => 'keynum', 'eplist' );
    return unless $ik_keynum;

    my $ds = $self->datastore;

    my $ik_vec = '';

    # for each entry point
    for my $ep ( split $Sp => $eplist ) {
        my $ep_key       = join $Sp => $ik_key, $ep;
        my $ep_keynum    = $self->retrieve_entry_point_kv( $ep_key => 'keynum' );
        my $ep_rec       = $ds->retrieve( $ep_keynum );
        my $ep_bitstring = (split $Sp => $ep_rec->data)[-1];
        my $ep_vec       = str2bit uncompress $ep_bitstring;
        $ik_vec = $ik_vec | $ep_vec;
    }

    my $ik_rec        = $ds->retrieve( $ik_keynum );
    my $old_bitstring = (split $Sp => $ik_rec->data)[-1];
    my $new_bitstring = compress bit2str $ik_vec;
    if( $new_bitstring ne $old_bitstring ) {
        $ds->update({
            record => $ik_rec,
            data   => howmany( $ik_vec ) . $Sp . $new_bitstring
            });
    }

    return $ik_rec;
}

#---------------------------------------------------------------------
#
# =head2 update_all_star_ds( $self )  # XXX self? (make a method)
#
#   for each index key (ik)
#       all_bitstring = all_bitstring | ik_bitstring
#   write all_rec => count all_bitstring
#   return all_rec
#
#   There is a disconnect between the "all star" entry and the act of
#   deleting a record.  Logically, it is deleting a record that should
#   make the bit for the record be set to 0 in the all star entry.
#   But (XXX so far) we are not calling any index routines for adding
#   or deleting a record.  We are only calling them when we add or
#   delete index entries, i.e., when we're indexing a record.
#
#   To maintain the above logic, we can force each record to have an
#   'id' field that is indexed.  That way, the id index entry for a
#   record will always be present until the record is deleted, at
#   which time the all star bit should be set to 0 by virtue of the
#   fact that there are no longer any index entries for the record.
#
#   XXX investigate how to update the all star entry on record
#   XXX add/delete
#

sub update_all_star_ds {
    my( $self ) = @_;

    my $all_keynum = $self->retrieve_all_star_kv();
    return unless $all_keynum;

    my $ds = $self->datastore;

    my @index_keys;

    my $config = $self->config;
    for( $config->{'kw'}{'tags'}, $config->{'ph'}{'tags'} ) {
        push @index_keys, keys %$_ if defined;
    }

    my $all_vec = '';

    # for each index key
    for my $ik_key ( @index_keys ) {
        my $ik_keynum    = $self->retrieve_index_key_kv( $ik_key => 'keynum' );
        next unless $ik_keynum;  # all indexes might not have entries

        my $ik_rec       = $ds->retrieve( $ik_keynum );
        my $ik_bitstring = (split $Sp => $ik_rec->data)[-1];
        my $ik_vec       = str2bit uncompress $ik_bitstring;
        $all_vec = $all_vec | $ik_vec;
    }

    my $all_rec       = $ds->retrieve( $all_keynum );
    my $old_bitstring = (split $Sp => $all_rec->data)[-1];
    my $new_bitstring = compress bit2str $all_vec;
    if( $new_bitstring ne $old_bitstring ) {
        $ds->update({
            record => $all_rec,
            data   => howmany( $all_vec ) . $Sp . $new_bitstring
            });
    }

    return $all_rec;
}

#---------------------------------------------------------------------
#
# =head2 retrieve_entry_group_kv( $key, @fields )
#
# $key   is entry group key, e.g., 'ti w war', 'au s smith', etc.
# @fields may contain 'keynum', 'count', 'prev', and/or 'next'
#     if no @fields, returns all fields

sub retrieve_entry_group_kv {
    my( $self, $key, @fields ) = @_;

    my @vals = $self->get_vals( $key );
    while( @vals < 4 ) { push @vals, '' }
    return @vals unless @fields;

    my @ret;
    for( @fields ) {
        m{^ keynum $}x and do{ push @ret, $vals[0];    next };
        m{^ count  $}x and do{ push @ret, $vals[1]||0; next };
        m{^ prev   $}x and do{ push @ret, $vals[2];    next };
        m{^ next   $}x and do{ push @ret, $vals[3];    next };
        croak qq/Unrecognized field: $_/;
    }
    return @ret if wantarray;
    return $ret[0];
}

#---------------------------------------------------------------------
#
# =head2 update_entry_group_kv( $key, $field )
#
# $key    is entry group key, e.g., 'ti w war', 'au s smith', etc.
# $fields is href with keys 'keynum', 'count', 'prev', and/or 'next'

sub update_entry_group_kv {
    my( $self, $key, $fields ) = @_;

    my @vals = $self->get_vals( $key );

    while( my( $field, $val ) = each %$fields ) {
        $field =~ m{^ keynum $}x and do{ $vals[0] = $val; next };
        $field =~ m{^ count  $}x and do{ $vals[1] = $val; next };
        $field =~ m{^ prev   $}x and do{ $vals[2] = $val; next };
        $field =~ m{^ next   $}x and do{ $vals[3] = $val; next };
        croak qq/Unrecognized field: $field/;
    }

die "caller: ".(caller)[2] if $key eq '';  # XXX debug

    $self->set_vals( $key => @vals );
}

#---------------------------------------------------------------------
#
# =head2 get_vals( $key )
#
#     $key: key whose value we want (not encoded yet)
#
# returns array of values (by splitting on $Sep)
#
# Private subroutine.
#
# =cut
#

sub get_vals {
    my( $self, $key ) = @_;

    my $enc = $self->encoding;
    my $dbm = $self->dbm;
    $key = Encode::encode( $enc, $key );

    if( my $val = $dbm->{ $key } ) {

        $val = Encode::decode( $enc, $val );
        my $vals = eval $val;
        croak $@ if $@;
        return @$vals if wantarray;
        return $$vals[0];  # scalar context
    }

    return;
}

#---------------------------------------------------------------------
#
# =head2 set_vals( $key, @vals )
#
#     $key: key whose value we're setting (key not encoded yet)
#     @vals: values we're storing (vals not encoded yet)
#
# no useful return value
#
# Private subroutine.
#
# =cut
#

sub set_vals {
    my( $self, $key, @vals ) = @_;

    my $enc = $self->encoding;
    my $dbm = $self->dbm;
    $key = Encode::encode( $enc, $key );

    for( @vals ) {
        $_ = '' unless defined;
    }

    local $Data::Dumper::Terse  = 1;
    local $Data::Dumper::Indent = 0;
    my $val = Dumper \@vals;
       $val = Encode::encode( $enc, $val );

    $dbm->{ $key } = $val;
}

#---------------------------------------------------------------------
#
# =head2 delete_kv( $key )
#
#     $key: key whose value we're setting (key not encoded yet)
#
# no useful return value  XXX right?
#
# Private subroutine.
#
# =cut
#

sub delete_kv {
    my( $self, $key ) = @_;

    my $enc = $self->encoding;
    my $dbm = $self->dbm;

    $key = Encode::encode( $enc, $key );

    delete $dbm->{ $key };
}

#---------------------------------------------------------------------
#
# =head2 delete_entry_group( $entry_group, $entry_point, $index_key );
#
#     $entry_group: to delete
#     $entry_point: to delete or update
#     $index_key:   to delete or update
#
# Private subroutine.
#
# =cut
#

=begin comment3

We delete an entry group when an index entry is deleted and it's the
last index entry in the entry group record.  So the counts for the
entry point and for the index key (if they are not deleted, too) will
be decremented by 1 -- not for the entry group we're deleting, but
for the index entry we deleted.

given: entry_group, prev_group, next_group, entry_point, $index_key

+ we delete the entry group
if entry group is the only group under its entry point
    - it's the only one if its prev group *and* next group are not under its entry point
    + we delete entry point, too
    if entry point is the only one in the index key eplist
        + we delete the index key, too
    else if entry point isn't the only one
        + we decrement the index key count by 1
        + we remove the entry point from the index key eplist

    if the entry group's next group isn't undef
        + we set the next group's prev group to the entry group's prev group
        + we set the next group's entry point's prev group to the entry group's prev group
    if the entry group's prev group isn't undef
        + we set the prev group's next group to the entry group's next group

else if entry group isn't the only one under its entry point
    + we decrement the entry group count by 1
    + we decrement the index key count by 1
    + we set the prev group's next group to the entry group's next group
    + we set the next group's prev group to the entry group's prev group

    if the entry group's next group isn't undef and isn't under it's entry point
        + we set the next group's entry point's prev group to the entry group's prev group
    if the entry group's prev group isn't undef and isn't under it's entry point
        - i.e., it's the first group under its entry point
        + we set the entry point's next group to the entry group's next group

=cut

sub delete_entry_group {
    my( $self, $entry_group, $entry_point, $index_key ) = @_;

    my( $eg_keynum, $eg_count, $eg_prev, $eg_next ) = $self->retrieve_entry_group_kv( $entry_group );
    my( $ep_keynum, $ep_count ) = $self->retrieve_entry_point_kv( $entry_point => 'keynum', 'count' );
    my( $ik_keynum, $ik_count, $eplist ) = $self->retrieve_index_key_kv( $index_key );

    my $ep_regx = qr/^$entry_point/;
    my $ep = (split $Sp => $entry_point)[1];  # e.g., 'a' in 'ti a'

    # + we delete the entry group (group_rec deleted in caller)
    $self->delete_kv( $entry_group );

    # if entry group is the only group under its entry point
    #   - it's the only one if its prev group *and* next group are not under its entry point

    if( (!$eg_prev || $eg_prev !~ $ep_regx) &&
        (!$eg_next || $eg_next !~ $ep_regx) ){

        my $ds = $self->datastore;

        # + we delete entry point, too
        $self->delete_kv( $entry_point );
        my $ep_rec = $ds->retrieve( $ep_keynum );
        $ds->delete({ record => $ep_rec, data => '' });

        # if entry point is the only one in the index key eplist
        if( $eplist eq $ep ) {

            # + we delete the index key, too
            $self->delete_kv( $index_key );
            my $ik_rec = $ds->retrieve( $ik_keynum );
            $ds->delete({ record => $ik_rec, data => '' });
        }

        # else if entry point isn't the only one
        else {

            # + we decrement the index key count by 1
            # - the resulting count will be > 0
            # + we remove the entry point from the index key eplist
            $eplist = join $Sp => grep { $_ ne $ep } split $Sp => $eplist;
            $self->update_index_key_kv( $index_key => { count => $ik_count - 1, eplist => $eplist } );
        }

        # if the entry group's next group isn't undef
        if( $eg_next ) {

            # + we set the next group's prev group to the entry group's prev group
            $self->update_entry_group_kv( $eg_next => { prev => $eg_prev } );

            # + we set the next group's entry point's prev group to the entry group's prev group
            my $next_ep_key = join( $Sp => (split( $Sp => $eg_next ))[0,1] );
            $self->update_entry_point_kv( $next_ep_key => { prev => $eg_prev } );
        }

        # if the entry group's prev group isn't undef
        if( $eg_prev ) {

            # + we set the prev group's next group to the entry group's next group
            $self->update_entry_group_kv( $eg_prev => { next => $eg_next } );
        }
    }

    # else if entry group isn't the only one under its entry point
    else {

        # + we decrement the entry point count by 1
        $self->update_entry_point_kv( $entry_point => { count => $ep_count - 1 } );

        # + we decrement the index key count by 1
        $self->update_index_key_kv( $index_key => { count => $ik_count - 1 } );

        # + we set the prev group's next group to the entry group's next group
        $self->update_entry_group_kv( $eg_prev => { next => $eg_next } );

        # + we set the next group's prev group to the entry group's prev group
        $self->update_entry_group_kv( $eg_next => { prev => $eg_prev } );

        # if the entry group's next group isn't undef and isn't under its entry point
        if( $eg_next and $eg_next !~ $ep_regx ) {

            # + we set the next group's entry point's prev group to the entry group's prev group
            my $next_ep_key = join( $Sp => (split( $Sp => $eg_next ))[0,1] );
            $self->update_entry_point_kv( $next_ep_key => { prev => $eg_prev } );
        }

        # if the entry group's prev group isn't undef and isn't under it's entry point
        # - i.e., it's the first group under its entry point
        if( $eg_prev and $eg_prev !~ $ep_regx ) {

            # + we set the entry point's next group to the entry group's next group
            $self->update_entry_point_kv( $entry_point => { next => $eg_next } );
        }
    }
}

#---------------------------------------------------------------------
#
# =head2 gen_userdata( $beg, $msg, $end, $len );
#
# returns "$beg$msg$end" with $len padding between $msg and $end
#

sub gen_userdata {
    my( $beg, $msg, $end, $len ) = @_;
    return '' unless $len;
    my $pad = $len-(length($beg)+length($end)+length($msg));
    return         $beg.$msg.(' 'x$pad) .$end if $pad >= 0;
    return substr( $beg.$msg, 0,  $pad ).$end;
}

#---------------------------------------------------------------------
#
# =head2 readlock()
#
# Gets the lock file name from the object, opens it for input, locks
# it, and stores the open file handle in the object.  This file handle
# isn't really used except for locking, so it's a bit of a "lock token"
#
# Private method.
#
# =cut
#

sub readlock {
    my( $self ) = @_;

    my $file = $self->dbm_lock_file;
    my $fh;

    sysopen( $fh, $file, O_RDONLY|O_CREAT ) or croak qq/Can't open for read $file: $!/;
    flock $fh, LOCK_SH   or croak qq/Can't lock shared $file: $!/;
    binmode $fh;

    $self->locked( $fh );
}

#---------------------------------------------------------------------
#
# =head2 writelock()
#
# Get the lock file name from the object, opens it for read/write,
# locks it, and stores the open file handle in the object.
#
# Private method.
#
# =cut
#

sub writelock {
    my( $self ) = @_;

    my $file = $self->dbm_lock_file;
    my $fh;

    sysopen( $fh, $file, O_RDWR|O_CREAT ) or croak qq/Can't open for read-write $file: $!/;
    my $ofh = select( $fh ); $| = 1; select ( $ofh );  # flush buffers
    flock $fh, LOCK_EX                    or croak qq/Can't lock exclusive $file: $!/;
    binmode $fh;

    $self->locked( $fh );
}

#---------------------------------------------------------------------
#
# =head2 unlock()
#
# Closes the file handle -- the "lock token" in the object.
#
# Private method.
#
# =cut
#

sub unlock {
    my( $self ) = @_;

    my $file = $self->dbm_lock_file;
    my $fh   = $self->locked;

    close $fh or croak qq/Problem closing $file: $!/;
}

#---------------------------------------------------------------------
#
# =head2 debug_kv()
#
# Prints a columnar report of the contents of the DBM file.
# Used for tests.
#
# $title: simple title string for report
# $force: boolean value to override 100-record index limit
#
# Private method.
#
# =cut
#

sub debug_kv {
    my( $self, $title, $force ) = @_;

    $title ||= '';
    $force ||= 0;
    my $enc  = $self->encoding;
    my $ds   = $self->datastore;
    my $dir  = $ds->dir;
    my $name = $ds->name;

    # guard against accidental use against a big index
    croak qq/Index too big for debug_kv()/ if $ds->howmany > 100 and !$force;

    my @ret; push @ret, "\n$title\n" if $title;

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};
    $self->dbm( \%dbm );

    my @keys = sort keys %dbm;
    my $max  = sub {$_[$_[0]<$_[1]]};
    my $mx   = 0;
       $mx   = $max->( $mx, length ) for @keys;
    for my $key ( @keys ) {
        my @vals     = $self->get_vals( $key );  # generic retrieve
        my @keyparts = split $Sp  => $key;
        no warnings 'uninitialized';
        if(    $key eq '*' ) { # all star
            push @ret, sprintf "%-${mx}s | %6s |\n",                            $key, @vals }
        elsif( @keyparts == 1 ) { # index key
            push @ret, sprintf "%-${mx}s | %6s | %6s= | %${mx}s |\n",           $key, @vals }
        elsif( @keyparts == 2 ) { # entry point
            push @ret, sprintf "%-${mx}s | %6s | %6s+ | %${mx}s | %${mx}s |\n", $key, @vals }
        else { # entry group
            push @ret, sprintf "%-${mx}s | %6s | %6s  | %${mx}s | %${mx}s |\n", $key, @vals }
    }
    push @ret, "\n";

    $self->dbm( '' );
    untie %dbm;
    $self->unlock;

    join '' => @ret;  # returned
}

1;  # returned

__END__

