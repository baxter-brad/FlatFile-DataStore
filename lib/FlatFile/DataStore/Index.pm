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
            enc  => 'utf-8',             # character encoding
            tags => {                    # index tags
                ti => {                  # keys are actual tags
                    label => 'title',    # label is required
                    eplen => 1,          # will override the global
                    eglen => 5,          # ditto
                },
                au => {
                    label => 'author',
                },
                su => {
                    label => 'subject',
                },
            },
            eplen => 1,  # global setting for entry point key length
            eglen => 8,  # global setting for entry group key length
        },
    });

    # instantiate an existing index

    my $index = FlatFile::DataStore::Index->new({
        name = $name,
        dir  = $dir,
    });

    # add/delete a keyword entry

    $index->add_kw({
        tag   => 'ti',
        kw    => $keyword,
        field => $field,
        occ   => $occurrence,
        pos   => $position,
        });

    $index->delete_kw({
        tag   => 'ti',
        kw    => $keyword,
        field => $field,
        occ   => $occurrence,
        pos   => $position,
        });

    # add/delete a phrase entry

    $index->add_ph({
        tag   => '_ti',
        ph    => $phrase,
        });

    $index->delete_ph({
        tag   => 'ti',
        ph    => $phrase,
        });

    # get a bitstring group for a keyword

    my $group     = $index->get_kw({
        tag => 'ti',
        kw  => $keyword,
        });

    # get a bitstring for a phrase

    my $bitstring = $index->get_ph({
        tag => '_ti',
        ph  => $phrase,
        });

=head1 DESCRIPTION

FlatFile::DataStore::Index

=head1 VERSION

FlatFile::DataStore::Index version 1.02

=cut

our $VERSION = '1.02';

use 5.008003;
use strict;
use warnings;

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

#---------------------------------------------------------------------

=head1 DESCRIPTION

Returns a reference to the FlatFile::DataStore::Index object.

=head2 Class Methods

=cut

#---------------------------------------------------------------------
# accessors
# the following are required attributes, so simple accessors are okay
#
# Private methods.

sub config        {for($_[0]->{config       }){$_=$_[1]if@_>1;return$_}}
sub datastore     {for($_[0]->{datastore    }){$_=$_[1]if@_>1;return$_}}
sub dbm_lock_file {for($_[0]->{dbm_lock_file}){$_=$_[1]if@_>1;return$_}}
sub locked        {for($_[0]->{locked       }){$_=$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------

=head3 new()

Create a new index object.

The parms C<name> and C<dir> are always required.

If the index has been initialized yet, C<uri> and C<config> are
also required to do that.

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

    eval qq{require $dbm_package; 1} or croak qq/Can't use $dbm_package: $@/;

    my ( $self, $parms ) = @_;

    my $dir    = $parms->{'dir' };  # always required
    my $name   = $parms->{'name'};  # always required
    my $uri    = $parms->{'uri' };
    my $config = $parms->{'config'};

    # uri is required to initialize a new datastore

    my $ds_parms = { name => $name, dir => $dir };
       $ds_parms->{'uri'} = $uri if $uri;

    my $ds = FlatFile::DataStore->new( $ds_parms );

    # config is required to configure a new index

    # 0th record is the config record
    my     $config_rec;
    eval { $config_rec = $ds->retrieve( 0 ) };
    if( $@ ) {
        die $@ unless $@ =~ /Record doesn't exist/;
    }

    # here is where we configure the index
    if( $config ) {
        croak qq/Index already configured, not allowed: config/ if $config_rec;

        # make config a one-liner
        local $Data::Dumper::Quotekeys = 0;
        local $Data::Dumper::Pair      = '=>';
        local $Data::Dumper::Useqq     = 1;
        local $Data::Dumper::Terse     = 1;
        local $Data::Dumper::Indent    = 0;

        $config_rec = $ds->create({ data => Dumper $config });
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
    $self->datastore( $ds );
    $self->dbm_lock_file( "$dir/$name$dbm_lock_ext" );

    return $self;
}

#---------------------------------------------------------------------

=head2 add_kw()

    # add a keyword

    $index->add_kw({
        tag   => 'ti',
        kw    => $keyword,
        field => $field,
        occ   => $occurrence,
        pos   => $position,
        num   => $keynum,
        });

=for comment

Things that may be touched:
- the group of index entries in the datastore
  - we may add a new group
  - we may add a new member of a group
  - we may flip a bit on in an existing group member
- the entry group in the dbm file
  - we may add a new entry group, with a new datastore keynum
  - we may update the entry count in an existing entry group
- the entry point in the dbm file
  - we may add a new entry point when we add a new entry group
  - we may update the entry count in an existing entry point
- the index key in the dbm file
  - we may add a new index key, with the new entry point we just added
  - we may add a new entry point to the entry point list of the index key

Steps (hopefully in the order of most likelihood over time):
- construct an entry group key: tag entry_point entry_group,
  and entry point key: tag entry_point, and an index key: tag
  - look for that in the dbm file
  - if it's there, get the keynum, entry_count, prev_group, next_group, i.e.,
    if there is already a group of index entries in the datastore
    - merge this entry into that group, i.e.,
      - retrieve the datastore record for the group
      - scan for "tag kw field occ pos" in the group
      - if found, flip on the bit for num
      - else if not found, insert an entry with the bit flipped on
      - recalculate the sum of counts for the entry group
        - update the entry count for the entry group in the dbm file
        - update the entry count for the entry point in the dbm file
  - else if it's not there (i.e., there isn't already a group of entries)
    - create a new datastore record for the group
    - fill it with this one entry with the bit flipped on
    - calculate the sum of counts for the entry group (i.e., 1)

    (at this juncture I need the entry point in order to track down
    the prev and next groups, so I start checking at the index key)

    - if the index key isn't in the dbm file
      - add the index key to the dbm file with this entry point in its list
    - else if there is an index key but the entry point isn't in the index key list
      - insert the entry point in the list
      - add an entry point in the dbm file
        - the entry count is the same as for the new group, i.e., 1
        - the prev_group is the last group of the prev entry point
        - the next_group is the first group of the next entry point
    - add the entry group to the dbm file
      - the keynum is from the new datastore record
      - the entry count is as calculated, i.e., 1
      - the prev_group is the prev group for this entry point
      - the next_group is the next group for this entry point

=cut

sub get_kw_keys {
    my( $self, $parms ) = @_;

    my $index_key;
    my $entry_point;
    my $entry_group;
    my $index_entry;

    my $eplen;
    my $eglen;
    my $config = $self->config;

    for( $parms{'tag'} ) {
        croak qq/Missing: tag/ unless defined;
        $index_key = $_;
        $eplen = $config->{ $_ }{'eplen'}||$config->{'eplen'};
        $eglen = $config->{ $_ }{'eglen'}||$config->{'eglen'};
    }
    for( $parms{'kw'} ) {
        croak qq/Missing: kw/ unless defined;
        my $ep = substr $_, 0, $eplen;
        my $eg = substr $_, 0, $eglen;
        $entry_point = "$index_key $ep";
        $entry_group = "$index_key $ep $eg";
        $index_entry = "$index_key $_";
    }
    for( $parms{'field'} ) {
        croak qq/Missing: field/ unless defined;
        $index_entry .= " $_";
    }
    for( $parms{'occ'} ) {
        croak qq/Missing: occ/ unless defined;
        $index_entry .= " $_";
    }
    for( $parms{'pos'} ) {
        croak qq/Missing: pos/ unless defined;
        $index_entry .= " $_ ";
    }

    # returned
    ( $index_key, $entry_point, $entry_group, $index_compare, $index_entry );
}

sub add_kw {
    my( $self, $parms ) = @_;

    my $ds    = $self->datastore;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    my $num   = $parms{'num'};
    croak qq/Missing: num/ unless defined $num;

    my( $index_key, $entry_point, $entry_group, $index_entry ) =
        $self->get_kw_keys( $parms );

    my $sep = ' ' x 2;  # separator between groups of elements

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};

    # if the entry group is already in the dbm file
    if( my $entry_group_val = $dbm{ $entry_group } ) {

        # val is ( keynum, group_count, prev_group, next_group )
        my( $keynum, $group_count ) = split $sep => $entry_group_val, 2;
        my( $group_rec )            = $ds->retrieve( $keynum );

        # make group_rec data into a hash
        my %entries = map { split $sep } split "\n" => ${$group_rec->data};

        my $vec = '';
        if( my $try = entries{ $index_entry } ) {
            my( undef, $bstr ) = split ' ' => $try;  # $try is "count bitstring"
            $vec = str2bit uncompress $bstr;
        }

        # update vector (or create a new one)
        set_bit( $vec, $num );
        $entries{ $index_entry } = join ' ' => howmany( $vec ), compress bit2str $vec;

        # recreate record data and update group counts
        my $newdata  = '';
        my $newcount = 0;
        for my $key ( sort keys %entries ) {
            my $val   = $entries{ $key };
            $newdata .= join( $sep => $key, $val ) . "\n";
            $newcount++;
        }
        $ds->update({ record => $group_rec, data => $newdata });

        # update the entry point count and index key count
        # (we may have added an index entry)
        if( my $diff = $newcount - $group_count ) {

            # val is ( count, prev_group, next_group )
            my( $count, $prev_group, $next_group ) = split $sep => $dbm{ $entry_point };
            $dbm{ $entry_point } = join $sep => $count + $diff, $prev_group, $next_group;

            # val is ( count, eplist )
            my( $epcount, $eplist ) = split $sep => $dbm{ $index_key };
            $dbm{ $index_key } = join $sep => $epcount + $diff, $eplist;
        }
    }

    # else if that entry group isn't there
    else {

        # create a new datastore record
        my $vec = '';
        set_bit( $vec, $num );

        my $newdata = join '' =>
            $index_entry, $sep, howmany( $vec ), ' ',
            compress( bit2str $vec ), "\n";

        my $rec = $ds->create({ data => $newdata });

        my $ep = (split ' ' => $entry_point)[1];

        # val is ( count, string of space-sep entry point characters )
        my( $count, $ep_chars ) = split $sep => $dbm{ $index_key };

        # if entry point not in the list
        if( not defined $ep_chars || index $ep_chars, $ep < 0 ) {

            # insert it in the list (1 is for our 1 index entry)
            my @eps            = sort $ep, split ' ' => $ep_chars;
            $dbm{ $index_key } = join $sep => 1, join ' ' => @eps;

            # get prev/next entry points
            my( $prev_ep, $next_ep );
            for my $i ( 0 .. $#eps ) {
                if( $eps[ $i ] eq $ep ) {
                    $prev_ep = $eps[ $i - 1 ] if $i;
                    $next_ep = $eps[ $i + 1 ] if $i < $#eps;
                    last;
                }
            }

            # start getting entry points to insert between
            # 1 is for our 1 index entry
            my @new_ep = ( 1 );
            my @new_eg = ( 1, $rec->keynum );

            # if there's a previous entry point, we start there to find
            # its last group, which will be our prev_group

            if( $prev_ep ) {

                # val is ( count, prev_group, next_group )
                my $prev_ep_key = "$index_key $prev_ep";
                my( $count, $prev_group, $next_group ) = split $sep => $dbm{ $prev_ep_key };

                my $keynum;
                my $this_group;
                while( $next_group ~= /^$prev_ep_key/ ) {
                    $this_group = $next_group;
                    ( $keynum, $count, $prev_group, $next_group ) = split $sep => $dbm{ $this_group };
                }

                # at this point, $this_group is the last group of the prev entry point
                push @new_ep, $this_group;  # we want it as our prev group
                push @new_eg, $this_group;  # for both of these

                # we also want to change its next group to our new group
                $dbm{ $this_group } = join $sep => $keynum, $count, $prev_group, $entry_group;
            }

            # else if there's no previous entry point, there's also
            # no previous group
                
            else {
                push @new_ep, '';
                push @new_eg, '';
            }

            # this entry point's next group is our new group
            push @new_ep, $entry_group;

            # if there's a next entry point, make our next group the same as its

            if( $next_ep ) {

                # val is ( count, prev_group, next_group )
                my $next_ep_key = "$index_key $next_ep";
                my( $count, $prev_group, $next_group ) = split $sep => $dbm{ $next_ep_key };

                # that's the next group we want for our group
                push @new_eg, $next_group;

                # now make its prev group our group
                $dbm{ $next_ep_key } = join $sep => $count, $entry_group, $next_group;

                # we also need to change the first group of the next entry point
                my $keynum;
                my $this_group = $next_group;
                ( $keynum, $count, $prev_group, $next_group ) = split $sep => $dbm{ $this_group };

                # make its prev group our group, too
                $dbm{ $this_group } = join $sep => $keynum, $count, $entry_group, $next_group;
            }

            # else if there's no next entry point, there's also no next group

            else {
                push @new_eg, '';
            }

            # ready now to add these to the dbm file
            $dbm{ $entry_point } = join $sep => @new_ep;
            $dbm{ $entry_group } = join $sep => @new_eg;

            # update the index key count for our 1 index entry
            my( $count, $eplist ) = split $sep => $dbm{ $index_key };
            $dbm{ $index_key } = join $sep => ++$count, $eplist;
        }

        # else if the entry point is already in the entry points list

        else {

            # locate groups to insert between
            my( $count, $prev_group, $next_group ) = split $sep => $dbm{ $entry_point };

            # add 1 for the index entry we're adding
            $dbm{ $entry_point } = join $sep => ++$count, $prev_group, $next_group;

            my $keynum;
            my $this_group;

            # entry point's next group is never null
            while( $next_group gt $entry_group ) { 
                $this_group = $next_group;
                ( $keynum, $count, $prev_group, $next_group ) = split $sep => $dbm{ $this_group };
            }

            # at this point, $this_group is the group we want to insert after

            # change its next group to our group
            $dbm{ $this_group } = join $sep => $keynum, $count, $prev_group, $entry_group;
            
            # make it our prev group and its old next group our next group
            # 1 is for our 1 index entry
            $dbm{ $entry_group } = join $sep => 1, $rec->keynum, $this_group, $next_group;

            # the next group might be under another entry point

            if( $next_group !~ /^$entry_point ) {

                my $other_ep = "$index_key " . (split ' ' => $next_group)[1];
                my ( $count, $prev_group, $next_group ) = split $sep => $dbm{ $other_ep };

                # make its prev_group our group
                $dbm{ $other_ep } = join $sep => $count, $entry_group, $next_group;

                # we also need to change the first group of the next entry point
                my $keynum;
                my $this_group = $next_group;
                ( $keynum, $count, $prev_group, $next_group ) = split $sep => $dbm{ $this_group };

                # make its prev group our group, too
                $dbm{ $this_group } = join $sep => $keynum, $count, $entry_group, $next_group;
            }

            # else the next group is still under our entry point

            else {

                # get the next group
                $this_group = $next_group;
                ( $keynum, $count, $prev_group, $next_group ) = split $sep => $dbm{ $this_group };

                # change its prev group to our group
                $dbm{ $this_group } = join $sep => $keynum, $count, $entry_group, $next_group;

            }

            # update the index key count for our 1 index entry
            my( $count, $eplist ) = split $sep => $dbm{ $index_key };
            $dbm{ $index_key } = join $sep => ++$count, $eplist;
        }
    }

    untie %dbm;
    $self->unlock;

    return( $entry_group, $entry_point, $index_key ) if wantarray;
    return  $entry_group;
}

#---------------------------------------------------------------------

=head2 delete_kw()

    # delete a keyword

    $index->delete_kw({
        tag   => 'ti',
        kw    => $keyword,
        field => $field,
        occ   => $occurrence,
        pos   => $position,
        num   => $keynum,
        });

=cut

sub delete_kw {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 add_ph()

    # add a phrase

    $index->add_ph({
        tag   => '_ti',
        ph    => $phrase,
        num   => $keynum,
        });


=cut

sub add_ph {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 delete_ph()

    # delete a phrase

    $index->delete_ph({
        tag   => 'ti',
        ph    => $phrase,
        num   => $keynum,
        });

=cut

sub delete_ph {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 get_kw_group()


    # get a bitstring group for a keyword

    my $group = $index->get_kw_group({
        tag => 'ti',
        kw  => $keyword,
        });

=cut

sub get_kw_group {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 get_kw_bitstring()


    # get a bitstring for a keyword

    my $group = $index->get_kw_bitsring({
        tag => 'ti',
        kw  => $keyword,
        });

=cut

sub get_kw_bitstring {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------

=head2 get_ph_bitstring()

    # get a bitstring for a phrase

    my $bitstring = $index->get_ph_bitsring({
        tag => '_ti',
        ph  => $phrase,
        });

=cut

sub get_ph_bitstring {
    my( $self, $parms ) = @_;
}

#---------------------------------------------------------------------
# readlock()
#     Takes a file name, opens it for input, locks it, and stores the
#     open file handle in the object.  This file handle isn't really
#     used except for locking, so it's bit of a "lock token"
#
# Private method.

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
# writelock()
#     Takes a file name, opens it for read/write, locks it, and
#     stores the open file handle in the object.
#
# Private method.

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

1;  # returned

__END__

