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

my $Sp   = ' ';      # one-space separator
my $Sep  = $Sp x 2;  # two-space separator

my $default_eplen = 1;
my $default_eglen = 8;

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
    # XXX please use ascii only in config?
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

        $config_rec = $ds->create({ data => Dumper( $config )."\n" });
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

given: index key,   e.g., 'ti'
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

sub get_kw_keys {
    my( $self, $parms ) = @_;

    my $index_key;
    my $entry_point;
    my $entry_group;
    my $index_entry;

    my $eplen;
    my $eglen;
    my $config = $self->config;

    # ascii
    for( $parms->{'tag'} ) {

        croak qq/Missing: tag/ unless defined;

        my $taginfo = $config->{'tags'}{ $_ };

        croak qq/Unrecognized tag: $_/ unless $taginfo;

        $eplen = $taginfo->{'eplen'}||$config->{'eplen'}||$default_eplen;
        $eglen = $taginfo->{'eglen'}||$config->{'eglen'}||$default_eglen;

        $index_key = $_;
    }

    # should be decoded already (i.e., in perl's internal format)
    for( $parms->{'kw'} ) {
        croak qq/Missing: kw/ unless defined;
        my $ep = substr $_, 0, $eplen;
        my $eg = substr $_, 0, $eglen;
        $entry_point = "$index_key $ep";
        $entry_group = "$index_key $ep $eg";
        $index_entry = "$index_key $_";
    }

    # ascii, preferably 0-9 even
    for( $parms->{'field'} ) {
        croak qq/Missing: field/ unless defined;
        $index_entry .= " $_";
    }

    # 0-9
    for( $parms->{'occ'} ) {
        croak qq/Missing: occ/ unless defined;
        $index_entry .= " $_";
    }

    # 0-9
    for( $parms->{'pos'} ) {
        croak qq/Missing: pos/ unless defined;
        $index_entry .= " $_";
    }

    # returned
    ( $index_key, $entry_point, $entry_group, $index_entry );
}

sub add_kw {
    my( $self, $parms ) = @_;

    my $enc   = $self->config->{'enc'};
    my $ds    = $self->datastore;

    my $dir   = $ds->dir;
    my $name  = $ds->name;

    my $num   = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    my( $index_key, $entry_point, $entry_group, $index_entry ) =
        $self->get_kw_keys( $parms );

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};

    # if the entry group is already in the dbm file

    my $dbm = \%dbm;

    if( my @vals = get_vals( $dbm, $enc, $entry_group ) ) {

        my( $keynum, $group_count, $prev_group, $next_group ) = @vals;

        my $group_rec = $ds->retrieve( $keynum );
        my $rec_data  = Encode::decode( $enc, $group_rec->data );

        # make group_rec data into a hash
        my %entries = map { split $Sep } split "\n" => $rec_data;

        my $vec = '';
        if( my $try = $entries{ $index_entry } ) {
            my( undef, $bstr ) = split $Sp => $try;  # $try is "count bitstring"
            $vec = str2bit uncompress $bstr;
        }

        # update vector (or create a new one)
        set_bit( $vec, $num );
        $entries{ $index_entry } = join $Sp => howmany( $vec ), compress bit2str $vec;

        # recreate record data and update group counts
        my $newdata  = '';
        my $newcount = 0;
        for my $key ( sort keys %entries ) {
            my $val   = $entries{ $key };
            $newdata .= join( $Sep => $key, $val ) . "\n";
            $newcount++;
        }
        $newdata = Encode::encode( $enc, $newdata );
        $ds->update({ record => $group_rec, data => $newdata });

        # update the entry point count and index key count
        # (we may have added an index entry)
        if( my $diff = $newcount - $group_count ) {

            # save the newcount in the entry group
            set_vals( $dbm, $enc, $entry_group =>
                $keynum, $newcount, $prev_group, $next_group );

            # add to the entry point count
            my( $count, $prev_group, $next_group ) =
                get_vals( $dbm, $enc, $entry_point );
            set_vals( $dbm, $enc, $entry_point =>
                $count + $diff, $prev_group, $next_group );

            # add to the index key count
            my( $index_count, $eplist ) =
                get_vals( $dbm, $enc, $index_key );
            set_vals( $dbm, $enc, $index_key =>
                $index_count + $diff, $eplist );
        }
    }

    # else if that entry group isn't there

    else {

        # create a new datastore record
        my $vec = '';
        set_bit( $vec, $num );

        my $newdata = join '' => $index_entry, $Sep,
            howmany( $vec ), $Sp, compress( bit2str $vec ), "\n";

        $newdata = Encode::encode( $enc, $newdata );
        my $rec = $ds->create({ data => $newdata });

        my $ep = (split $Sp => $entry_point)[1];  # e.g., 'a' in 'ti a'

        # eplist is string of space-separated entry point characters
        my( $index_count, $eplist ) = get_vals( $dbm, $enc, $index_key );
        $index_count ||= 0;
        for( $eplist ) { $_ = '' unless defined }

        # if entry point not in the list
        if( index( $eplist, $ep ) < 0 ) {

            # insert it in the list
            my @eps;
               @eps = split $Sp => $eplist if $eplist;
               @eps = sort $ep, @eps;

            set_vals( $dbm, $enc, $index_key =>
                $index_count, join $Sp => @eps );

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
            # 1 is for our 1 new index entry
            my @new_ep = ( 1 );
            my @new_eg = ( $rec->keynum, 1 );

            # if there's a previous entry point, we start there to find
            # its last group, which will be our prev_group

            if( $prev_ep ) {

                my $prev_ep_key = "$index_key $prev_ep";
                my( $count, $prev_group, $next_group ) = 
                    get_vals( $dbm, $enc, $prev_ep_key );

                my $keynum;
                my $this_group;

                while( $next_group =~ /^$prev_ep_key/ ) {
                    $this_group = $next_group;
                    ( $keynum, $count, $prev_group, $next_group ) =
                        get_vals( $dbm, $enc, $this_group );
                }

                # at this point, $this_group is the last group of the prev entry point

                push @new_ep, $this_group;  # we want it as our prev group
                push @new_eg, $this_group;  # for both of these

                # we also want to change its next group to our new group
                set_vals( $dbm, $enc, $this_group =>
                    $keynum, $count, $prev_group, $entry_group );
            }

            # else if there's no previous entry point, there's also
            # no previous group
                
            else {
                push @new_ep, '';
                push @new_eg, '';
            }

            # this entry point's next group is our new group
            push @new_ep, $entry_group;

            # if there's a next entry point, make our next group
            # the same as its next group

            if( $next_ep ) {

                my $next_ep_key = "$index_key $next_ep";
                my( $count, $prev_group, $next_group ) =
                    get_vals( $dbm, $enc, $next_ep_key );

                # that's the next group we want for our group
                push @new_eg, $next_group;

                # now make its prev group our group
                set_vals( $dbm, $enc, $next_ep_key =>
                    $count, $entry_group, $next_group );

                # we also need to change the first group of the next entry point

                my $keynum;
                my $this_group = $next_group;

                ( $keynum, $count, $prev_group, $next_group ) =
                    get_vals( $dbm, $enc, $this_group );
        
                # make its prev group our group, too
                set_vals( $dbm, $enc, $this_group =>
                    $keynum, $count, $entry_group, $next_group );
            }

            # else if there's no next entry point, there's also no next group

            else {
                push @new_eg, '';
            }

            # ready now to add these to the dbm file
            set_vals( $dbm, $enc, $entry_point => @new_ep );
            set_vals( $dbm, $enc, $entry_group => @new_eg );
        }

        # else if the entry point is already in the entry points list

        else {

            # locate groups to insert between
            my( $count, $prev_group, $next_group ) =
                get_vals( $dbm, $enc, $entry_point );

            my $keynum;
            my $this_group;

            # if we want to insert after the entry point (i.e., become first group)

            if( $next_group gt $entry_group ) {

                # make its next group our group
                # add 1 for the index entry we're adding
                set_vals( $dbm, $enc, $entry_point =>
                    $count + 1, $prev_group, $entry_group );

                # make its prev group our prev group and its old next group our next group
                # 1 is for our 1 index entry
                set_vals( $dbm, $enc, $entry_group =>
                    $rec->keynum, 1, $prev_group, $next_group );

                # save the next group for processing after
                my $save_group = $next_group;

                # now get the entry point's prev group and make it point to us
                $this_group = $prev_group;
                ( $keynum, $count, $prev_group, $next_group ) =
                    get_vals( $dbm, $enc, $this_group );

                # change its next group to our group
                set_vals( $dbm, $enc, $this_group =>
                    $keynum, $count, $prev_group, $entry_group );

                # now get the saved next group (it's always under this entry point)
                $this_group = $save_group;
                ( $keynum, $count, $prev_group, $next_group ) =
                    get_vals( $dbm, $enc, $this_group );

                # change its prev group to our group
                set_vals( $dbm, $enc, $this_group =>
                    $keynum, $count, $entry_group, $next_group );
            }

            # else if we're not inserting after the entry point, find the group to insert after

            else {

                # go ahead and update the entry point (with above values)
                # add 1 for the index entry we're adding
                set_vals( $dbm, $enc, $entry_point =>
                    $count + 1, $prev_group, $next_group );

                # entry point's next group is never null
                while( $next_group lt $entry_group ) { 
                    $this_group = $next_group;
                    ( $keynum, $count, $prev_group, $next_group ) =
                        get_vals( $dbm, $enc, $this_group );
                    last unless $next_group;
                }

                # at this point, $this_group is the group we want to insert after

                # change its next group to our group
                set_vals( $dbm, $enc, $this_group =>
                    $keynum, $count, $prev_group, $entry_group );
                
                # make it our prev group and its old next group our next group
                # 1 is for our 1 index entry
                set_vals( $dbm, $enc, $entry_group =>
                    $rec->keynum, 1, $this_group, $next_group );

                if( $next_group ) {

                    # the next group might be under another entry point

                    if( $next_group !~ /^$entry_point/ ) {

                        my $other_ep = "$index_key " . (split $Sp => $next_group)[1];
                        my( $count, $prev_group, $next_group ) =
                            get_vals( $dbm, $enc, $other_ep );

                        # make its prev_group our group
                        set_vals( $dbm, $enc, $other_ep =>
                            $count, $entry_group, $next_group );

                        # we also need to change the first group of the next entry point
                        my $keynum;
                        my $this_group = $next_group;
                        ( $keynum, $count, $prev_group, $next_group ) =
                            get_vals( $dbm, $enc, $this_group );

                        # make its prev group our group, too
                        set_vals( $dbm, $enc, $this_group =>
                            $keynum, $count, $entry_group, $next_group );
                    }

                    # else the next group is still under our entry point

                    else {

                        # get the next group
                        $this_group = $next_group;
                        ( $keynum, $count, $prev_group, $next_group ) =
                            get_vals( $dbm, $enc, $this_group );

                        # change its prev group to our group
                        set_vals( $dbm, $enc, $this_group =>
                            $keynum, $count, $entry_group, $next_group );
                    }
                }
            }
        }

        # update the index key count for our 1 index entry
        ( $index_count, $eplist ) = get_vals( $dbm, $enc, $index_key );
        set_vals( $dbm, $enc, $index_key => $index_count + 1, $eplist );
    }

    untie %dbm;
    $self->unlock;

    return( $entry_group, $entry_point, $index_key ) if wantarray;
    return  $entry_group;
}

#---------------------------------------------------------------------
# get_vals( $dbm, $enc, $key )
#
# $dbm: tied dbm hash ref
# $enc: character encoding of the dbm file (keys and values)
# $key: key whose value we want (not encoded yet)
#
# returns array of values (by splitting on $Sep)
# (returning extra null strings so missing values are defined)

sub get_vals {
    my( $dbm, $enc, $key ) = @_;

    $key = Encode::encode( $enc, $key );

    if( my $val = $dbm->{ $key } ) {

        $val = Encode::decode( $enc, $val );
        my @vals = split( $Sep => $val );
        return @vals;
    }

    return;
}

#---------------------------------------------------------------------
# set_vals( $dbm, $enc, $key, @vals )
#
# $dbm: tied dbm hash ref
# $enc: character encoding of the dbm file (keys and values)
# $key: key whose value we're setting (key not encoded yet)
# @vals: values we're storing (vals not encoded yet)
#
# no useful return value
#
# Private subroutine.

sub set_vals {
    my( $dbm, $enc, $key, @vals ) = @_;

    $key = Encode::encode( $enc, $key );

    for( @vals ) {
        $_ = '' unless defined;
    }

    my $val = join $Sep => @vals;
       $val = Encode::encode( $enc, $val );

    $dbm->{ $key } = $val;
}

#---------------------------------------------------------------------
# delete_key( $dbm, $enc, $key )
#
# $dbm: tied dbm hash ref
# $enc: character encoding of the dbm file (keys and values)
# $key: key whose value we're setting (key not encoded yet)
#
# Private subroutine.

sub delete_key {
    my( $dbm, $enc, $key ) = @_;

    $key = Encode::encode( $enc, $key );

    delete $dbm->{ $key };
}

#---------------------------------------------------------------------
# delete_entry_group( $dbm, $enc, $entry_group, $entry_point, $index_key );
# dbm: tied dbm hash ref
# enc: character encoding of the dbm file (keys and values)
# $entry_group: to delete
# $entry_point: to delete or update
# $index_key:   to delete or update
#
# Private subroutine.
#

=for comment

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

XXX don't think this is right:

        if the entry group's prev group is the only group under its entry point
        - it's the only group under its entry point if its prev group is undef or
          isn't under its entry point and it's next group isn't under its entry point
          (which we established already)
        + we set the prev group's entry point's next group to the entry group's next group

=cut

sub delete_entry_group {
    my( $dbm, $enc, $entry_group, $entry_point, $index_key ) = @_;

    my( $keynum, $eg_count, $eg_prev, $eg_next ) = get_vals( $dbm, $enc, $entry_group );
    my(          $ep_count, $ep_prev, $ep_next ) = get_vals( $dbm, $enc, $entry_point );
    my(          $ik_count, $eplist            ) = get_vals( $dbm, $enc, $index_key   );

    my $ep_regx = qr/^$entry_point/;
    my $ep = (split $Sp => $entry_point)[1];  # e.g., 'a' in 'ti a'

    delete_key( $dbm, $enc, $entry_group );

#   if entry group is the only group under its entry point
#       - it's the only one if its prev group *and* next group are not under its entry point

    if( (!$eg_prev || $eg_prev !~ $ep_regx) &&
        (!$eg_next || $eg_next !~ $ep_regx) ){

#       + we delete entry point, too

        delete_key( $dbm, $enc, $entry_point );

#       if entry point is the only one in the index key eplist

        if( $eplist eq $ep ) {

#           + we delete the index key, too

            delete_key( $dbm, $enc, $index_key );

        }

#       else if entry point isn't the only one

        else {

#           + we decrement the index key count by 1
#           - the resulting count will be > 0

            --$ik_count;

#           + we remove the entry point from the index key eplist
            $eplist = join $Sp => grep { $_ ne $ep } split $Sp => $eplist;

            set_vals( $dbm, $enc, $index_key => $ik_count, $eplist )

        }

#       if the entry group's next group isn't undef

        if( $eg_next ) {

#           + we set the next group's prev group to the entry group's prev group
            
            my( $ng_num, $ng_count, $ng_prev, $ng_next ) = get_vals( $dbm, $enc, $eg_next );
            set_vals( $dbm, $enc, $eg_next => $ng_num, $ng_count, $eg_prev, $ng_next );

#           + we set the next group's entry point's prev group to the entry group's prev group

            my $next_ep_key = join( $Sp => (split( $Sp => $eg_next ))[0,1] );
            my( $nep_count, $nep_prev, $nep_next ) = get_vals( $dbm, $enc, $next_ep_key );
            set_vals( $dbm, $enc, $next_ep_key => $nep_count, $eg_prev, $nep_next );

        }

#       if the entry group's prev group isn't undef

        if( $eg_prev ) {

#           + we set the prev group's next group to the entry group's next group

            my( $pg_num, $pg_count, $pg_prev, $pg_next ) = get_vals( $dbm, $enc, $eg_prev );
            set_vals( $dbm, $enc, $eg_prev => $pg_num, $pg_count, $pg_prev, $eg_next);

        }
    }


#   else if entry group isn't the only one under its entry point

    else {

#       + we decrement the entry point count by 1

        set_vals( $dbm, $enc, $entry_point => --$ep_count, $ep_prev, $ep_next );

#       + we decrement the index key count by 1

        set_vals( $dbm, $enc, $index_key => $ik_count - 1, $eplist );

#       + we set the prev group's next group to the entry group's next group

        my( $pg_num, $pg_count, $pg_prev, $pg_next ) = get_vals( $dbm, $enc, $eg_prev );
        set_vals( $dbm, $enc, $eg_prev => $pg_num, $pg_count, $pg_prev, $eg_next);

#       + we set the next group's prev group to the entry group's prev group

        my( $ng_num, $ng_count, $ng_prev, $ng_next ) = get_vals( $dbm, $enc, $eg_next );
        set_vals( $dbm, $enc, $eg_next => $ng_num, $ng_count, $eg_prev, $ng_next );

#       if the entry group's next group isn't undef and isn't under its entry point

        if( $eg_next and $eg_next !~ $ep_regx ) {

#           + we set the next group's entry point's prev group to the entry group's prev group

            my $next_ep_key = join( $Sp => (split( $Sp => $eg_next ))[0,1] );
            my( $nep_count, $nep_prev, $nep_next ) = get_vals( $dbm, $enc, $next_ep_key );
            set_vals( $dbm, $enc, $next_ep_key => $nep_count, $eg_prev, $nep_next );

        }

#       if the entry group's prev group isn't undef and isn't under it's entry point
#           - i.e., it's the first group under its entry point

        if( $eg_prev and $eg_prev !~ $ep_regx ) {

#           + we set the entry point's next group to the entry group's next group

            set_vals( $dbm, $enc, $entry_point => $ep_count, $ep_prev, $eg_next );

# XXX don't think this is right ...

# #           if the entry group's prev group is the only group under its entry point
# #           - it's the only group under its entry point if its prev group is undef or
# #             isn't under its entry point and it's next group isn't under its entry point
# #             (which we established already)
# 
#             my $prev_ep_key = join( $Sp => (split( $Sp => $eg_prev ))[0,1] );
#             my( $pep_count, $pep_prev, $pep_next ) = get_vals( $dbm, $enc, $prev_ep_key );
# 
#             if( !$pep_prev or $pep_prev !~ /^$index_key $prev_ep_key / ) {
# 
# #               + we set the prev group's entry point's next group to the entry group's next group
# 
#                 set_vals( $dbm, $enc, $prev_ep_key => $pep_count, $pep_prev, $eg_next );
# 
#             }
        }
    }
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

=for comment

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

sub delete_kw {
    my( $self, $parms ) = @_;

    my $enc  = $self->config->{'enc'};
    my $ds   = $self->datastore;

    my $dir  = $ds->dir;
    my $name = $ds->name;

    my $num  = $parms->{'num'};
    croak qq/Missing: num/ unless defined $num;

    my( $index_key, $entry_point, $entry_group, $index_entry ) =
        $self->get_kw_keys( $parms );

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $dbm = \%dbm;

#   if entry group exists in the dbm file

    if( my @vals = get_vals( $dbm, $enc, $entry_group ) ) {

#       - index key exists with at least one element in its eplist
#       - entry point exists with at least this one entry group under it
#       - an index entry record exists

        my( $keynum, $group_count, $prev_group, $next_group ) = @vals;

        my $group_rec = $ds->retrieve( $keynum );
        my $rec_data  = Encode::decode( $enc, $group_rec->data );

        # make group_rec data into a hash
        my %entries = map { split $Sep } split "\n" => $rec_data;

#       if the entry group contains our index entry

        my $vec;
        if( my $try = $entries{ $index_entry } ) {
            my( $bit_count, $bstr ) = split $Sp => $try;
            $vec = str2bit uncompress $bstr;

#           + we turn off our bit (e.g., number 45)

            set_bit( $vec, $num, 0 );
            $bit_count = howmany( $vec );

#           if the bit count is zero

            if( $bit_count == 0 ) {

#               + we remove the index entry from the record

                delete $entries{ $index_entry };

#               if there are no more index entries in the record

                if( not %entries ) {

#                   + we delete the entry group from the dbm file
#                     (no need to delete the record -- it's simply ignored)

                    delete_entry_group( $dbm, $enc, $entry_group, $entry_point, $index_key );

                }

#               else if there are still index entries in the record

                else {

#                   + we update the record with the remaining entries

                    my $newdata  = '';
                    for my $key ( sort keys %entries ) {
                        my $val   = $entries{ $key };
                        $newdata .= join( $Sep => $key, $val ) . "\n";
                    }
                    $newdata = Encode::encode( $enc, $newdata );
                    $ds->update({ record => $group_rec, data => $newdata });

#                   + we decrement the entry group count (for the index entry we removed)

                    set_vals( $dbm, $enc, $entry_group => $keynum, $group_count - 1, $prev_group, $next_group );

#                   + we decrement the entry point count (for the index entry we removed)

                    my( $ep_count, $ep_prev, $ep_next ) = get_vals( $dbm, $enc, $entry_point );
                    set_vals( $dbm, $enc, $entry_point => $ep_count - 1, $ep_prev, $ep_next );

#                   + we decrement the index key count (for the index entry we removed)

                    my( $ik_count, $eplist ) = get_vals( $dbm, $enc, $index_key);
                    set_vals( $dbm, $enc, $index_key => $ik_count - 1, $eplist );

                }

            }

#           else if the bit count isn't zero

            else {

#               + we update the record with the updated index entry

                $entries{ $index_entry } = join $Sp => $bit_count, compress bit2str $vec;

                my $newdata  = '';
                for my $key ( sort keys %entries ) {
                    my $val   = $entries{ $key };
                    $newdata .= join( $Sep => $key, $val ) . "\n";
                }
                $newdata = Encode::encode( $enc, $newdata );
                $ds->update({ record => $group_rec, data => $newdata });

            }

        }

#       else if the entry group doesn't contain our index entry

        else {

#           + error

            croak qq/index entry not found: $index_entry/;
        }

    }

#   else if entry group doesn't exist

    else {

#       + error

        croak qq/entry group not found: $entry_group/;
    }

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
#     used except for locking, so it's a bit of a "lock token"
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

#---------------------------------------------------------------------
# unlock()
#     closes the file handle -- the "lock token" in the object
#
# Private method.

sub unlock {
    my( $self ) = @_;

    my $file = $self->dbm_lock_file;
    my $fh   = $self->locked;

    close $fh or croak qq/Problem closing $file: $!/;
}

#---------------------------------------------------------------------
# debug()
# one-liner:
# /usr/local/bin/perl -MSDBM_File -e'sub max{$_[$_[0]<$_[1]]}tie%h,"SDBM_File","example",256|2,0666;for(keys%h){$x=max($x,length$_)}for(sort keys%h){printf" %-${x}s | %6s | %${x}s | %${x}s | %s\n",$_,split"  ",$h{$_}}'

sub debug {
    my( $self, $parms ) = @_;

    my $enc   = $self->config->{'enc'};
    my $ds    = $self->datastore;

    my $dir   = $ds->dir;
    my $name  = $ds->name;

    $self->writelock;
    tie my %dbm, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $max = sub {$_[$_[0]<$_[1]]};
    my $x = 0;
    $x = $max->( $x, length ) for keys %dbm;
    for( sort keys %dbm ) {
        no warnings 'uninitialized';
        my @parts = split;
        # index key
        if( @parts == 1 ) {
            printf "%-${x}s | %6s | %6s= | %${x}s |\n", $_, "", split "  ", $dbm{ $_ };
        }
        # entry point
        elsif( @parts == 2 ) {
            printf "%-${x}s | %6s | %6s+ | %${x}s | %${x}s |\n", $_, "", split "  ", $dbm{ $_ };
        }
        # entry group
        elsif( @parts == 3 ) {
            printf "%-${x}s | %6s | %6s  | %${x}s | %${x}s |\n", $_, split "  ", $dbm{ $_ };
        }
    }

    print "\n";

    untie %dbm;
    $self->unlock;
}

1;  # returned

__END__
