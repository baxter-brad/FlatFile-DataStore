#---------------------------------------------------------------------
  package FlatFile::DataStore::DBM;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::DBM - Perl module that implements a flat file
data store with a DBM file key access.

=head1 SYNOPSYS

    use Fctnl;
    use FlatFile::DataStore::DBM;

    $FlatFile::DataStore::DBM::dbm_package  = "SDBM_File";  # the defaults
    $FlatFile::DataStore::DBM::dbm_parms    = [ O_CREAT|O_RDWR, 0666 ];
    $FlatFile::DataStore::DBM::dbm_lock_ext = ".dir";

    # new datastore object

    tie my %dshash, 'FlatFile::DataStore::DBM', {
        name        => "dsname",
        dir         => "/my/datastore/directory",
    };

    # create a record and retrieve it
    my $id     = "testrec1";
    my $record = $dshash{ $id } = { data => "Test record", user => "Test user data" };

    # update it (must have a record to update it)

    $record->data( "Updating the test record." );
    $dshash{ $id } = $record;

    # delete it

    delete $dshash{ $id };

    # -or-

    tied(%dshash)->delete({ id => $id, record => $record });

    # get its history

    my @records = tied(%dshash)->history( $id );

=head1 DESCRIPTION

FlatFile::DataStore::DBM implements a tied hash interface to a
flat file data store.  The hash keys are strings that you provide.
These keys do not necessarily have to exist as data in the record.

In the case of delete, you're limited in the tied interface -- you
can't supply a "delete record" (one that has information about the
delete operation).  Instead, it will simply retrieve the existing
record and store that as the delete record.

Note that record data may be created or updated (i.e., STORE'd) two
ways:

As a hash reference, e.g.

    $record = $dshash{ $id } = { data => $record_data, user => $user_data };

As a record object (record data and user data gotten from object),
e.g.,

    $record->data( $record_data );
    $recore->user( $user_data );
    $record = $dshash{ $id } = $record;

Note that in the last line above, the object fetched is not the same as
the one given to be stored (it has a different preamble).

FWIW, this module is not a subclass of FlatFile::DataStore.  Instead,
it is a wrapper, so it's a "has a" relationship rather than an "is a"
one.  But in general, all of the public flat file methods are available
via the tied object, as illustrated by the history() call in the
synopsis.

=head1 VERSION

FlatFile::DataStore::DBM version 0.17

=cut

our $VERSION = '0.17';

use 5.008003;
use strict;
use warnings;

use Fcntl qw(:DEFAULT :flock);
use Carp;

use FlatFile::DataStore;

#---------------------------------------------------------------------
# globals

our $dbm_package  = "SDBM_File";
our $dbm_parms    = [ O_CREAT|O_RDWR, 0666 ];
our $dbm_lock_ext = ".dir";

#---------------------------------------------------------------------

=head1 Tieing the hash

Accepts hash ref giving values for C<dir> and C<name>.

    use Fctnl;
    tie my %dshash, 'FlatFile::DataStore::DBM', {
        name        => $name,
        dir         => $dir,
    };

To initialize a new data store, pass the URI as the value of the
C<uri> parameter, e.g.,

    tie my %dshash, 'FlatFile::DataStore::DBM', {
        dir         => $dir,
        name        => $name,
        uri         => join( ";" =>
        "http://example.com?name=$name",
        "desc=My%20Data%20Store",
        "defaults=medium",
        "user=8-%20-%7E",
        "recsep=%0A",
        ),
    };

(See URI Configuration in FlatFile::DataStore.)
Also accepts a C<userdata> parameter, which sets the default user
data for this instance, e.g.,

Returns a reference to the FlatFile::DataStore::DBM object.

=cut

#---------------------------------------------------------------------
# accessors

# the following are required attributes, so simple accessors are okay

sub ds            {for($_[0]->{ds           }){$_=$_[1]if@_>1;return$_}}
sub dbm_lock_file {for($_[0]->{dbm_lock_file}){$_=$_[1]if@_>1;return$_}}
sub locked        {for($_[0]->{locked       }){$_=$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------
# TIEHASH() supports tied hash access

sub TIEHASH {

    eval qq{require $dbm_package; 1} or die "Can't use $dbm_package: $@";

    my $class = shift;
    my $ds    = FlatFile::DataStore->new( @_ );
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    my $self = {
        ds            => $ds,
        dbm_lock_file => "$dir/$name$dbm_lock_ext",
    };

    bless $self, $class;
}

#---------------------------------------------------------------------
# FETCH() supports tied hash access
#     Returns a FlatFile::DataStore::Record object.

sub FETCH {
    my( $self, $key ) = @_;

    # block efforts to fetch a "_keynum" entry
    croak "Unsupported key format" if $key =~ /^_[0-9]+$/;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and read the keynum
    $self->readlock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $keynum = $dbm_hash{ $key };

    untie %dbm_hash;
    $self->unlock;

    return unless defined $keynum;
    $ds->retrieve( $keynum );  # retrieve and return record
}

#---------------------------------------------------------------------
# STORE() supports tied hash access
#     Returns a FlatFile::DataStore::Record object.
#
#     to help with FIRSTKEY/NEXTKEY, we're keeping two entries
#     in the dbm file for every record:
#         1. record id => key sequence number
#         2. key sequence number => record id
#
#     to avoid collisions with numeric keys, the key of the second
#     entry has an underscore pasted on to the front, e.g., a record
#     whose id is "able_baker_charlie" and whose keynum is 257 would
#     have these entries:
#         1. able_baker_charlie => 257
#         2. _257 => able_baker_charlie

sub STORE {
    my( $self, $key, $parms ) = @_;

    # block efforts to store to "_keynum" entries
    croak "Unsupported key format" if $key =~ /^_[0-9]+$/;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and read the keynum
    $self->writelock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $keynum  = $dbm_hash{ $key };
    my $reftype = ref $parms;

    my $record;

    # for updates, $parms must be a record object
    if( defined $keynum ) {

        croak "Not a record object: $parms ($keynum)"
            unless $reftype and $reftype =~ /Record/;

        # trying to update the record using the wrong key?
        croak "Record key number doesn't match key"
            unless $keynum == $parms->keynum;

        $record = $ds->update( $parms );
    }

    # for creates, $parms may be record, href, sref, or string
    else {

        # record data string
        if( !$reftype or $reftype eq "SCALAR" ) {
            $record = $ds->create({ data => $parms }); 
        }

        # record object or hash, e.g.,
        #     { data => 'record data', user => 'user data' }
        elsif( $reftype =~ /Record/ or
               $reftype eq 'HASH'      ) {
            $record = $ds->create( $parms );
        }

        else { croak "Unrecognized: '$reftype'" }

        # create succeeded, let's store the key
        for( $record->keynum ) {
            $dbm_hash{ $key  } = $_;
            $dbm_hash{ "_$_" } = $key;
        }
    }

    untie %dbm_hash;
    $self->unlock;

    $record;  # returned

}

#---------------------------------------------------------------------
# DELETE() supports tied hash access
#     Returns a FlatFile::DataStore::Record object.
#
#     Otherwise, we must have a record to delete one, so we retrieve
#     it first.
#

sub DELETE {
    my( $self, $key ) = @_;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and read the keynum
    $self->writelock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $keynum = $dbm_hash{ $key };

    # must have a record to delete it
    my $record = $ds->retrieve( $keynum );
       $record = $ds->delete( $record );

    # record delete succeeded, so delete the dbm hash entries
    delete $dbm_hash{ $key };
    delete $dbm_hash{ "_$keynum" };

    untie %dbm_hash;
    $self->unlock;

    $record;  # return the "delete record"
}

#---------------------------------------------------------------------
# CLEAR() supports tied hash access
#     except we don't support CLEAR, because it would be very
#     destructive and it would be a pain to recover from an
#     accidental %h = ();

sub CLEAR {
    croak "Clearing the entire data store is not supported";
}

#---------------------------------------------------------------------
# FIRSTKEY() supports tied hash access

sub FIRSTKEY {
    my( $self ) = @_;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and read the first key (stored as '_0')
    $self->readlock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $firstkey = $dbm_hash{ '_0' };

    untie %dbm_hash;
    $self->unlock;

    $firstkey;  # returned, might be undef
}

#---------------------------------------------------------------------
# NEXTKEY() supports tied hash access

sub NEXTKEY {
    my( $self, $prevkey ) = @_;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    my $nextkey;

    # lock the dbm file and get the prev key's keynum
    $self->readlock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $keynum = $dbm_hash{ $prevkey };

    if( $keynum++ < $ds->lastkeynum ) {
        $nextkey = $dbm_hash{ "_$keynum" };
    }

    untie %dbm_hash;
    $self->unlock;

    $nextkey;  # returned, might be undef
}

#---------------------------------------------------------------------
# SCALAR() supports tied hash access
#     Here we're bypassing the dbm file altogether and simply getting
#     the number of non-deleted records in the data store.  The should
#     be the same as the number of (logical) entries in the dbm hash.

sub SCALAR {
    my $self = shift;
    $self->ds->howmany;  # create|update (no deletes)
}

#---------------------------------------------------------------------
# EXISTS() supports tied hash access

sub EXISTS {
    my( $self, $key ) = @_;

    # block efforts to look at a "_keynum" entry
    croak "Unsupported key format" if $key =~ /^_[0-9]+$/;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and call exists on dbm hash
    $self->readlock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $exists = exists $dbm_hash{ $key };

    untie %dbm_hash;
    $self->unlock;

    return unless $exists;
    $exists;
}

#---------------------------------------------------------------------
# UNTIE() supports tied hash access
#     (see perldoc perltie, The "untie" Gotcha)

sub UNTIE {
    my( $self, $count ) = @_;
    carp "untie attempted while $count inner references still exist" if $count;
}

sub DESTROY {}  # to keep from calling AUTOLOAD

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

    # open $fh, '<', $file or croak "Can't open for read $file: $!";
    sysopen( $fh, $file, O_RDONLY|O_CREAT ) or croak "Can't open for read $file: $!";
    flock $fh, LOCK_SH   or croak "Can't lock shared $file: $!";
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

    sysopen( $fh, $file, O_RDWR|O_CREAT ) or croak "Can't open for read/write $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );  # flush buffers
    flock $fh, LOCK_EX                    or croak "Can't lock exclusive $file: $!";
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

    close $fh or croak "Problem closing $file: $!";
}

#---------------------------------------------------------------------
# get_key()
#     get the key associated with a record sequence number (keynum)

sub get_key {
    my( $self, $keynum ) = @_;

    croak "Not a keynum" unless $keynum =~ /^[0-9]+$/;

    my $ds    = $self->ds;
    my $dir   = $ds->dir;
    my $name  = $ds->name;

    # lock the dbm file and read the key
    $self->readlock;
    tie my %dbm_hash, $dbm_package, "$dir/$name", @{$dbm_parms};

    my $key = $dbm_hash{ "_$keynum" };

    untie %dbm_hash;
    $self->unlock;

    $key;  # returned
}

#---------------------------------------------------------------------
our $AUTOLOAD;
sub AUTOLOAD {

    my   $method = $AUTOLOAD;
         $method =~ s/.*:://;
    for( $method ) {
        croak "Unsupported: $_" unless /^
            retrieve           |
            retrieve_preamble  |
            locate_record_data |
            history            |
            userdata           |
            howmany            |
            lastkeynum         |
            nextkeynum
            $/x;
    }

    my $self = shift;
    $self->ds->$method( @_ );
}

1;  # returned

__END__
