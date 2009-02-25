#---------------------------------------------------------------------
  package FlatFile::DataStore;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore - Perl module that implements a flat file data store.

=head1 SYNOPSYS

 use FlatFile::DataStore;

 # new datastore object

 my $dir  = "/my/datastore/area";
 my $name = "dsname";
 my $ds   = FlatFile::DataStore->new( { dir => $dir, name => $name } );

 # create a record

 my $record_data = "This is a test record.";
 my $user_data   = "Test1";
 my $record = $ds->create( $record_data, $user_data );
 my $record_number = $record->keynum();

 # retrieve it

 $record = $ds->retrieve( $record_number );

 # update it

 $record->data( "Updating the test record." );
 $record = $ds->update( $record );

 # delete it

 $record = $ds->delete( $record );

 # get its history

 my @records = $ds->history( $record_number );

=head1 DESCRIPTION

FlatFile::DataStore implements a simple flat file data store.  When you
create (store) a new record, it is appended to the flat file.  When you
update an existing record, the existing entry in the flat file is
flagged as updated, and the updated record is appended to the flat
file.  When you delete a record, the existing entry is flagged as
deleted, and a I<deleted> record is I<appended> to the flat file.

The result is that all versions of a record are retained in the data
store, and running a history will return all of them.  Another result
is that each record in the data store represents a transaction: create,
update, or delete.

A key file acts as an index into the data file(s).  The versions of the
records in the data file(s) act as linked lists:

 - the first version of a record links just to it's successor
 - a second, third, etc., versions, to their successors and predecessors
 - the final (current) version, just to its predecessor

Each record is stored with a I<preamble>, which is a fixed-length
string of fields containing:

 - crud indicator       (flag for created, updated, deleted, etc.)
 - transaction number   (incremented when a record is touched)
 - date                 (of the "transaction")
 - key number           (record sequence number)
 - record length        (in bytes)
 - user data            (for out-of-band* user-defined data)
 - "this" file number   (linked list pointers ...)
 - "this" seek position
 - "prev" file number
 - "prev" seek position
 - "next" file number
 - "next" seek position

* That is, data about the record not stored in the record.

The formats and sizes of these fixed-length fields may be configured
when the data store is first defined, and will determine certain
constraints on the size of the data store.  For example, if the file
number is base 10 and 2 bytes in size, then the data store may have
up to 99 data files.  And if the seek position is base 10 and 9 bytes
in size, then each data file may contain up to 1 Gig of data.

Number bases larger than 10 (up to 36 for file numbers and up to 62 for
other numbers) may be used to help shorten the length of the preamble
string.

A data store will always have only one key file, but may have many data
files.  To retrieve a record, one must know the data file number and
the seek position into that data file, or one must know the record's
sequence number (the order it was added to the data store).  With a
sequence number, the file number and seek position can be looked up in
the key file, so these sequence numbers are called "key numbers" or
C<keynum>.

Methods support the following actions:

 - create
 - retrieve
 - update
 - delete
 - history
 - iterate (over all transactions in the data files)

Scripts supplied in the distribution perform:

 - validation of a data store
 - migration of data store records to newly configured data store
 - comparison of pre-migration and post-migration data stores

=head1 MOTIVATION

Several factors motivated the development of this module:

 - the desire for simple, efficient reading and writing of records
 - the need to handle any (reasonable) number and size of records
 - the desire to identify records using sequence numbers
 - the need to retain previous versions of records and to view history
 - the ability to store any sort of data: binary or text in any encoding
 - the desire for a relatively simple file structure
 - the desire for the data to be fairly easily read by a human
 - the ability to easily increase the data store size (through migration)

The key file makes it easy and efficient to retrieve the current
version of a record--you just need the record's sequence number.  Other
retrievals via file number and seek position (e.g., gotten from a
history list) are also fast and easy.

Because the size and number of data files is configurable, the
data store should scale up to large numbers of (perhaps large)
records.  This while still retaining efficient reading and writing.

When a record is created, it is assigned a sequence number (keynum)
that persistently identifies that record for the life of the data
store.  This should help user-developed indexing schemes that
employ, e.g., bit maps to remain correct.

Since a record links to it's predecessors, it's easy to get a history
of that record's changes over time.  This can facilitate recovery and
reporting.

Since record retrieval is by seek position and record length (in
bytes), any sequence of bytes may be stored and retrieved.  Disparate
types of data may be stored in the same data store.

Outside of the record data itself, the data store file structure uses
ascii characters for the key file and preambles.  It appends a record
separator, typically a newline character, after each record.  This is
intended to make the file structure relatively simple and more easily
read by a human--to aid copying, debugging, disaster recovery, simple
curiosity, etc.

Migration scripts are included in the module distribution.  If your
initial configuration values prove too small to accommodate your data,
you can configure a new data store with larger values and migrate all
the records to the new data store.  All of the transaction and sequence
numbers remain the same; the record data and user data are identical;
and interfacing with the new data store vs. the old one should be
completely transparent to programs using the FlatFile::DataStore
module.

=head1 VERSION

FlatFile::DataStore version 0.02

=cut

our $VERSION = '0.02';

use 5.008003;
use strict;
use warnings;

use Fcntl qw(:DEFAULT :flock);
use URI;
use URI::Escape;
use Data::Dumper;
use Carp;

use FlatFile::DataStore::Preamble;
use FlatFile::DataStore::Record;
use Math::Int2Base qw( base_chars int2base base2int );
use Data::Omap qw( :ALL );

#---------------------------------------------------------------------
# globals:

# user-supplied attributes that define the preamble ...
my %Optional = qw(
    user         1
    );

my %Preamble = ( %Optional, qw(
    user         1
    indicator    1
    date         1
    keynum       1
    reclen       1
    transnum     1
    thisfilenum  1
    thisseekpos  1
    prevfilenum  1
    prevseekpos  1
    nextfilenum  1
    nextseekpos  1
    ) );

# attributes that we generate (vs. user-supplied)
my %Generated = qw(
    uri          1
    crud         1
    dateformat   1
    specs        1
    regx         1
    preamblelen  1
    filenumlen   1
    filenumbase  1
    translen     1
    transbase    1
    );

# all attributes, including some more user-supplied ones
my %Attrs = ( %Preamble, %Generated, qw(
    name         1
    dir          1
    desc         1
    recsep       1
    maxfilesize  1
    ) );

my $Ascii_chars = qr/^[ -~]+$/;
my( %Read_fh, %Write_fh );  # inside-outish object attributes

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore->new();

Constructs a new FlatFile::DataStore object.

Accepts hash ref giving values for C<dir> and C<name>.

 my $ds = FlatFile::DataStore->new( { dir => $dir, name => $name } );

Returns a reference to the FlatFile::DataStore object.

=cut

#---------------------------------------------------------------------
# new(), called by user to construct a data store object

sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self = $self->init( $parms ) if $parms;  # $self could change ...
    return $self;
}

#---------------------------------------------------------------------
# init(), called by new() to initialize a data store object
#     parms: dir,  the directory where the data store lives
#            name, the name of the data store
#     will look for name.obj or name.uri and load those values

sub init {
    my( $self, $parms ) = @_;

    my $dir  = $parms->{'dir'};
    my $name = $parms->{'name'};
    croak qq/Need "dir" and "name"/
        unless defined $dir and defined $name;

    my $obj_file = "$dir/$name.obj";

    # if database has been initialized, there's an object file
    if( -e $obj_file ) {
        my $obj = $self->read_file( $obj_file );
        $self = eval $obj;  # note: *new* $self
        croak qq/Problem with $obj_file: $@/ if $@;
        $self->dir( $dir );  # dir not in obj_file
    }

    # otherwise read the uri file and initialize the database
    else {
        my $uri_file = "$dir/$name.uri";
        my $uri = $self->read_file( $uri_file );
        chomp $uri;

        $self->uri( $uri );

        my $uri_parms = $self->burst_query();
        for my $attr ( keys %$uri_parms ) {
            croak qq/Unrecognized parameter: "$attr"/ unless $Attrs{ $attr };
            # (using $attr as method name:)
            $self->$attr( $uri_parms->{ $attr } );
        }

        # now for some generated attributes
        my( $len, $base ) = split /-/, $self->thisfilenum();
        $self->filenumlen( $len );
        $self->filenumbase( $base );
        ( $len, $base ) = split /-/, $self->transnum();
        $self->translen( $len );
        $self->transbase( $base );
        $self->dateformat( (split /-/, $self->date())[1] );
        $self->regx( $self->make_preamble_regx() );
        $self->crud( $self->make_crud() );
        $self->maxfilesize( $self->convert_maxfilesize() );
        $self->dir( $dir );  # dir not in uri

        for my $attr ( keys %Attrs ) {
            croak qq/Uninitialized attribute: "$_"/
                if not defined $self->$attr() and not $Optional{ $attr };
        }

        $self->initialize();
    }
    $self->close_files();

    return $self;  # this is either the same self or a new self
}

#---------------------------------------------------------------------
# burst_query(), called by init() to parse the name.uri file
#     also generates values for 'spec' and 'preamblelen'

sub burst_query {
    my( $self ) = @_;

    my $uri   = $self->uri();
    my $query = URI->new( $uri )->query();

    my @pairs = split /[;&]/, $query;
    my $omap  = [];  # psuedo-new(), ordered hash
    my $pos   = 0;
    my %parms;
    for( @pairs ) {
        my( $name, $val ) = split /=/, $_, 2;

        $name = uri_unescape( $name );
        $val  = uri_unescape( $val );

        croak qq/"$name" duplicated in uri/ if $parms{ $name };

        $parms{ $name } = $val;
        if( $Preamble{ $name } ) {
            my( $len, $parm ) = split /-/, $val, 2;
            omap_add( $omap, $name => [ $pos, 0+$len, $parm ] );
            $pos += $len;
        }
    }

    # some attributes are generated here:
    $parms{'specs'}       = $omap;
    $parms{'preamblelen'} = $pos;

    return \%parms;
}

#---------------------------------------------------------------------
# make_preamble_regx(), called by init() to construct a regular
#     expression that should match any record's preamble
#     this regx should capture each fields value

sub make_preamble_regx {
    my( $self ) = @_;

    my $regx = "";
    for( $self->specs() ) {  # specs() returns an array of hashrefs
        my( $key, $aref )       = %$_;
        my( $pos, $len, $parm ) = @$aref;

        for( $key ) {

            # note: user regx must allow only /[ -~]/ (printable ascii)
            # not checked for here (here we just use user-supplied regx),
            # but checked for /[ -~]/ other places
            if( /indicator|user/ ) {
                $regx .= ($len == 1 ? "([$parm])" : "([$parm]{$len})");
            }

            # XXX want better match pattern for date, is there one?
            # as is: 8 decimal digits or 4 base62 digits
            elsif( /date/ ) {
                $regx .= ($len == 8 ? "([0-9]{8})" : "([0-9A-Za-z]{4})");
            }

            # here we get the base characters and compress into ranges
            else {
                my $chars = base_chars( $parm );
                $chars =~ s/([0-9])[0-9]+([0-9])/$1-$2/;
                $chars =~ s/([A-Z])[A-Z]+([A-Z])/$1-$2/;
                $chars =~ s/([a-z])[a-z]+([a-z])/$1-$2/;
                # '-' is 'null' character:
                $regx .= ($len == 1 ? "([-$chars])" : "([-$chars]{$len})");
            }
        }
    }
    return qr($regx);
}

#---------------------------------------------------------------------
# make_crud(), called by init() to construct a hash of indicators
#     CRUD indicators: Create, Retrieve, Update, Delete
#     the following are suggested, but configurable in uri
#         + Create
#         # Old Update (old record flagged as updated)
#         = Update
#         * Old Delete (old record flagged as deleted)
#         - Delete
#     (no indicator for Retrieve, n/a--but didn't want to say CUD)

sub make_crud {
    my( $self ) = @_;

    my( $len, $chars ) = split /-/, $self->indicator(), 2;
    croak qq/Only single-character indicators supported/
        if $len != 1;

    my @c = split //, $chars;
    my %c = map { $_ => 1 } @c;
    my @n = keys %c;
    croak qq/Need five unique indicator characters/
        if @n != 5 or @c != 5;

    my %crud;
    @crud{ qw( create oldupd update olddel delete ) } = @c;
    return \%crud;
}

#---------------------------------------------------------------------
# convert_maxfilesize(), called by init() to convert user-supplied
#     maxfilesize value into an integer: one can say, "500_000_000",
#     "500M", or ".5G" to mean 500,000,000 bytes

sub convert_maxfilesize {
    my( $self ) = @_;

    # ignoring M/G ambiguities and using round numbers:
    my %sizes = (
        M => 10**6,
        G => 10**9,
        );

    my $max = $self->maxfilesize();
    $max =~ s/_//g;
    if( $max =~ /^([.0-9]+)([MG])/ ) {
        my( $n, $s ) = ( $1, $2 );
        $max = $n * $sizes{ $s };
    }

    return 0+$max;
}

#---------------------------------------------------------------------
# CRUD cases:
#
# Create: no previous preamble required or allowed
#     - create a record object (with no previous)
#     - write the record
#     - return the record object
# Retrieve:
#     - read a data record
#     - create a record object (with a preamble, which may become a previous)
#     - return the record object
# Update: previous preamble required (and it must not have changed)
#     - create a record object (with a previous preamble)
#     - write the record (updating the previous in the data store)
#     - return the record object
# Delete: previous preamble required (and it must not have changed)
#     - create a record object (with a previous preamble)
#     - write the record (updating the previous in the data store)
#     - return the record object
#
# Some notes about the previous preamble:
#
# In order to protect data from conflicting concurrent updates, you may
# not update or delete a record without first retrieving it from the data
# store.  Supplying the previous preamble along with the new record data is
# reasonable proof that you did this.  Before the new record is written,
# the supplied previous preamble is compared with what's in the data store,
# and if they are not exactly the same, it means that someone else
# retrieved and updated/deleted the record between the time you read it
# and the time you tried to update/delete it.
#
# So unless you supply a previous preamble and unless the one you supply
# matches exactly the one in the data store, your update/delete will not
# be accepted--you will have to re-retrieve the new version of the record
# (getting a more recent preamble) and apply your updates to it.

#---------------------------------------------------------------------

=head1 OBJECT METHODS, RECORD PROCESSING (C.R.U.D.)

=head2 create( $record_data, [$user_data] )

Creates a record.  The parm C<$record_data> may be one of

 - data string
 - scalar reference (to the data string)
 - FlatFile::DataStore::Record object

The parm C<$user_data> may be omitted if C<$record_data> is an object,
in which case the user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

Note: the record data (but not user data) is stored in the FF::DS:Record
object as a scalar reference.  This is done for efficiency in the cases
where the record data may be very large.  Likewise, the first parm to
create() may be a scalar reference for the same reason.

=cut

sub create {
    my( $self, $record_data, $user_data ) = @_;

    # study parms for alternative calling schemes
    my $data_ref;
    if( my $reftype = ref $record_data ) {
        for( $reftype ) {
            if( /SCALAR/ ) { $data_ref = $record_data; }
            elsif( /FlatFile::DataStore::Record/ ) {
                $data_ref = $record_data->data();    
                $user_data = $record_data->user()
                    unless defined $user_data;
            }
            else { croak qq/Unrecognized ref type: $reftype/; }
        }
    }
    else {
        $data_ref = \$record_data;
    }

    # collect some magic values
    my $indicator = $self->crud()->{'create'};
    my $date      = now( $self->dateformat() );

    my $dir     = $self->dir();
    my $name    = $self->name();
    my $keyfile = "$dir/$name.key";

    # need to lock files before checking sizes
    my $keyfh   = $self->locked_for_write( $keyfile );
    my $keynum  = $self->nextkeynum( $keyfile );
    my $keypos  = -s $keyfile;  # seekpos into keyfile

    # want to lock keyfile before datafile
    my $reclen                 = length $$data_ref;
    my( $datafile, $filenum )  = $self->current_datafile( $reclen );
    my $datafh                 = $self->locked_for_write( $datafile );
    my $datapos                = -s $datafile;  # seekpos into datafile
    my( $transint, $transnum ) = $self->nexttransnum( $datafh );

    # use these magic values to create a record
    my $preamble_parms = {
        indicator   => $indicator,
        date        => $date,
        transnum    => $transint,
        keynum      => $keynum,
        reclen      => $reclen,
        thisfilenum => $filenum,
        thisseekpos => $datapos,
        };
    $preamble_parms->{ user } = $user_data
        if defined $user_data;

    my $record = $self->new_record( {
        preamble => $preamble_parms,
        data     => $data_ref,
        } );

    # commence to writin' ...
    my $preamble = $record->string();
    my $recsep   = $self->recsep();
    my $dataline = "$preamble$$data_ref$recsep";
    my $keyline  = "$preamble$recsep";

    seek $datafh, $datapos, 0;
    print $datafh $dataline or croak "Can't write $datafile: $!";
    my $datatell = tell $datafh;

    # "belt and suspenders" ...
    if( $datapos + length $dataline ne $datatell ) {
        croak "Bad write?: $datafile: things don't add up";
    }

    $self->write_transnum( $datafh, $transnum );

    seek $keyfh, $keypos, 0;
    print $keyfh $keyline or croak "Can't write $keyfile: $!";
    my $keytell = tell $keyfh;

    if( $keypos + length $keyline ne $keytell ) {
        croak "Bad write?: $keyfile: things don't add up";
    }

    return $record;
}

#---------------------------------------------------------------------

=head2 retrieve( $num, [$pos] )

Retrieves a record.  The parm C<$num> may be one of

 - a key number, i.e., record sequence number
 - a file number

The parm C<$pos> is required if C<$num> is a file number.

Returns a Flatfile::DataStore::Record object.

=cut

sub retrieve {
    my( $self, $num, $pos ) = @_;

    my $preamblelen = $self->preamblelen();

    my $filenum;
    my $seekpos;
    my $keystring;

    if( defined $pos ) {
        $filenum = $num;
        $seekpos = $pos;
    }
    else {
        my $keynum     = $num;
        my $recsep     = $self->recsep();
        my $keyseekpos = $keynum * ($preamblelen + length $recsep);

        my $dir     = $self->dir();
        my $name    = $self->name();
        my $keyfile = "$dir/$name.key";
        my $keyfh   = $self->locked_for_read( $keyfile );

        my $trynum  = $self->nextkeynum( $keyfile );
        croak qq/Record doesn't exist: "$keynum"/
            if $keynum >= $trynum;

        $keystring = $self->read_preamble( $keyfh, $keyseekpos );
        my $parms  = $self->burst_preamble( $keystring );

        $filenum = $parms->{'thisfilenum'};
        $seekpos = $parms->{'thisseekpos'};
    }

    my $datafile = $self->which_datafile( $filenum );
    my $datafh   = $self->locked_for_read( $datafile );
    my $string   = $self->read_preamble( $datafh, $seekpos );

    croak qq/Mismatch [$string] [$keystring]/
        if $keystring and $string ne $keystring;

    my $preamble = $self->new_preamble( { string => $string } );

    $seekpos   += $preamblelen;
    my $reclen  = $preamble->reclen();
    my $recdata = $self->read_bytes( $datafh, $seekpos, $reclen ); 

    my $record = $self->new_record( {
        preamble => $preamble,
        data     => \$recdata,
        } );

    return $record;
}

#---------------------------------------------------------------------

=head2 update( $object_or_string, [$record_data], [$user_data] )

Updates a record.  The parm $object_or_string may be one of:

 - FlatFile::DataStore::Record object
 - FlatFile::DataStore::Preamble object
 - Preamble string

The parms C<$record_data> and C<$user_data> may be omitted only if
C<$object_or_string> is a FF::DS::Record object, in which case the
record and user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

=cut

# XXX shares way too much code with delete()

sub update {
    my( $self, $obj, $record_data, $user_data ) = @_;

    # get preamble string and keynum from object
    my $prevpreamble;
    my $keynum;
    my $prevind;
    my $prevfilenum;
    my $prevseekpos;
    my $data_ref;
    if( my $reftype = ref $obj ) {
        $prevpreamble = $obj->string();
        $keynum       = $obj->keynum();
        $prevind      = $obj->indicator();
        $prevfilenum  = $obj->thisfilenum();
        $prevseekpos  = $obj->thisseekpos();
        if( $reftype eq "FlatFile::DataStore::Record" ) {
            $data_ref  = $obj->data() unless defined $record_data;
            $user_data = $obj->user() unless defined $user_data;
        }
    }
    else {
        $prevpreamble = $obj;
        my $parms     = $self->burst_preamble( $prevpreamble );
        $keynum       = $parms->{'keynum'};
        $prevind      = $parms->{'prevind'};
        $prevfilenum  = $parms->{'thisfilenum'};
        $prevseekpos  = $parms->{'thisseekpos'};
    }
    # preamble is sentinel for success
    croak qq/Bad call to update()/ unless $prevpreamble;

    my $create = $self->crud()->{'create'};
    my $update = $self->crud()->{'update'};
    my $delete = $self->crud()->{'delete'};
    my $regx   = qr/[$create$update$delete]/;
    croak qq/Update not allowed: "$prevind"/
        unless $prevind =~ $regx;

    # study parms for alternative calling schemes
    unless( $data_ref ) {
        if( my $reftype = ref $record_data ) {
            if( $reftype eq "SCALAR" ) {
                $data_ref = $record_data;
            }
            elsif( $reftype eq "FlatFile::DataStore::Record" ) {
                $data_ref = $record_data->data();    
                $user_data = $record_data->user()
                    unless defined $user_data;
            }
            else {
                croak qq/Unrecognized ref type: $reftype/;
            }
        }
        else {
            $data_ref = \$record_data;
        }
    }

    # collect some magic values
    my $indicator = $self->crud()->{'update'};
    my $date      = now( $self->dateformat() );

    my $dir     = $self->dir();
    my $name    = $self->name();
    my $keyfile = "$dir/$name.key";

    # need to lock files before getting info from them
    my $keyfh   = $self->locked_for_write( $keyfile );
    my $keypos  = $self->keypos( $keynum );

    my $try = $self->read_preamble( $keyfh, $keypos );
    croak qq/Mismatch [$try] [$prevpreamble]/
        unless $try eq $prevpreamble;

    # want to lock datafile after keyfile
    my $reclen                 = length $$data_ref;
    my( $datafile, $filenum )  = $self->current_datafile( $reclen );
    my $datafh                 = $self->locked_for_write( $datafile );
    my $datapos                = -s $datafile;  # seekpos into datafile
    my( $transint, $transnum ) = $self->nexttransnum( $datafh );

    # use these magic values to create an update record
    my $preamble_parms = {
        indicator   => $indicator,
        date        => $date,
        transnum    => $transint,
        keynum      => $keynum,
        reclen      => $reclen,
        thisfilenum => $filenum,
        thisseekpos => $datapos,
        prevfilenum => $prevfilenum,
        prevseekpos => $prevseekpos,
        };
    $preamble_parms->{ user } = $user_data
        if defined $user_data;

    my $record = $self->new_record( {
        preamble => $preamble_parms,
        data     => $data_ref,
        } );

    # commence to writin' ...
    my $preamble = $record->string();
    my $recsep   = $self->recsep();
    my $dataline = "$preamble$$data_ref$recsep";

    seek $datafh, $datapos, 0;
    print $datafh $dataline or croak "Can't write $datafile: $!";
    my $datatell = tell $datafh;

    # "belt and suspenders" ...
    if( $datapos + length $dataline ne $datatell ) {
        croak qq/Bad write?: $datafile: things don't add up/;
    }

    $self->write_transnum( $datafh, $transnum );
    $self->write_bytes( $keyfh, $keypos, $preamble );

    # update the old preamble
    $prevpreamble = $self->update_preamble( $prevpreamble, {
        indicator   => $self->crud()->{'oldupd'},
        nextfilenum => $filenum,
        nextseekpos => $datapos,
        } );
    my $prevdatafile = $self->which_datafile( $prevfilenum );
    my $prevdatafh   = $self->locked_for_write( $prevdatafile );
    $self->write_bytes( $prevdatafh, $prevseekpos, $prevpreamble );

    return $record;
}

#---------------------------------------------------------------------

=head2 delete( $object_or_string, [$record_data], [$user_data] )

Deletes a record.  The parm $object_or_string may be one of:

 - FlatFile::DataStore::Record object
 - FlatFile::DataStore::Preamble object
 - Preamble string

The parms C<$record_data> and C<$user_data> may be omitted only if
C<$object_or_string> is a FF::DS::Record object, in which case the
record and user data will be gotten from it.

Returns a Flatfile::DataStore::Record object.

=cut

# XXX shares way too much code with update()

sub delete {
    my( $self, $obj, $record_data, $user_data ) = @_;

    # get preamble string and keynum from object
    my $prevpreamble;
    my $keynum;
    my $prevind;
    my $prevfilenum;
    my $prevseekpos;
    my $data_ref;
    if( my $reftype = ref $obj ) {
        $prevpreamble = $obj->string();
        $keynum       = $obj->keynum();
        $prevind      = $obj->indicator();
        $prevfilenum  = $obj->thisfilenum();
        $prevseekpos  = $obj->thisseekpos();
        if( $reftype eq "FlatFile::DataStore::Record" ) {
            $data_ref  = $obj->data() unless defined $record_data;
            $user_data = $obj->user() unless defined $user_data;
        }
    }
    else {
        $prevpreamble = $obj;
        my $parms     = $self->burst_preamble( $prevpreamble );
        $keynum       = $parms->{'keynum'};
        $prevind      = $parms->{'prevind'};
        $prevfilenum  = $parms->{'thisfilenum'};
        $prevseekpos  = $parms->{'thisseekpos'};
    }
    # $prevpreamble is sentinel for success
    croak qq/Bad call to delete()/ unless $prevpreamble;

    my $create = $self->crud()->{'create'};
    my $update = $self->crud()->{'update'};
    my $regx   = qr/[$create$update]/;
    croak qq/Delete not allowed: "$prevind"/
        unless $prevind =~ $regx;

    # study parms for alternative calling schemes
    unless( $data_ref ) {
        if( my $reftype = ref $record_data ) {
            if( $reftype eq "SCALAR" ) {
                $data_ref = $record_data;
            }
            elsif( $reftype eq "FlatFile::DataStore::Record" ) {
                $data_ref = $record_data->data();    
                $user_data = $record_data->user()
                    unless defined $user_data;
            }
            else {
                croak qq/Unrecognized ref type: $reftype/;
            }
        }
        else {
            $data_ref = \$record_data;
        }
    }

    # collect some magic values
    my $indicator = $self->crud()->{'delete'};
    my $date      = now( $self->dateformat() );

    my $dir     = $self->dir();
    my $name    = $self->name();
    my $keyfile = "$dir/$name.key";

    # need to lock files before getting info from them
    my $keyfh   = $self->locked_for_write( $keyfile );
    my $keypos  = $self->keypos( $keynum );

    my $try = $self->read_preamble( $keyfh, $keypos );
    croak qq/Mismatch [$try] [$prevpreamble]/
        unless $try eq $prevpreamble;

    # want to lock datafile after keyfile
    my $reclen                 = length $$data_ref;
    my( $datafile, $filenum )  = $self->current_datafile( $reclen );
    my $datafh                 = $self->locked_for_write( $datafile );
    my $datapos                = -s $datafile;  # seekpos into datafile
    my( $transint, $transnum ) = $self->nexttransnum( $datafh );

    # use these magic values to create an update record
    my $preamble_parms = {
        indicator   => $indicator,
        date        => $date,
        transnum    => $transint,
        keynum      => $keynum,
        reclen      => $reclen,
        thisfilenum => $filenum,
        thisseekpos => $datapos,
        prevfilenum => $prevfilenum,
        prevseekpos => $prevseekpos,
        };
    $preamble_parms->{ user } = $user_data
        if defined $user_data;

    my $record = $self->new_record( {
        preamble => $preamble_parms,
        data     => $data_ref,
        } );

    # commence to writin' ...
    my $preamble = $record->string();
    my $recsep   = $self->recsep();
    my $dataline = "$preamble$$data_ref$recsep";

    seek $datafh, $datapos, 0;
    print $datafh $dataline or croak "Can't write $datafile: $!";
    my $datatell = tell $datafh;

    # "belt and suspenders" ...
    if( $datapos + length $dataline ne $datatell ) {
        croak qq/Bad write?: $datafile: things don't add up/;
    }

    $self->write_transnum( $datafh, $transnum );
    $self->write_bytes( $keyfh, $keypos, $preamble );

    # update the old preamble
    $prevpreamble = $self->update_preamble( $prevpreamble, {
        indicator   => $self->crud()->{'olddel'},
        nextfilenum => $filenum,
        nextseekpos => $datapos,
        } );
    my $prevdatafile = $self->which_datafile( $prevfilenum );
    my $prevdatafh   = $self->locked_for_write( $prevdatafile );
    $self->write_bytes( $prevdatafh, $prevseekpos, $prevpreamble );

    return $record;
}

#---------------------------------------------------------------------

=head2 history( $keynum )

Retrieves a record's history.  The parm C<$keynum> is always a key
number, i.e., a record sequence number.

Returns an array of FlatFile::DataStore::Record objects.

The first element of this array is the current record.  The last
element is the original record.  That is, the array is in reverse
chronological order.

=cut

sub history {
    my( $self, $keynum ) = @_;

    my @history;

    my $rec = $self->retrieve( $keynum );
    push @history, $rec;

    my $prevfilenum = $rec->prevfilenum();
    my $prevseekpos = $rec->prevseekpos();

    while( $prevfilenum ) {

        my $rec = $self->retrieve( $prevfilenum, $prevseekpos );
        push @history, $rec;

        $prevfilenum = $rec->prevfilenum();
        $prevseekpos = $rec->prevseekpos();
    }

    return @history;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, ACCESSORS

=head2 $ds->specs( [$omap] )

Sets and returns the C<specs> attribute value if C<$omap> is given,
otherwise just returns the value.

An 'omap' is an ordered hash as defined in

 http://yaml.org/type/omap.html

That is, it's an array of single-key hashes.  This ordered hash
contains the specifications for constructing and parsing a record
preamble as defined in the name.uri file.

=cut

sub specs {
    my( $self, $omap ) = @_;
    for( $self->{specs} ) {
        if( $omap ) {
            croak qq/Invalid omap: /.omap_errstr()
                unless omap_is_valid( $omap );
            $_ = $omap;
        }
        return unless defined;
        return @$_ if wantarray;
        return $_;
    }
}

#---------------------------------------------------------------------

=head2 $ds->dir( [$dir] )

Sets and returns the C<dir> attribute value if C<$dir> is given,
otherwise just returns the value.

If C<$dir> is given, the directory must already exist.

=cut

sub dir {
    my( $self, $dir ) = @_;
    if( defined $dir and $dir eq "" ) { delete $self->{dir} }
    else {
        for( $self->{dir} ) {
            if( defined $dir ) {
                croak qq/$dir doesn't exist/ unless -d $dir;
                $_ = $dir
            }
            return $_;
        }
    }
}

#---------------------------------------------------------------------

=head2 Preamble accessors

The following methods set and return their respective attribute values
if C<$value> is given.  Otherwise, they just return the value.

 $ds->indicator(   [$value] ); # from uri (length-characters)
 $ds->date(        [$value] ); # from uri (length-format)
 $ds->transnum(    [$value] ); # from uri (length-base)
 $ds->keynum(      [$value] ); # from uri (length-base)
 $ds->reclen(      [$value] ); # from uri (length-base)
 $ds->thisfilenum( [$value] ); # from uri (length-base)
 $ds->thisseekpos( [$value] ); # from uri (length-base)
 $ds->prevfilenum( [$value] ); # from uri (length-base)
 $ds->prevseekpos( [$value] ); # from uri (length-base)
 $ds->nextfilenum( [$value] ); # from uri (length-base)
 $ds->nextseekpos( [$value] ); # from uri (length-base)
 $ds->user(        [$value] ); # from uri (length-characters)

=head2 Other accessors

 $ds->name(        [$value] ); # from uri, name of data store
 $ds->desc(        [$value] ); # from uri, description of data store
 $ds->recsep(      [$value] ); # from uri (character(s))
 $ds->uri(         [$value] ); # full uri as is
 $ds->preamblelen( [$value] ); # length of full preamble string
 $ds->translen(    [$value] ); # length of stored transaction number
 $ds->transbase(   [$value] ); # base of stored trancation number
 $ds->filenumlen(  [$value] ); # length of stored file number
 $ds->filenumbase( [$value] ); # base of stored file number
 $ds->dateformat(  [$value] ); # format from uri
 $ds->regx(        [$value] ); # capturing regx for preamble string
 $ds->maxfilesize( [$value] ); # max as an integer
 $ds->crud(        [$value] ); # hash ref, e.g.,

     {
        create => '+',
        oldupd => '#',
        update => '=',
        olddel => '*',
        delete => '-'
     }

 (translates logical actions into their symbolic indicators)

=cut

sub indicator   {for($_[0]->{indicator}   ){$_=$_[1]if@_>1;return$_}}
sub date        {for($_[0]->{date}        ){$_=$_[1]if@_>1;return$_}}
sub transnum    {for($_[0]->{transnum}    ){$_=$_[1]if@_>1;return$_}}
sub keynum      {for($_[0]->{keynum}      ){$_=$_[1]if@_>1;return$_}}
sub reclen      {for($_[0]->{reclen}      ){$_=$_[1]if@_>1;return$_}}
sub thisfilenum {for($_[0]->{thisfilenum} ){$_=$_[1]if@_>1;return$_}}
sub thisseekpos {for($_[0]->{thisseekpos} ){$_=$_[1]if@_>1;return$_}}
sub prevfilenum {for($_[0]->{prevfilenum} ){$_=$_[1]if@_>1;return$_}}
sub prevseekpos {for($_[0]->{prevseekpos} ){$_=$_[1]if@_>1;return$_}}
sub nextfilenum {for($_[0]->{nextfilenum} ){$_=$_[1]if@_>1;return$_}}
sub nextseekpos {for($_[0]->{nextseekpos} ){$_=$_[1]if@_>1;return$_}}
sub user        {for($_[0]->{user}        ){$_=$_[1]if@_>1;return$_}}

sub name        {for($_[0]->{name}        ){$_=$_[1]if@_>1;return$_}}
sub desc        {for($_[0]->{desc}        ){$_=$_[1]if@_>1;return$_}}
sub recsep      {for($_[0]->{recsep}      ){$_=$_[1]if@_>1;return$_}}
sub uri         {for($_[0]->{uri}         ){$_=$_[1]if@_>1;return$_}}
sub preamblelen {for($_[0]->{preamblelen} ){$_=$_[1]if@_>1;return$_}}
sub translen    {for($_[0]->{translen}    ){$_=$_[1]if@_>1;return$_}}
sub transbase   {for($_[0]->{transbase}   ){$_=$_[1]if@_>1;return$_}}
sub filenumlen  {for($_[0]->{filenumlen}  ){$_=$_[1]if@_>1;return$_}}
sub filenumbase {for($_[0]->{filenumbase} ){$_=$_[1]if@_>1;return$_}}
sub dateformat  {for($_[0]->{dateformat}  ){$_=$_[1]if@_>1;return$_}}
sub regx        {for($_[0]->{regx}        ){$_=$_[1]if@_>1;return$_}}
sub maxfilesize {for($_[0]->{maxfilesize} ){$_=$_[1]if@_>1;return$_}}
sub crud        {for($_[0]->{crud}        ){$_=$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------

=head1 OBJECT METHODS, UTILITARIAN

=cut

#---------------------------------------------------------------------
# initialize(), called by init() when datastore is first used
#     creates name.obj file to bypass uri parsing from now on

sub initialize {
    my( $self ) = @_;

    my $dir      = $self->dir();
    my $name     = $self->name();
    my $len      = $self->filenumlen();
    my $filenum  = sprintf "%0${len}d", 1;  # one is 1 in any base
    my $datafile = "$dir/$name.$filenum.dat";

    croak qq/Can't initialize database: data files exist (e.g., $datafile)./
        if -e $datafile;

    local $Data::Dumper::Terse  = 1;
    local $Data::Dumper::Indent = 0;  # make object a one-liner

    my $save = $self->dir();
    # delete these, don't want in obj file
    $self->dir("");

    my $obj_file = "$dir/$name.obj";
    $self->write_file( $obj_file, Dumper $self );
    $self->close_files();

    $self->dir( $save );
}

#---------------------------------------------------------------------
# new_preamble(), called by various subs
#     wrapper for FlatFile::DataStore::Preamble->new()

sub new_preamble {
    my( $self, $parms ) = @_;
    $parms->{'datastore'} = $self;
    FlatFile::DataStore::Preamble->new( $parms );
}

#---------------------------------------------------------------------
# new_record(), called by various subs
#     wrapper for FlatFile::DataStore::Record->new()

sub new_record {
    my( $self, $parms ) = @_;
    my $preamble = $parms->{'preamble'};
    if( ref $preamble eq 'HASH' ) {  # not an object
        $parms->{'preamble'} = $self->new_preamble( $preamble );
    }
    FlatFile::DataStore::Record->new( $parms );
}

#---------------------------------------------------------------------

=head2 all_datafiles()

Returns an array of all the data store's data file paths.

Since the file numbers should sort well, the array should be in
chronological order.

=cut

sub all_datafiles {
    my( $self ) = @_;

    my $dir        = $self->dir();
    my $name       = $self->name();
    my $filenumlen = $self->filenumlen();

    my $base = $self->filenumbase();
    my $chars = base_chars( $base );
    my $pattern = "[$chars]" x $filenumlen;

    my @files = glob "$dir/$name.$pattern.dat";

    return @files;
}

#---------------------------------------------------------------------
# current_datafile(), called by create(), update(), and delete() to
#     get the current data file; the parm $reclen is used to see if
#     new_datafile() needs to be called

sub current_datafile {
    my( $self, $reclen ) = @_;

    $reclen ||= 0;

    my $dir        = $self->dir();
    my $name       = $self->name();
    my $filenumlen = $self->filenumlen();
    my $recseplen  = length( $self->recsep() );
    my @files      = $self->all_datafiles();

    my $datafile;
    my $filenum  = 0;  # zero is 0 in any base
    my $transint;
    my $transnum;
    if( @files ) {
        # get the last data file, i.e., the current one
        $datafile    = $files[-1];
        ( $filenum ) = $datafile =~ m{^$dir/$name\.(.+)\.dat$};
        my $datafh   = $self->locked_for_write( $datafile );
        ( $transint, $transnum ) = $self->nexttransnum( $datafh );
    }
    else {
        # create first data file if none exists
        $transint = 1;
        ( $datafile, $filenum ) =
            $self->new_datafile( $filenum, $transint )
    }

    # check if we're about to overfill the data file
    # and if so, create a new data file--the new current one
    my $maxfilesize = $self->maxfilesize();
    my $checksize = $self->preamblelen() + $reclen + $recseplen;

    if( (-s $datafile) + $checksize > $maxfilesize ) {

        # head is:
        #
        # uri: [uri][recsep]                    5 + urilen         + recseplen
        # file: [filenum] of [filenum][recsep] 10 + (2*filenumlen) + recseplen
        # trans: [transnum] to [transnum][recsep]    11 + (2*translen)   + recseplen
        #
        # so headsize is:
        #
        # 26 + urilen + (2*filenumlen) + (2*translen) + (3*recseplen)

        my $headsize = 26
            + length( $self->uri() )
            + ( 2 * $filenumlen )
            + ( 2 * $self->translen() )
            + ( 3 * $recseplen );
        croak qq/Record too long/
            if $headsize + $checksize > $maxfilesize;
        ( $datafile, $filenum ) =
            $self->new_datafile( $filenum, $transint );
    }

    return $datafile, $filenum;
}

#---------------------------------------------------------------------
# increment_base(), adds one to a number in any base and returns the
#     incremented number
#     XXX not actually used by anything yet

sub increment_base {
    my( $num, $base ) = @_;
    int2base( 1 + base2int( $num, $base ), $base );
}

#---------------------------------------------------------------------
# new_datafile(), called by current_datafile() to create a new current
#     data file when a) this is the first data file in a newly created
#     data store or b) the record being written would make the current
#     file exceed the max file size

sub new_datafile {
    my( $self, $old_filenum, $transint ) = @_;

    my $filenumlen  = $self->filenumlen();
    my $dir         = $self->dir();
    my $name        = $self->name();
    my $translen    = $self->translen();
    my $transbase   = $self->transbase();
    my $trans_is    = int2base( $transint,   $transbase, $translen );
    my $trans_was   = int2base( $transint-1, $transbase, $translen );
    my $filenumbase = $self->filenumbase();
    my $filenumint  = base2int( $old_filenum, $filenumbase );
    my $filenum     = int2base( ++$filenumint, $filenumbase, $filenumlen );
    croak qq/Database exceeds configured size (filenum: "$filenum" too long)/
        if length $filenum > $filenumlen;

    my $datafile = "$dir/$name.$filenum.dat";

    # initialize each data file with the datastore uri
    # to help identify them and tie them together logically
    # we also add file: X of Y to help disaster recovery
    # and trans: A to B for similar reasons

    my $recsep     = $self->recsep();
    my $uri        = "uri: ".$self->uri().$recsep;
    my $files      = "file: $filenum of $filenum$recsep";
    # trans_was because of how we get the nexttransnum ...
    my $transrange = "trans: $trans_is to $trans_was$recsep";
    $self->write_file( $datafile, "$uri$files$transrange" );

    # now update the files verbage in the previous data files
    my $seekpos = length $uri;
    for( 1 .. $filenumint-1 ) {
        my $this     = int2base( $_, $filenumbase, $filenumlen );
        my $datafile = "$dir/$name.$this.dat";
        my $fh       = $self->locked_for_write( $datafile );
        my $files    = "file: $this of $filenum";
        $self->write_bytes( $fh, $seekpos, $files );
    }
    $self->close_files();

    return $datafile, $filenum;
}

#---------------------------------------------------------------------

=head2 keyfile()

Returns the file path of the data store's key file.

=cut

sub keyfile {
    my( $self ) = @_;

    my $dir     = $self->dir();
    my $name    = $self->name();
    my $keyfile = "$dir/$name.key";

    return $keyfile;
}

#---------------------------------------------------------------------

=head2 which_datefile( $filenum )

The parm C<$filenum> should be passed as it is stored in the preamble,
not as a decimal integer.  E.g., if the file number is 10 and the base
is greater than 10, then C<$filenum> should be C<"A">.

Returns the data file path that corresponds to the requested file
number.

=cut

sub which_datafile {
    my( $self, $filenum ) = @_;

    my $dir        = $self->dir();
    my $name       = $self->name();
    my $datafile   = "$dir/$name.$filenum.dat";

    return $datafile;
}

#---------------------------------------------------------------------

=head2 howmany()

Returns the number of current (non-deleted) records in the data store.
This routine's speed is a function of the number of records in the data
store.  It scans the key file, so the more records, the more lines to
scan.  A more efficient way to get this data is to maintain a separate
index as records are created, updated, deleted (outside the scope of
this module--but facilitated by it).

=cut

sub howmany {
    my( $self ) = @_;

    my $create = $self->crud()->{'create'};
    my $update = $self->crud()->{'update'};
    my $regx   = qr/[$create$update]/;

    my $dir     = $self->dir();
    my $name    = $self->name();
    my $keyfile = "$dir/$name.key";
    return unless -e $keyfile;
                       # XXX
$self->close_files();  # XXX needs figuring out ...
                       # XXX
    my $keyfh   = $self->locked_for_read( $keyfile );

    # brute force scan of keyfile
    my $keynum = 0;
    my $keyvec = "";  # bit vector for keynums
    while( <$keyfh> ) {
        chomp;
        my $parms = $self->burst_preamble( $_ );
        setbit( $keyvec, $keynum, 1 ) if $parms->{'indicator'} =~ $regx;
        $keynum++;
    }
    return @{bit2num( $keyvec )} if wantarray;
    return bitcount( $keyvec, 1 );
}

#---------------------------------------------------------------------
# keypos(), called various places to seek to a particular line in the
#     key file

# keynum preamble lines
# 0      preamble1
# 1      preamble2
# 2      preamble3
#        ^ keypos for keynum 2
# 
# For example, if keynum is 2, then keypos is 2 * preamble line length,
# which places it just before the third preamble.

sub keypos {
    my( $self, $keynum ) = @_;
    
    my $preamblelen = $self->preamblelen();
    my $recsep      = $self->recsep();
    my $keypos      = $keynum * ($preamblelen + length $recsep);

    return $keypos;
}

#---------------------------------------------------------------------
# nextkeynum(), called by create() to get next record sequence number
#     and by retrieve() to check if requested number exists

# Since the key file consists of fixed-length records, dividing the
# size of the file by the length of each record should give us the
# number of records in the file.  Since the keynums start with 0,
# the number of records in the file is also the next available keynum,
# e.g.,
#
#     keynum record
#     0      rec1
#     1      rec2
#     2      rec3
#
# This shows that there are three records in the file and the next
# available keynum is 3.
#
# Each record in the key file consists of a preamble and a record
# separator, so we divide the file size by the lengths of those.

sub nextkeynum {
    my( $self, $key_file ) = @_;

    my $preamblelen = $self->preamblelen();
    my $recsep      = $self->recsep();
    my $keynum      = (-s $key_file) / ($preamblelen + length $recsep);

    return $keynum;
}

#---------------------------------------------------------------------
# nexttransnum(), called various places to get the next transaction
#     number from the current data file

# head is:
#
# uri: [uri][recsep]                       5 + urilen         + recseplen
# file: [filenum] of [filenum][recsep]    10 + (2*filenumlen) + recseplen
# trans: [transnum] to [transnum][recsep] 11 + (2*translen)   + recseplen
#                      ^
# so seekpos to 2nd transnum is:
#
# 26 + urilen + (2*filenumlen) + (2*recseplen) + translen
#

sub nexttransnum {
    my( $self, $datafh ) = @_;

    my $urilen     = length( $self->uri() );
    my $recseplen  = length( $self->recsep() );
    my $filenumlen = $self->filenumlen();
    my $translen   = $self->translen();
    my $transbase  = $self->transbase();
    my $seekpos    =
        26 + $urilen + (2*$filenumlen) + (2*$recseplen) + $translen;
    my $transnum   = $self->read_bytes( $datafh, $seekpos, $translen );
    my $transint   = base2int( $transnum, $transbase );
    ++$transint;
    $transnum = int2base( $transint, $transbase, $translen );

    return $transint, $transnum;
}

#---------------------------------------------------------------------
# write_transnum(), called by create(), update(), delete() to update
#     the transaction number in the head of the current data file

sub write_transnum {
    my( $self, $datafh, $transnum ) = @_;

    my $urilen     = length( $self->uri() );
    my $recseplen  = length( $self->recsep() );
    my $filenumlen = $self->filenumlen();
    my $translen   = $self->translen();
    my $seekpos    =
        26 + $urilen + (2*$filenumlen) + (2*$recseplen) + $translen;

    $self->write_bytes( $datafh, $seekpos, $transnum );
}

#---------------------------------------------------------------------
# burst_pramble(), called various places to parse preamble string

sub burst_preamble {
    my( $self, $string ) = @_;
    croak qq/No preamble to burst/ unless $string;

    my @fields = $string =~ $self->regx();
    croak qq/Something is wrong with "$string"/ unless @fields;

    my %parms;
    my $i;
    for( $self->specs() ) {  # specs() returns an array of hashrefs
        my( $key, $aref )       = %$_;
        my( $pos, $len, $parm ) = @$aref;
        my $field = $fields[ $i++ ];
        for( $key ) {
            if( /indicator|date|user/ ) {
                $parms{ $key } = $field;
            }
            elsif( /filenum/ ) {
                next if $field =~ /^-+$/;
                $parms{ $key } = $field;
            }
            else {
                next if $field =~ /^-+$/;
                $parms{ $key } = base2int( $field, $parm );
            }
        }
    }
    return \%parms;
}

#---------------------------------------------------------------------
# update_preamble(), called by update() and delete() to flag old recs

sub update_preamble {
    my( $self, $preamble, $parms ) = @_;

    my $omap = $self->specs();

    for( keys %$parms ) {

        my $value = $parms->{ $_ };
        my( $pos, $len, $parm ) = @{omap_get_values( $omap, $_ )};

        my $try;
        if( /indicator|date|user/ ) {
            $try = sprintf "%-${len}s", $value;
            croak qq/Invalid value for "$_" ($try)/
                unless $try =~ $Ascii_chars;
        }
        # the filenums should be in their base form already
        elsif( /filenum/ ) {
            $try = sprintf "%0${len}s", $value;
        }
        else {
            $try = sprintf "%0${len}s", int2base( $value, $parm );
        }
        croak qq/Value of "$_" ($try) too long/ if length $try > $len;

        substr $preamble, $pos, $len, $try;  # update the field
    }

    croak qq/Something is wrong with preamble: "$preamble"/
        unless $preamble =~ $self->regx();

    return $preamble;
}

#---------------------------------------------------------------------
# analyze_preamble() (XXX will probably go away)

sub analyze_preamble {
    my( $self, $preamble ) = @_;
    return unless $preamble;

    my @fields = $preamble =~ $self->regx();
    croak qq/Something is wrong with "$preamble"/ unless @fields;

    my $parsed = "[" . join( "][", @fields ) . "]";

    my $omap = $self->specs();
    my @keys = omap_get_keys( $omap );

    my $omap_out = [];
    for my $i ( 0 .. $#keys ) {
        omap_add( $omap_out, $keys[ $i ], $fields[ $i ] );
    }

    my $report = join( "\n\n", $preamble, $parsed, Dumper( $omap_out ) )."\n";
    return $report;
}

#---------------------------------------------------------------------
# file read/write:
#---------------------------------------------------------------------

#---------------------------------------------------------------------
sub DESTROY {
    my $self = shift;
    $self->close_files();
}

#---------------------------------------------------------------------
sub close_files {
    my $self = shift;
    if( my $href = $Read_fh{ $self } ) {
        while( my( $file, $fh ) = each %$href ) {
            close $fh or die "Can't close $file: $!";
        }
        delete $Read_fh{ $self };
    }
    if( my $href = $Write_fh{ $self } ) {
        while( my( $file, $fh ) = each %$href ) {
            close $fh or die "Can't close $file: $!";
        }
        delete $Write_fh{ $self };
    }
}

#---------------------------------------------------------------------
sub locked_for_read {
    my( $self, $file ) = @_;

    my $open_fh = $Read_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    my $fh;
    open $fh, '<', $file or croak "Can't open $file: $!";
    flock $fh, LOCK_SH   or croak "Can't lock $file: $!";
    binmode $fh;

    $Read_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
sub locked_for_write {
    my( $self, $file ) = @_;

    my $open_fh = $Write_fh{ $self }{ $file };
    return $open_fh if $open_fh;

    my $fh;
    sysopen( $fh, $file, O_RDWR|O_CREAT ) or croak "Can't open $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );
    flock $fh, LOCK_EX                    or croak "Can't lock $file: $!";
    binmode $fh;

    $Write_fh{ $self }{ $file } = $fh;
    return $fh;
}

#---------------------------------------------------------------------
sub read_record {
    my( $self, $fh, $seekpos ) = @_;

    my $len      = $self->preamblelen();
    my $string   = $self->read_bytes( $fh, $seekpos, $len );
    my $preamble = $self->new_preamble( { string => $string } );

    $seekpos    += $len;
    $len         = $preamble->reclen();
    my $recdata  = $self->read_bytes( $fh, $seekpos, $len ); 

    my $record = $self->new_record( {
        preamble => $preamble,
        data     => \$recdata,
        } );

    return $record;
}

#---------------------------------------------------------------------
sub read_preamble {
    my( $self, $fh, $seekpos ) = @_;

    my $len = $self->preamblelen();

    my $string;
    sysseek $fh, $seekpos, 0   or croak "Can't seek: $!";
    sysread $fh, $string, $len or croak "Can't read: $!";

    return $string;
}

#---------------------------------------------------------------------
sub read_bytes {
    my( $self, $fh, $seekpos, $len ) = @_;

    my $string;
    sysseek $fh, $seekpos, 0   or croak "Can't seek: $!";
    sysread $fh, $string, $len or croak "Can't read: $!";

    return $string;
}

#---------------------------------------------------------------------
sub write_bytes {
    my( $self, $fh, $seekpos, $string ) = @_;

    sysseek  $fh, $seekpos, 0 or croak "Can't seek: $!";
    syswrite $fh, $string     or croak "Can't write: $!";

    return $string;
}

#---------------------------------------------------------------------
# read_file(), slurp contents of file

sub read_file {
    my( $self, $file ) = @_;

    my $fh = $self->locked_for_read( $file );
    local $/;
    return <$fh>;
}

#---------------------------------------------------------------------
# write_file(), dump contents to file (opposite of slurp, sort of)

sub write_file {
    my( $self, $file, $contents ) = @_;

    my $fh = $self->locked_for_write( $file );
    my $type = ref $contents;
    if( $type ) {
        if   ( $type eq 'SCALAR' ) { print $fh $$contents           }
        elsif( $type eq 'ARRAY'  ) { print $fh join "", @$contents  }
        else                       { croak "Unrecognized type: $type" }
    }
    else { print $fh $contents }
}

#---------------------------------------------------------------------
# utilities (XXX will probably move to individual modules)
#---------------------------------------------------------------------

#---------------------------------------------------------------------
# new(), expects yyyymmdd or yymd (or mmddyyyy, mdyy, etc.)
#        returns current date formatted as requested

sub now {
    my( $format ) = @_;
    my( $y, $m, $d ) =
        sub{($_[5]+1900,$_[4]+1,$_[3])}->(localtime());
    for( $format ) {
        if( /yyyy/ ) {  # decimal year/month/day
            s/ yyyy / sprintf("%04d",$y) /ex;
            s/ mm   / sprintf("%02d",$m) /ex;
            s/ dd   / sprintf("%02d",$d) /ex;
        }
        else {  # assume base62 year/month/day
            s/ yy / int2base( $y, 62) /ex;
            s/ m  / int2base( $m, 62) /ex;
            s/ d  / int2base( $d, 62) /ex;
        }
    }
    return $format;
}

#---------------------------------------------------------------------
# then(), translates stored date to YYYY-MM-DD

sub then {
    my( $self, $date, $format ) = @_;
    my( $y, $m, $d );
    my $ret;
    for( $format ) {
        if( /yyyy/ ) {  # decimal year/month/day
            $y = substr $date, index( $format, 'yyyy' ), 4;
            $m = substr $date, index( $format, 'mm'   ), 2;
            $d = substr $date, index( $format, 'dd'   ), 2;
        }
        else {  # assume base62 year/month/day
            $y = substr $date, index( $format, 'yy' ), 2;
            $m = substr $date, index( $format, 'm'  ), 1;
            $d = substr $date, index( $format, 'd'  ), 1;
            $y = sprintf "%04d", base2int( $y, 62 );
            $m = sprintf "%02d", base2int( $m, 62 );
            $d = sprintf "%02d", base2int( $d, 62 );
        }
    }
    return "$y-$m-$d";
}

#---------------------------------------------------------------------
# setbit(), 3 parms: bit vector, number, 0|1; changes vector
#           e.g., set_bit( $vec, 20 );
sub setbit { vec( $_[0], $_[1], 1 ) = $_[2] }

#---------------------------------------------------------------------
# bit2str(), 1 parm: bit vector; returns string of [01]+
#            e.g., $str = bit2str( $vec );
sub bit2str { unpack "b*", $_[0] }

#---------------------------------------------------------------------
# str2bit(), 1 parm: string of [01]+; returns bit vector
#            e.g., $vec = str2bit( $str );
sub str2bit { pack "b*", $_[0] }

#---------------------------------------------------------------------
# num2bit(), 1 parm: ref to array of integers; returns bit vector
#            e.g., $vec = num2bit( \@a );
sub num2bit {
    my $bvec = "";
    foreach my $num ( @{$_[0]} ) { vec( $bvec, $num, 1 ) = 1 }
    $bvec;  # returned
}

#---------------------------------------------------------------------
# bitcount(), 2 parm: bit vector, 0|1; returns number where bit==0|1
#             e.g., $one_count  = bitcount( $vec, 1 )
#             e.g., $zero_count = bitcount( $vec, 0 )
sub bitcount {
    my( $bvec, $bitval ) = @_;

    my $setbits = unpack "%32b*", $bvec;
    return $setbits if $bitval;
    return 8 * length($bvec) - $setbits;

}

#---------------------------------------------------------------------
# bit2num(), 1 parm: bit vector; returns aref of numbers where bit==1
#            e.g., @a = bit2num( $vec );
sub bit2num {
    my( $v, $beg, $cnt ) = @_;
    my @num;
    my $count;

    if( $beg ) {
        if( $cnt ) {
            my $end = $beg + $cnt - 1;
            for( my $i = 0; $i < 8 * length $v; ++$i ) {
                if( vec $v, $i, 1 and ++$count >= $beg and $count <= $end ) {
                    push @num, $i } }
        }
        else {
            for( my $i = 0; $i < 8 * length $v; ++$i ) {
                if( vec $v, $i, 1 and ++$count >= $beg ) {
                    push @num, $i } }
        }
    }

    else {
        for( my $i = 0; $i < 8 * length $v; ++$i ) {
            push @num, $i if vec $v, $i, 1 }
    }

    \@num;  # returned

}

1;  # returned

__END__

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

