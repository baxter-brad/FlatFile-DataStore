#---------------------------------------------------------------------
  package FlatFile::DataStore::Toc;
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Toc - Perl module that implements a flat
file data store toc (table of contents) class.

=head1 SYNOPSYS

 use FlatFile::DataStore::Toc;


$toc = FlatFile::DataStore::Toc->new( { int => 10,
    datastore => $datastore_obj } );

# or

$toc = FlatFile::DataStore::Toc->new( { num => "A",
    datastore => $datastore_obj } );

=head1 DESCRIPTION

FlatFile::DataStore::Toc is a Perl module that implements a flat file
data store toc (table of contents) class.

=head1 VERSION

FlatFile::DataStore::Toc version 0.02

=cut

our $VERSION = '0.02';

use 5.008003;
use strict;
use warnings;

use Carp;
use Math::Int2Base qw( base_chars int2base base2int );

my %Attrs = qw(
    datastore 1
    datafile  1
    keyfile   1
    tocfile   1
    keynum    1
    transnum  1
    create    1
    oldupd    1
    update    1
    olddel    1
    delete    1
    );

#---------------------------------------------------------------------

=head1 CLASS METHODS

=head2 FlatFile::DataStore::Toc->new( $parms )

Constructs a new FlatFile::DataStore::Toc object.

The parm C<$parms> is a hash reference containing key/value pairs to
populate the record string.  Two keys are recognized:

 - str, a toc string that comprises a line in the toc file (with or without an ending recsep)
 - int, data file number as integer, will load object from tocfile
 - num, data file number as number in number base, will load from tocfile

An C<int> or C<num> of 0 will load the first (totals) line from tocfile

=cut

sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self->init( $parms ) if $parms;
    return $self;
}


#---------------------------------------------------------------------
# init(), called by new() to parse the parms

sub init {
    my( $self, $parms ) = @_;

    my $ds = $parms->{'datastore'} || croak "Missing datastore";
    $self->datastore( $ds );

    my $filenumbase = $ds->filenumbase;
    my $keybase     = $ds->keybase;
    my $transbase   = $ds->transbase;

    my $datafileint;
    if(    defined( my $int = $parms->{'int'} ) ) { $datafileint = $int                        }
    elsif( defined( my $num = $parms->{'num'} ) ) { $datafileint = base2int $num, $filenumbase }

    my $string = defined $datafileint? $self->read_toc( $datafileint ): $parms->{'str'};

    return $self unless $string;

    my $recsep = $ds->recsep;
    $string =~ s/$recsep$//;  # chompish
    $self->string( $string );

    my @fields = split " ", $string;
    my $i = 0;
    for( qw( datafile keyfile tocfile )                    ) {
        $self->$_( base2int $fields[ $i++ ], $filenumbase  ) }
    for( qw( keynum )                                      ) {
        $self->$_( base2int $fields[ $i++ ], $keybase      ) }
    for( qw( transnum create oldupd update olddel delete ) ) {
        $self->$_( base2int $fields[ $i++ ], $transbase    ) }

    return $self;
}

#---------------------------------------------------------------------

=head1 OBJECT METHODS

=cut

#---------------------------------------------------------------------
sub to_string {
    my( $self ) = @_;

    my $ds = $self->datastore;

    my $filenumbase = $ds->filenumbase;
    my $filenumlen  = $ds->filenumlen;
    my $keybase     = $ds->keybase;
    my $keylen      = $ds->keylen;
    my $transbase   = $ds->transbase;
    my $translen    = $ds->translen;

    my @fields;
    for( qw( datafile keyfile tocfile )                                ) {
        push @fields, int2base( $self->$_(), $filenumbase, $filenumlen ) }
    for( qw( keynum )                                                  ) {
        push @fields, int2base( $self->$_(), $keybase, $keylen         ) }
    for( qw( transnum create oldupd update olddel delete )             ) {
        push @fields, int2base( $self->$_(), $transbase, $translen     ) }

    return join( " " => @fields ) . $ds->recsep;
}

#---------------------------------------------------------------------
# seekpos if tocmax, e.g., tocmax=3, fileint=7, toclen=4
#
# 1: 0   xxxx     skip    = int( fileint / tocmax )
#    1   xxxx             = int(    7    /   3    )
#    2   xxxx             = 2 (files to skip)
# 2: 3   xxxx     seekpos = toclen * ( fileint - ( skip * tocmax ) )
#    4   xxxx             =   4    * (    7    - (  2   *   3    ) )
#    5   xxxx             =   4    * (    7    -        6          )
# 3: 6   xxxx             =   4    *           1
#    7 =>xxxx             = 4
#    8   xxxx     '=>' marks seekpos 4 in file 3
            
sub read_toc {
    my( $self, $fileint ) = @_;

    my $ds = $self->datastore;

    my $tocfh  = $ds->locked_for_read( $self->tocfilename( $fileint ) );
    my $toclen = $ds->toclen;

    my $seekpos;
    if( my $tocmax = $ds->tocmax ) {
        my $skip = int( $fileint / $tocmax );
        $seekpos = $toclen * ( $fileint - ( $skip * $tocmax ) ); }
    else {
        $seekpos = $toclen * $fileint; }

    my $string = $ds->read_bytes( $tocfh, $seekpos, $toclen );
    croak "No toc string?" unless $string;
    return $string;
}

#---------------------------------------------------------------------
sub write_toc {
    my( $self, $fileint ) = @_;

    my $ds = $self->datastore;

    my $tocfh   = $ds->locked_for_write( $self->tocfilename( $fileint ) );
    my $toclen  = $ds->toclen;

    my $seekpos;
    if( my $tocmax = $ds->tocmax ) {
        my $skip = int( $fileint / $tocmax );
        $seekpos = $toclen * ( $fileint - ( $skip * $tocmax ) ); }
    else {
        $seekpos = $toclen * $fileint; }

    return $ds->write_bytes( $tocfh, $seekpos, $self->to_string );
}

#---------------------------------------------------------------------
sub tocfilenum {
    my( $self, $fileint ) = @_;

    my $ds = $self->datastore;

    my $filenumlen  = $ds->filenumlen;
    my $filenumbase = $ds->filenumbase;

    # get toc file number based on tocmax and fileint
    my $tocfileint;

    my $tocmax = $ds->tocmax;
    if( $tocmax ) { $tocfileint = int( $fileint / $tocmax ) + 1 }
    else          { $tocfileint = 1                             }

    my $tocfilenum = int2base( $tocfileint, $filenumbase, $filenumlen );

    return( $tocfileint, $tocfilenum ) if wantarray;
    return $tocfileint;

}

#---------------------------------------------------------------------
sub tocfilename {
    my( $self, $fileint ) = @_;

    my $ds = $self->datastore;

    my $dir         = $ds->dir;
    my $name        = $ds->name;
    my $filenumlen  = $ds->filenumlen;
    my $filenumbase = $ds->filenumbase;
    my $filenum     = int2base( $fileint, $filenumbase, $filenumlen );

    my( $tocfileint, $tocfilenum ) = $self->tocfilenum( $fileint );
    my $tocpath = $name . ( $ds->tocmax? ".$tocfilenum.": ".") . "toc";

    # get toc path based on dirlev, dirmax, and toc file number
    if( my $dirlev = $ds->dirlev ) {
        my $dirmax = $ds->dirmax||croak "No dirmax?";
        my $path   = "";
        my $this   = $tocfileint;
        for( 1 .. $dirlev ) {
            my $dirint = int( ( $this - 1 ) / $dirmax ) + 1;
            my $dirnum = int2base( $dirint, $filenumbase, $filenumlen );
            $path = "$dirnum/$path";
            $this = $dirint;
        }
        $tocpath = "toc$path/$tocpath";
    }

    return "$dir/$tocpath";

}

#---------------------------------------------------------------------

=head1 OBJECT METHODS: ACCESSORS

The following read/write methods set and return their respective
attribute values if C<$value> is given.  Otherwise, they just return
the value.

 $record->data(     [$value] ); # actual record data
 $record->preamble( [$value] ); # FlatFile::DataStore::Preamble object

=cut

sub data     {for($_[0]->{data}    ){$_=$_[1]if@_>1;return$_}}
sub preamble {for($_[0]->{preamble}){$_=$_[1]if@_>1;return$_}}

=pod

The following read-only methods just return their respective values.
The values all come from the record's contained preamble object.

 $record->datastore()
 $record->datafile()
 $record->keyfile()
 $record->tocfile()
 $record->keynum()
 $record->transnum()
 $record->create()
 $record->oldupd()
 $record->update()
 $record->olddel()
 $record->delete()
 $record->string()

=cut

sub datastore {for($_[0]->{datastore} ){$_=$_[1]if@_>1;return$_}}
sub datafile  {for($_[0]->{datafile}  ){$_=$_[1]if@_>1;return$_}}
sub keyfile   {for($_[0]->{keyfile}   ){$_=$_[1]if@_>1;return$_}}
sub tocfile   {for($_[0]->{tocfile}   ){$_=$_[1]if@_>1;return$_}}
sub keynum    {for($_[0]->{keynum}    ){$_=$_[1]if@_>1;return$_}}
sub transnum  {for($_[0]->{transnum}  ){$_=$_[1]if@_>1;return$_}}
sub create    {for($_[0]->{create}    ){$_=$_[1]if@_>1;return$_}}
sub oldupd    {for($_[0]->{oldupd}    ){$_=$_[1]if@_>1;return$_}}
sub update    {for($_[0]->{update}    ){$_=$_[1]if@_>1;return$_}}
sub olddel    {for($_[0]->{olddel}    ){$_=$_[1]if@_>1;return$_}}
sub delete    {for($_[0]->{delete}    ){$_=$_[1]if@_>1;return$_}}
sub string    {for($_[0]->{string}    ){$_=$_[1]if@_>1;return$_}}

__END__

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

