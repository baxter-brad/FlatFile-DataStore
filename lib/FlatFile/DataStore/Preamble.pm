#!/usr/local/bin/perl

use strict;
use warnings;

#---------------------------------------------------------------------
package FlatFile::DataStore::Preamble;

use Carp;
use Math::Int2Base qw( base_chars int2base base2int );
use Data::Omap qw( :ALL );

my %Generated = qw(
    string      1
    );

my %Attrs = ( %Generated, qw(
    indicator   1
    date        1
    keynum      1
    reclen      1
    transnum    1
    thisfilenum 1
    thisseekpos 1
    prevfilenum 1
    prevseekpos 1
    nextfilenum 1
    nextseekpos 1
    user        1
    ) );

my $asc_chr = qr/^[ -~]+$/;

#---------------------------------------------------------------------
sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self->init( $parms ) if $parms;
    return $self;
}

#---------------------------------------------------------------------

=for comment

 parms (
     datastore   = $datastore_obj,  # FlatFile::DataStore object
     parent      = $old_preamble,   # FlatFile::DataStore::Preamble object
     indicator   = $indicator,      # required, one of qw(- = + * #) usually
        # -|delete, =|update, +|create, *|olddel, #|oldupd
        # indicator  '+'  must NOT have a parent
        # indicators '-=' MUST     have a parent
        # (XXX what's the deal with '*' and '#', then?)
     date        = $date,           # required, yyyymmdd usually
     keynum      = $keynum,         # required, may be 0
     reclen      = $reclen,         # required, may be 0
     thisfilenum = $thisfilenum,    # required, is never 0
     thisseekpos = $thisseekpos,    # required, may be 0
     prevfilenum = $prevfilenum,    # required for -=, is never 0
     prevseekpos = $prevseekpos,    # required for -=, may be 0
     nextfilenum = $nextfilenum,    # required for *#, prohibited for -=+, is never 0
     nextseekpos = $nextseekpos,    # required for *#, prohibited for -=+, may be 0
     user        = $user,           # required if configured, must match /^[ -~]+$/
 );

 if parent, it is changed like this:
     - nextfilenum = new file number   (can't be zero)
     - nextseekpos = new seek position (could be zero, if new file)
     - for (update, including recover),  indicator = '#'
     - for (delete),                     indicator = '*'
     - all other parent fields are left unchanged

=cut

#---------------------------------------------------------------------
sub init {
    my( $self, $parms ) = @_;

    my $datastore = $parms->{'datastore'} || croak "Missing datastore";
    if( my $string = $parms->{'string'} ) {
        $parms = $datastore->burst_preamble( $string );
    }

    my $crud   = $datastore->crud();
    my $create = $crud->{'create'};
    my $update = $crud->{'update'};
    my $delete = $crud->{'delete'};
    my $oldupd = $crud->{'oldupd'};
    my $olddel = $crud->{'olddel'};

    my $indicator = $parms->{'indicator'} || croak "Missing indicator";

    # change, e.g., 'create' to '+'
    for( $indicator ) { $_ = $crud->{ $_ } if exists $crud->{ $_ } }
    $self->indicator( $indicator );

    my $string = "";
    for my $href ( $datastore->specs() ) {  # each field is href of aref
        my( $field, $aref )     = %$href;
        my( $pos, $len, $parm ) = @$aref;
        my $value               = $parms->{ $field };

        for( $field ) {
            if( /indicator/ ) {
                croak qq'Missing value for "$_"' unless defined $value;
                croak qq'Invalid value for "$_" ($value)' unless length $value == $len;

                $self->{ $_ } = $value;
                $string      .= $value;
            }
            elsif( /date/ ) {
                croak qq'Missing value for "$_"' unless defined $value;
                croak qq'Invalid value for "$_" ($value)' unless length $value == $len;

                $self->{ $_ } = $datastore->then( $value, $parm );
                $string      .= $value;
            }
            elsif( /user/ ) {
                croak qq'Missing value for "$_"' unless defined $value;
                croak qq'Invalid value for "$_" ($value)' unless $value =~ $asc_chr;

                my $try = sprintf "%-${len}s", $value;  # pads with blanks
                croak qq'Value of "$_" ($try) too long' if length $try > $len;

                $self->{ $_ } = $value;
                $string      .= $try;
            }
            elsif( not defined $value ) {
                if( (/keynum|reclen|transnum|thisfilenum|thisseekpos/              ) or
                    (/prevfilenum|prevseekpos/ and $indicator =~ /[$update$delete]/) or
                    (/nextfilenum|nextseekpos/ and $indicator =~ /[$oldupd$olddel]/) ) {
                    croak qq'Missing value for "$_"';
                }
                $string .= "-" x $len;  # string of '-' for null
            }
            else {
                if( (/nextfilenum|nextseekpos/ and $indicator =~ /[$update$delete]/) or
                    (/prevfilenum|prevseekpos/ and $indicator =~ /[$create]/       ) ) {
                    croak qq'Setting value of "$_" not permitted';
                }
                my $try = sprintf "%0${len}s", /filenum/? $value: int2base( $value, $parm );
                croak qq'Value of "$_" ($try) too long' if length $try > $len;

                $self->{ $_ } = /filenum/? $try: $value;
                $string      .= $try;
            }
        }
    }

    croak qq'Something is wrong with preamble string: "$string"'
        unless $string =~ $datastore->regx();
    
    $self->string( $string );

    return $self;
}

#---------------------------------------------------------------------
# accessors

#---------------------------------------------------------------------
# read/write

sub string      {for($_[0]->{string}      ){$_=$_[1]if@_>1;return$_}}
sub indicator   {for($_[0]->{indicator}   ){$_=$_[1]if@_>1;return$_}}
sub date        {for($_[0]->{date}        ){$_=$_[1]if@_>1;return$_}}
sub keynum      {for($_[0]->{keynum}      ){$_=$_[1]if@_>1;return$_}}
sub reclen      {for($_[0]->{reclen}      ){$_=$_[1]if@_>1;return$_}}
sub transnum    {for($_[0]->{transnum}    ){$_=$_[1]if@_>1;return$_}}
sub thisfilenum {for($_[0]->{thisfilenum} ){$_=$_[1]if@_>1;return$_}}
sub thisseekpos {for($_[0]->{thisseekpos} ){$_=$_[1]if@_>1;return$_}}
sub prevfilenum {for($_[0]->{prevfilenum} ){$_=$_[1]if@_>1;return$_}}
sub prevseekpos {for($_[0]->{prevseekpos} ){$_=$_[1]if@_>1;return$_}}
sub nextfilenum {for($_[0]->{nextfilenum} ){$_=$_[1]if@_>1;return$_}}
sub nextseekpos {for($_[0]->{nextseekpos} ){$_=$_[1]if@_>1;return$_}}
sub user        {for($_[0]->{user}        ){$_=$_[1]if@_>1;return$_}}

__END__
