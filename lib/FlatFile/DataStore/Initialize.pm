#---------------------------------------------------------------------
  package FlatFile::DataStore;  # not FlatFile::DataStore::Initialize
#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Initialize - Provides routines that are used
only when initializing a data store

=head1 SYNOPSYS

 require FlatFile::DataStore::Initialize;

 # new datastore object
 # XXX add uri here

 my $dir  = "/my/datastore/area";
 my $name = "dsname";
 my $ds   = FlatFile::DataStore->new( { dir => $dir, name => $name } );

=head1 DESCRIPTION

FlatFile::DataStore::Initialize provides the routines that
are used only when a data store is initialized.

=head1 VERSION

FlatFile::DataStore::Initialize version 0.11

=cut

our $VERSION = '0.11';

use 5.008003;
use strict;
use warnings;

use URI;
use URI::Escape;
use Digest::MD5 qw(md5_hex);
use Data::Dumper;
use Carp;

use Math::Int2Base qw( base_chars int2base base2int );
use Data::Omap qw( :ALL );

#---------------------------------------------------------------------
# burst_query(), called by init() to parse the datastore's uri
#     also generates values for 'spec' and 'preamblelen'

sub burst_query {
    my( $self, $Preamble ) = @_;

    my $uri   = $self->uri;
    my $query = URI->new( $uri )->query();

    my @pairs = split /[;&]/, $query;
    my $omap  = [];  # psuedo-new(), ordered hash
    my $pos   = 0;
    my %parms;
    my $load_parms = sub {
        my( $name, $val ) = split /=/, $_, 2;

        $name = uri_unescape( $name );
        $val  = uri_unescape( $val );

        croak qq/"$name" duplicated in uri/ if $parms{ $name };

        $parms{ $name } = $val;
        if( $Preamble->{ $name } ) {
            my( $len, $parm ) = split /-/, $val, 2;
            omap_add( $omap, $name => [ $pos, 0+$len, $parm ] );
            $pos += $len;
        }
    };
    for( @pairs ) {
        if( /^defaults=(.*)/ ) {
            $load_parms->( $_ ) for get_defaults( $1 );
            next;
        }
        $load_parms->( $_ );
    }

    # some attributes are generated here:
    $parms{'specs'}       = $omap;
    $parms{'preamblelen'} = $pos;

    return \%parms;
}

#---------------------------------------------------------------------
sub get_defaults {
    my( $want ) = @_;

    my @xsmall_nohist = (
        'indicator=1-+#=*-',
        'transind=1-+#=*-',
        'date=4-yymd',
        'transnum=2-62 ',  # 3,843 transactions
        'keynum=2-62',     # 3,843 records
        'reclen=2-62',     # 3,843 bytes/record
        'thisfnum=1-36',   # 35 data files
        'thisseek=4-62',   # 14,776,335 bytes/file
    );
    my @xsmall = (
        @xsmall_nohist,
        'prevfnum=1-36',
        'prevseek=4-62',
        'nextfnum=1-36',
        'nextseek=4-62',
    );

    my @small_nohist = (
        'indicator=1-+#=*-',
        'transind=1-+#=*-',
        'date=4-yymd',
        'transnum=3-62 ',  # 238,327 transactions
        'keynum=2-62',     # 238,327 records
        'reclen=2-62',     # 238,327 bytes/record
        'thisfnum=1-36',   # 35 data files
        'thisseek=5-62',   # 916,132,831 bytes/file
    );
    my @small = (
        @small_nohist,
        'prevfnum=1-36',
        'prevseek=5-62',
        'nextfnum=1-36',
        'nextseek=5-62',
    );

    my @medium_nohist = (
        'indicator=1-+#=*-',
        'transind=1-+#=*-',
        'date=4-yymd',
        'transnum=4-62 ',  # 14,776,335 transactions
        'keynum=4-62',     # 14,776,335 records
        'reclen=4-62',     # 14,776,335 bytes/record
        'thisfnum=2-36',   # 1,295 data files
        'thisseek=5-62',   # 916,132,831 bytes/file
    );
    my @medium = (
        @medium_nohist,
        'prevfnum=2-36',
        'prevseek=5-62',
        'nextfnum=2-36',
        'nextseek=5-62',
    );

    my @large_nohist = (
        'datamax=1.9G',
        'dirmax=300',
        'keymax=100_000',
        'indicator=1-+#=*-',
        'transind=1-+#=*-',
        'date=4-yymd',
        'transnum=5-62 ',  # 916,132,831 transactions
        'keynum=5-62',     # 916,132,831 records
        'reclen=5-62',     # 916,132,831 bytes/record
        'thisfnum=3-36',   # 46,655 data files
        'thisseek=6-62',   # 56G per file (but see datamax)
    );
    my @large = (
        @large_nohist,
        'prevfnum=3-36',
        'prevseek=6-62',
        'nextfnum=3-36',
        'nextseek=6-62',
    );

    my @xlarge_nohist = (
        'datamax=1.9G',
        'dirmax=300',
        'dirlev=2',
        'keymax=100_000',
        'tocmax=100_000',
        'indicator=1-+#=*-',
        'transind=1-+#=*-',
        'date=4-yymd',
        'transnum=6-62 ',  # 56B transactions
        'keynum=6-62',     # 56B records
        'reclen=6-62',     # 56G per record
        'thisfnum=4-36',   # 1,679,615 data files
        'thisseek=6-62',   # 56G per file (but see datamax)
    );
    my @xlarge = (
        @xlarge_nohist,
        'prevfnum=4-36',
        'prevseek=6-62',
        'nextfnum=4-36',
        'nextseek=6-62',
    );

    my $ret = {
        xsmall        => \@xsmall,
        xsmall_nohist => \@xsmall_nohist,
        small         => \@small,
        small_nohist  => \@small_nohist,
        medium        => \@medium,
        medium_nohist => \@medium_nohist,
        large         => \@large,
        large_nohist  => \@large_nohist,
        xlarge        => \@xlarge,
        xlarge_nohist => \@xlarge_nohist,
    }->{ $want };

    croak "Unrecognized default: $want." unless $ret;
    @$ret;  # returned
}

#---------------------------------------------------------------------
# make_preamble_regx(), called by init() to construct a regular
#     expression that should match any record's preamble
#     this regx should capture each fields value

sub make_preamble_regx {
    my( $self ) = @_;

    my $regx = "";
    for( $self->specs ) {  # specs() returns an array of hashrefs
        my( $key, $aref )       = %$_;
        my( $pos, $len, $parm ) = @$aref;

        for( $key ) {
            if( /indicator/ or /transind/ ) {
                $regx .= ($len == 1 ? "([\Q$parm\E])" : "([\Q$parm\E]{$len})");
            }
            elsif( /user/ ) {  # should only allow $Ascii_chars
                $regx .= ($len == 1 ? "([$parm])" : "([$parm]{$len})");
            }
            elsif( /date/ ) {
                $regx .= ($len == 8 ? "([0-9]{8})" : "([0-9A-Za-z]{4})");
            }
            else {
                my $chars = base_chars( $parm );
                $chars =~ s/([0-9])[0-9]+([0-9])/$1-$2/;  # compress
                $chars =~ s/([A-Z])[A-Z]+([A-Z])/$1-$2/;
                $chars =~ s/([a-z])[a-z]+([a-z])/$1-$2/;
                # '-' is 'null' character:
                $regx .= ($len == 1 ? "([-$chars])" : "([-$chars]{$len})");
            }
        }
    }
    return qr/$regx/;
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

    my( $len, $chars ) = split /-/, $self->indicator, 2;
    croak qq/Only single-character indicators supported./ if $len != 1;

    my @c = split //, $chars;
    my %c = map { $_ => 1 } @c;
    my @n = keys %c;
    croak qq/Need five unique indicator characters/ if @n != 5 or @c != 5;

    my %crud;
    @crud{ qw( create oldupd update olddel delete ) } = @c;
    @crud{ @c } = qw( create oldupd update olddel delete );
    return \%crud;
}

#---------------------------------------------------------------------
# convert_datamax(), called by init() to convert user-supplied
#     datamax value into an integer: one can say, "500_000_000",
#     "500M", or ".5G" to mean 500,000,000 bytes

sub convert_max {
    my( $max ) = @_;

    # ignoring M/G ambiguities and using round numbers:
    my %sizes = ( M => 10**6, G => 10**9 );

    $max =~ s/_//g;
    if( $max =~ /^([.0-9]+)([MG])/ ) {
        my( $n, $s ) = ( $1, $2 );
        $max = $n * $sizes{ $s };
    }

    return 0+$max;
}

#---------------------------------------------------------------------
# initialize(), called by init() when datastore is first used
#     adds a serialized object to bypass uri parsing from now on

sub initialize {
    my( $self ) = @_;

    # can't initialize after data has been added
    my $fnum     = int2base 1, $self->fnumbase, $self->fnumlen;
    my $datafile = $self->which_datafile( $fnum );
    croak qq/Can't initialize database: data files exist (e.g., $datafile)./
        if -e $datafile;

    # make object a one-liner
    local $Data::Dumper::Quotekeys = 0;
    local $Data::Dumper::Pair      = '=>';
    local $Data::Dumper::Useqq     = 1;
    local $Data::Dumper::Terse     = 1;
    local $Data::Dumper::Indent    = 0;

    # delete dir, don't want it in obj file
    my $savedir = $self->dir;
    $self->dir("");

    my $uri_file = "$savedir/" . $self->name . ".uri";
    my $uri = $self->uri;
    my $obj = Dumper $self;
    my $uri_md5 = md5_hex( $uri );
    my $obj_md5 = md5_hex( $obj );
    my $contents = <<_end_;
$uri
$obj
$uri_md5
$obj_md5
_end_
    $self->write_file( $uri_file, \$contents );

    # restore dir
    $self->dir( $savedir );

}

1;  # returned

__END__

=head1 AUTHOR

Brad Baxter, E<lt>bbaxter@cpan.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Brad Baxter

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.

=cut

