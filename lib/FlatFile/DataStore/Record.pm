#!/usr/local/bin/perl

use strict;
use warnings;

#---------------------------------------------------------------------
package FlatFile::DataStore::Record;

use Carp;

my %Attrs = qw(
    preamble  1
    data      1
    );

#---------------------------------------------------------------------
sub new {
    my( $class, $parms ) = @_;

    my $self = bless {}, $class;

    $self->init( $parms ) if $parms;
    return $self;
}


#---------------------------------------------------------------------
sub init {
    my( $self, $parms ) = @_;

    # want to store record data as a scalar reference
    for( $parms->{'data'} ) {
        if( defined ) {
            if( ref eq 'SCALAR' ) { $self->data( $_  ) }
            else                  { $self->data( \$_ ) }
        }
        else                      { $self->data( \"" ) }
    }

    if( my $preamble = $parms->{'preamble'} ) {
        $self->preamble( $preamble );
    }
    
    return $self;
}

#---------------------------------------------------------------------
# accessors

#---------------------------------------------------------------------
# read/write

sub data     {for($_[0]->{data}    ){$_=$_[1]if@_>1;return$_}}
sub preamble {for($_[0]->{preamble}){$_=$_[1]if@_>1;return$_}}

#---------------------------------------------------------------------
# readonly

sub user        {for($_[0]->preamble()){defined&&return$_->user()}}
sub string      {$_[0]->preamble()->string()     }
sub indicator   {$_[0]->preamble()->indicator()  }
sub date        {$_[0]->preamble()->date()       }
sub keynum      {$_[0]->preamble()->keynum()     }
sub reclen      {$_[0]->preamble()->reclen()     }
sub transnum    {$_[0]->preamble()->transnum()   }
sub thisfilenum {$_[0]->preamble()->thisfilenum()}
sub thisseekpos {$_[0]->preamble()->thisseekpos()}
sub prevfilenum {$_[0]->preamble()->prevfilenum()}
sub prevseekpos {$_[0]->preamble()->prevseekpos()}
sub nextfilenum {$_[0]->preamble()->nextfilenum()}
sub nextseekpos {$_[0]->preamble()->nextseekpos()}

__END__
