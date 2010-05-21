#---------------------------------------------------------------------
package FlatFile::DataStore::Utils;

use 5.008003;
use strict;
use warnings;

use Fcntl qw(:DEFAULT :flock);
use SDBM_File;
use Digest::MD5 qw(md5_hex);
use FlatFile::DataStore;
use Math::Int2Base qw( base_chars int2base base2int );

#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Utils - a collection of utility routines for
FlatFile::DataStore data stores.

=cut

#---------------------------------------------------------------------

=head1 VERSION

VERSION: 0.11

=cut

our $VERSION = '0.11'; $VERSION = eval $VERSION;

#---------------------------------------------------------------------

=head1 EXPORTS

Nothing is exported by default.  The following may be exported
individually; all three may be exported using the C<:all> tag:

 - migrate
 - validate
 - compare

Examples:

 use FlatFile::DataStore::Utils qw( migrate validate compare );
 use FlatFile::DataStore::Utils qw( :all );

=cut

our ( @ISA, @EXPORT_OK, %EXPORT_TAGS );

BEGIN {
    require Exporter;
    @ISA       = qw( Exporter );
    @EXPORT_OK = qw(
        migrate
        validate
        compare
        );
    %EXPORT_TAGS = ( all => [ @EXPORT_OK ] );
}

#---------------------------------------------------------------------

=head1 SYNOPSIS

    use FlatFile::DataStore::Utils qw( migrate validate compare );
    
=cut

#---------------------------------------------------------------------

=head1 DESCRIPTION

This module provides routines for validating a data store (checking
that it can be traversed, and that its past record data has not
changed) and for migrating a data store to a new set of (probably
differently configured) files.

=cut

#---------------------------------------------------------------------

=head1 SUBROUTINES

Descriptions and parameters for the exportable subroutines are
detailed below.

=head2 validate( $dir, $name, $seen_href )

=head3 Parameters:

=head4 $dir

The directory of the data store.

=head4 $name

The name of the data store.

=head4 $seen_href

This optional parameter allows you to provide your own hash reference
(perhaps tied) for the "seen" hash


=cut

#---------------------------------------------------------------------

=for comment

 1. - for each keynum in name.key file (each current record)
      - get the history
      - for each preamble in the history
        - seen{ preamble }++ (name.tmp.seen dbm file)
        - write to name.tmp.history
          - transnum keynum status [date reclen user]?
 2. - for each record in name.n.dat files (each transaction)
      - die "seen too many times" if seen{ preamble }++ > 1
      - write to name.tmp.transactions
        - transnum keynum status [date reclen user]?
      - if read line from name.md5
        - compare transnum keynum user md5--die if not equal
      - else write to name.md5
        - transnum keynum user md5 (of record data) [date reclen]?

 result:
     name.tmp.seen dbm file(s)       - of no use after step 2.
     name.tmp.history flat file      - can compare to new after migrate
                                           of no use after that
     name.tmp.transactions flat file - can compare to new after migrate
                                           of no use after that
     name.md5 [sha]?                 - can compare to new after migrate
                                           keep around for next validation

=cut

{ my %seenfile;
sub validate {
    my( $dir, $name, $seen ) = @_;

    my $ds = FlatFile::DataStore->new( { dir => $dir, name => $name } );

    unless( $seen ) {
        $seenfile{ $name } = "$dir/$name.tmp.seen";
        my %seen;  # only reference is used
        tie( %seen, "SDBM_File", $seenfile{ $name }, O_RDWR|O_CREAT, 0666 )
            or die qq/Couldn't tie file $seenfile{ $name }: $!/;
        $seen = \%seen;
    }

    # status is a reverse crud hash for writing to
    # history and transactions files
    my $crud   = $ds->crud;
    my %status = reverse %$crud;

    # build history file for comparing after migrate
    my $histfile = "$dir/$name.tmp.history";
    my $histfh   = locked_for_write( $histfile );

    for my $keynum ( 0 .. $ds->lastkeynum ) {

        for my $rec ( $ds->history( $keynum ) ) {

            my $string = $rec->preamble->string;

            # shouldn't have been seen yet
            die qq/Seen $string too many times/ if $seen->{ $string }++;

            my $transnum  = $rec->transnum;
            my $keynum    = $rec->keynum;
            my $status    = $status{ $rec->indicator };
            my $md5       = md5_hex( ${$rec->data} );
            print $histfh "$transnum $keynum $status $md5\n";
        }

    }
    close $histfh;

    # parse data files and build
    #     transaction file for comparing after migrate, and
    #     md5 file for future validations

    my $recsep      = $ds->recsep;
    my $recseplen   = length( $recsep );
    my $preamblelen = $ds->preamblelen;

    my $transfile = "$dir/$name.tmp.transactions";
    my $transfh   = locked_for_write( $transfile );

    # our position in md5file will tell us if we have to
    # add this md5 or compare this md5 to an older one

    my $md5file = "$dir/$name.md5";
    my $md5fh   = locked_for_readwrite( $md5file );
    my $md5size = -s $md5file;
    my $md5pos  = 0;

    for my $datafile ( $ds->all_datafiles ) {

        my $datafh   = locked_for_read( $datafile );
        my $filesize = -s $datafile;
        my $seekpos  = 0;

        RECORD: while( $seekpos < $filesize ) {

            my $rec       = $ds->read_record( $datafh, $seekpos );
            my $transnum  = $rec->transnum;
            my $keynum    = $rec->keynum;
            my $reclen    = $rec->reclen;
            my $data_ref  = $rec->data;
            my $status    = $status{ $rec->indicator };
            my $string    = $rec->preamble->string;

            # should have been seen only once before in the history
            die qq/Seen "$string" too many times/ if $seen->{ $string }++ > 1;

            my $md5 = md5_hex( $$data_ref );
            print $transfh "$transnum $keynum $status $md5\n";

            # add this md5 or compare this md5 to an older one?

            my $md5out = "$transnum $keynum $reclen $md5\n";
            my $outlen = length( $md5out );

            if( $md5pos < $md5size ) {
                my $md5line = $ds->read_bytes( $md5fh, $md5pos, $outlen );
                die qq/Mismatched md5 lines/ unless $md5line eq $md5out;
            }
            else {
                $ds->write_bytes( $md5fh, $md5pos, $md5out );
            }

            $md5pos += $outlen;

            # move forward in data file
            $seekpos += $preamblelen + $reclen;

            # use recsep as a sentinel for probably okay progress so far
            my $sentinel = $ds->read_bytes( $datafh, $seekpos, $recseplen );
            die qq/Expected a recsep but got: "$sentinel" (at byte "$seekpos" in "$datafile")/
                unless $sentinel eq $recsep;

            $seekpos += $recseplen;
        }
    }
    close $transfh;
    close $md5fh;
}

END {
    for my $name ( keys %seenfile ) {
        for( "$seenfile{ $name }.dir", "$seenfile{ $name }.pag" ) {
            unlink or die qq/Can't delete $_: $!/;
        }
    }
}}

#---------------------------------------------------------------------

=for comment

    data scanning procedure:

    read each data record in from_ds
      read first preamble
        get reclen, read record, skip recsep
        read next preamble
        repeat until end of file
    repeat for every datafile

=cut

sub migrate {
    my( $from_dir, $from_name, $to_dir, $to_name ) = @_;

    my $from_ds = FlatFile::DataStore->new( { dir => $from_dir, name => $from_name } );
    my $to_ds   = FlatFile::DataStore->new( { dir => $to_dir,   name => $to_name   } );

    # check some fundamental constraints

    my $from_count = $from_ds->howmany;  # should *not* be zero
    die qq/Can't migrate: "$from_name" datastore empty?/ unless $from_count;

    my $to_count = $to_ds->howmany;  # *should* be zero
    die qq/Can't migrate: "$to_name" datastore not empty?/ if $to_count;

    my $try = $to_ds->which_datafile( 1 );  # first datafile
    die qq/Can't migrate: "$to_name" has a data file, e.g., "$try"/ if -e $try;

    # get ready to loop through datafiles

    my $from_recsep      = $from_ds->recsep;
    my $from_recseplen   = length( $from_recsep );
    my $from_preamblelen = $from_ds->preamblelen;

    my $from_crud = $from_ds->crud;
    my $create    = $from_crud->{'create'};  # these are single ascii chars
    my $oldupd    = $from_crud->{'oldupd'};
    my $update    = $from_crud->{'update'};
    my $olddel    = $from_crud->{'olddel'};
    my $delete    = $from_crud->{'delete'};

    my $last_keynum = -1;  # to be less than 0

    for my $datafile ( $from_ds->all_datafiles ) {

        my $datafh   = locked_for_read( $datafile );
        my $filesize = -s $datafile;
        my $seekpos  = 0;

        my %pending_deletes;

        RECORD: while( $seekpos < $filesize ) {

            my $from_rec       = $from_ds->read_record( $datafh, $seekpos );
            my $keynum         = $from_rec->keynum;
            my $reclen         = $from_rec->reclen;
            my $from_data_ref  = $from_rec->data;
            my $from_user_data = $from_rec->user;

            # cases:                               
            # indicator:  keynum:     pending_delete:  action:              because:
            # ----------  ----------  ---------------  -------------------  ----------
            # create  +   always new                   create               is current
            # oldupd  #   new                          create               was +
            # oldupd  #   old         if on, turn off  retrieve and delete  was -
            # oldupd  #   old                          retrieve and update  was =
            # update  =   always old                   retrieve and update  is current
            # olddel  *   new         turn on          create               was +
            # olddel  *   old         turn on          retrieve and update  was =
            # delete  -   always old  turn off         retrieve and delete  is current

            my $new_keynum = $keynum > $last_keynum;

            for( $from_rec->indicator ) {
                /[$create]/ && do { $to_ds->create( $from_data_ref, $from_user_data );
                                    last };
                /[$oldupd]/ && $new_keynum
                            && do { $to_ds->create( $from_data_ref, $from_user_data );
                                    last };
                /[$oldupd]/ && $pending_deletes{ $keynum }
                            && do { my $to_rec =
                                    $to_ds->retrieve( $keynum );
                                    $to_ds->delete( $to_rec, $from_data_ref, $from_user_data );
                                    delete $pending_deletes{ $keynum };
                                    last };
                /[$oldupd]/ && do { my $to_rec =
                                    $to_ds->retrieve( $keynum );
                                    $to_ds->update( $to_rec, $from_data_ref, $from_user_data );
                                    last };
                /[$update]/ && do { my $to_rec =
                                    $to_ds->retrieve( $keynum );
                                    $to_ds->update( $to_rec, $from_data_ref, $from_user_data );
                                    last };
                /[$olddel]/ && $new_keynum
                            && do { $to_ds->create( $from_data_ref, $from_user_data );
                                    ++$pending_deletes{ $keynum };
                                    last };
                /[$olddel]/ && do { my $to_rec =
                                    $to_ds->retrieve( $keynum );
                                    $to_ds->update( $to_rec, $from_data_ref, $from_user_data );
                                    ++$pending_deletes{ $keynum };
                                    last };
                /[$delete]/ && do { my $to_rec =
                                    $to_ds->retrieve( $keynum );
                                    $to_ds->delete( $to_rec, $from_data_ref, $from_user_data );
                                    delete $pending_deletes{ $keynum };
                                    last };
            }

            $last_keynum = $keynum if $new_keynum;

            # move forward in data file
            $seekpos += $from_preamblelen + $reclen;

            # use recsep as a sentinel for probably okay progress so far
            my $sentinel = $from_ds->read_bytes( $datafh, $seekpos, $from_recseplen );
            die qq/Expected a recsep but got: "$sentinel" (at byte "$seekpos" in "$datafile")/
                unless $sentinel eq $from_recsep;

            $seekpos += $from_recseplen;
        }
    }
}

#---------------------------------------------------------------------
# it goes like this:
#
# 1. validate from_ds
# 2. migrate from from_ds to to_ds
# 3. validate to_ds
# 4. compare from_ds history/transactions/md5 to to_ds
#    i.e, compare only works right after validate/migrate/validate

# my $histfile = "$name.tmp.history";
# my $transfile = "$name.tmp.transactions";
# my $md5file = "$name.md5";

sub compare {
    my( $from_dir, $from_name, $to_dir, $to_name ) = @_;
    my $maxdiff = 10;

    my @report;

    for ( qw( tmp.history tmp.transactions md5 ) ) {
        my $from_file = "$from_dir/$from_name.$_";
        my $to_file   = "$to_dir/$to_name.$_";
        push @report, "Comparing: $from_file $to_file\n";
        if( -e $from_file and -e $to_file ) {
            if( -s $from_file == -s $to_file ) {
                my @diff = `diff -U 1 $from_file $to_file`;
                if( @diff ) {
                    push @report, "Files differ:\n";
                    push @report, @diff[ 0 .. $maxdiff ];
                    push @report, '...' if @diff > $maxdiff
                }
            }
            else {
                push @report, "Files are different sizes.\n";
                push @report, "$from_file: ".(-s $from_file)."\n";
                push @report, "$to_file: ".(-s $to_file)."\n";
            }
        }
        else {
            push @report, "$to_file doesn't exist.\n" if -e $from_file;
            push @report, "$from_file doesn't exist.\n" if -e $to_file;
        }

    }
    return \@report;
}

#---------------------------------------------------------------------
sub locked_for_read {
    my( $file ) = @_;

    my $fh;
    open $fh, '<', $file or die "Can't open (read) $file: $!";
    flock $fh, LOCK_SH   or die "Can't lock (shared) $file: $!";
    binmode $fh;

    return $fh;
}

#---------------------------------------------------------------------
sub locked_for_write {
    my( $file ) = @_;

    my $fh;
    open $fh, '>', $file or die "Can't open (write) $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );
    flock $fh, LOCK_EX   or die "Can't lock (exclusive) $file: $!";
    binmode $fh;

    return $fh;
}

#---------------------------------------------------------------------
sub locked_for_readwrite {
    my( $file ) = @_;

    my $fh;
    sysopen( $fh, $file, O_RDWR|O_CREAT ) or die "Can't open (read/write) $file: $!";
    my $ofh = select( $fh ); $| = 1; select ( $ofh );
    flock $fh, LOCK_EX                    or die "Can't lock (exclusive) $file: $!";
    binmode $fh;

    return $fh;
}

1;  # return true

__END__
