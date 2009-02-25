#---------------------------------------------------------------------

=head1 NAME

FlatFile::DataStore::Tutorial - POD containing in-depth discussion
of and tutorials for using FlatFile::DataStore.

=head1 VERSION

Discusses FlatFile::DataStore version 0.03.

=head1 SYNOPSYS

 man FlatFile::DataStore
 man FlatFile::DataStore::Tutorial

or

 perldoc FlatFile::DataStore
 perldoc FlatFile::DataStore::Tutorial

or

 http://search.cpan.org/dist/FlatFile-DataStore/

=head1 DESCRIPTION

=head2 Overview

This tutorial only contains POD, so don't do this:

 use FlatFile::DataStore::Tutorial;  # don't do this

Instead, simply read the POD (as you are doing). Also please read
the docs for FlatFile::DataStore, which is essentially the reference
manual.

This tutorial/discussion is intended to augment those docs with
longer explanations for the design of the module, more usage
examples, and other materials that will hopefully help you make
better use of it.

=head1 DISCUSSION

=head2 Overview

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

=head2 Data Store Files and Directories

Key files acts as indexes into the data files.  The different versions
of the records in the data files act as linked lists:

 - the first version of a record links just to it's successor
 - a second, third, etc., versions, to their successors and predecessors
 - the final (current) version, just to its predecessor

(A key file entry always points to the final (current) version of a
record.)

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

*That is, data about the record not stored in the record.

The formats and sizes of these fixed-length fields may be configured
when the data store is first defined, and will determine certain
constraints on the size of the data store.  For example, if the file
number is base 10 and 2 bytes in size, then the data store may have
up to 99 data files.  And if the seek position is base 10 and 9 bytes
in size, then each data file may contain up to 1 Gig of data.

Number bases larger than 10 (up to 36 for file numbers and up to 62 for
other numbers) may be used to help shorten the length of the preamble
string.

A data store will have the following files:

 - uri  file,  contains the uri, i.e., the configuration parameters
 - obj  file,  contains dump of perl object constructed from uri
 - toc  files, contain transaction numbers for each data file
 - key  files, contain pointers to every current record version
 - data files, contain all the versions of all the records

The directory structure follows this scheme:

 - dir (the top-level directory, given with C<dir> parm)
   - name.uri, uri file (name as given with C<name> parm)
   - name.obj, obj file
   - name.toc1, first directory containing toc files
     - name.1.toc, first toc file (file/dir numbers start with 1)
     - ... (more toc files)
   - ... (more toc directories)
   - name.key1, first directory containing key files
     - name.1.key, first key file
     - ... (more key files)
   - ... (more key directories)
   - name.data1, first directory containing data files
     - name.1.data
     - ... (more data files)
   - ... (more data directories)

Different datastores may coexist in the same top-level directory--they
just have to have different names.

To retrieve a record, one must know the data file number and the seek
position into that data file, or one must know the record's sequence
number (the order it was added to the data store).  With a sequence
number, the file number and seek position can be looked up in the key
file, so these sequence numbers are called "key numbers" or C<keynum>.

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

=head2 Motivation

Several factors motivated the development of this module:

 - the desire for simple, efficient reading and writing of records
 - the desire to handle any number and size of records
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

Because the size and number of data files is configurable, the data
store should scale up to large numbers of (perhaps large) records.
This while still retaining efficient reading and writing.

(In the extreme case that a record is too large for a single file,
users might break up the record into parts, store them as multiple data
store records and store a "directory" record to guide the reassembly.
While that's outside the scope of this module, that sort of scheme is
accommodated by the fact that the data store doesn't care if the record
data is not a complete unit of a known format.)

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

=head2 CRUD cases

 Create: no previous preamble required or allowed
    - create a record object (with no previous)
    - write the record
    - return the record object
 Retrieve:
    - read a data record
    - create a record object (with a preamble, which may become a previous)
    - return the record object
 Update: previous preamble required (and it must not have changed)
    - create a record object (with a previous preamble)
    - write the record (updating the previous in the data store)
    - return the record object
 Delete: previous preamble required (and it must not have changed)
    - create a record object (with a previous preamble)
    - write the record (updating the previous in the data store)
    - return the record object

Some notes about the "previous" preamble:

In order to protect data from conflicting concurrent updates, you may
not update or delete a record without first retrieving it from the data
store.  Supplying the previous preamble along with the new record data
is proof that you did this.  Before the new record is written, the
supplied previous preamble is compared with what's in the data store,
and if they are not exactly the same, it means that someone else
retrieved and updated/deleted the record between the time you read it
and the time you tried to update/delete it.

So unless you supply a previous preamble and unless the one you supply
matches exactly the one in the data store, your update/delete will not
be accepted--you will have to re-retrieve the new version of the record
(getting a more recent preamble) and apply your updates to it.

=cut

