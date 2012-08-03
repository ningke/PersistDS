Persistent Data Structures
==========================

# Create persistent (as in immutable) data structures that also automatically persists on permanent storage aka disk.

## Introduction

I was building an ngram database in the form of a Trie data structure,
the size of which, being more than 100 GB by my estimate, would not
fit in the RAM of my desktop machine. This means that I have to put
the data structure on disk and only work with the part of the data
structure that can fit in memory. The solution to this problem is to
break up the data into small chunks and make a number of small trie
data structures, marshal each trie onto disk, and finally merge all the
parts. When merging, I can't unmarshal the whole trie because as parts
merge, it gets larger. Instead, I have to read only the trie nodes
that I need at the moment (along the search path) into memory and work
with those. This means I can't use Python's pickling mechanism as is
since it reads/writes the whole data structure at a time.

Wouldn't it be nice if I could automate the marshal/unmarshal work and
work with the data structure as I would if it fits in RAM? I wrote
PersistDS to do just that - the library writes data structure to disk
automatically so it always persists (on disk), it also reads a "node"
(e.g. that of a tree or list) of the data structure into memory when
program needs it, all without manual intervention. With the PersistDS
library, a programmer can concentrate on building data structures,
without worrying about memory constraints, no matter how big the data
structure is.

## How does it work

In a normal program, an object is created in memory and the running
program uses a reference to access that memory. In the PersistDS
library, an object, after being created in memory, is also written to
disk. So an object reference actually refers to a piece of storage on
disk. Since a computer program cannot directly access disk (there is
no CPU instructions for loading/storing to a disk location), the
object must be read into memory whenever it is needed by the
program. So an object reference in PersistDS indirectly refers a piece
of disk storage through RAM. This object reference is called an
OID. The OID is a persistent object reference - it contains a record
index (a file offset) for the object on disk, access to the persistent
object is arbitrated by the PersistDS API.

It is important to point out that a persistent object record
constructed in this fashion can contain references to other object
records. After all, the PDS API is designed to present the storage as
some "unlimited" RAM to a running program. Because persistent objects
can reference each other, it is possible to constructed "linked" data
structures such as linked lists, trees, and graphs. Contrast this to a
database record, which is addressed by a key and does not usually
reference another record.

Some pictures to illustrate the workings of PDS:

Create an object:  Fields of an Object ==> OID
--------------------------------------------------------------------------
                    Memory                  |        Disk
--------------------------------------------------------------------------
Running Program      |      PersistDS       |  Storage Protocol(e.g. File)
--------------------------------------------------------------------------
                     |                      |
create object     ------> Pack object    ------>     Write to File
     ^               |                      |              |
   return         <------ Create OID     <------   record location
                     |                      |
--------------------------------------------------------------------------

Retrieve an object:  OID => Fields of an Object
--------------------------------------------------------------------------
                    Memory                  |        Disk
--------------------------------------------------------------------------
Running Program      |      PersistDS       |  Storage Protocol(e.g. File)
--------------------------------------------------------------------------
                     |                      |
retrieve object   ------> record location ----->   Read File
  w/ OID             |                      |          |
                     |                      |          V
 object fields    <------ Unpack record  <------    get record
                     |                      |
--------------------------------------------------------------------------

PersistDS only support immutable (aka functional) data
structures. Because of this, once a persistent object is created, it
will never change, and the actual object record on disk will never
change either. This means that the object storage protocol doesn't
need to support update. Not supporting update makes it easy to
allocate new space, new record can simply be appended to the
end. Number of seeks are reduced as well.

So how can the PersistDS library (PDS) achieve decent performance
given that objects are stored on disk - a medium that is not only
indirectly accessible by CPU but also orders of magnitudes slower than
main memory? The answer is to use a cache, or a buffer. When an object
is created, it is stored in a object cache (PDSCache) and the write
(to disk) is delayed as long as there is still space in the
cache. When a new object needs a cache slot and can't find any because
cache is full, the least recently used (LRU) entry is flushed to the
storage. Similarly, when a program try to retrieve an object (OID
read) and can't find it in the cache, the PDS library will read it
from storage and cache the object before returning it. The PDSCache
doesn't need to be big, it should roughly match the size of the
"working set" of the running program, i.e., the size of the current
stack and global address space.

## PersistDS (PDS) library API

## PersistDS source code
 
## License
Apache License Version 2.0
