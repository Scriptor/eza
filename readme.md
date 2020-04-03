# Eza DB:
Eza is a very barebones log-structured kv-store with aspirations to be a slightly-less
barebones log-structured database someday.

Currently it's a basic KV store that provides the ability to set a key, set
multiple keys, and look up the value of a key.

It is currently ACID-compliant (for some definition of ACID compliance :) ):

Atomic     - Currently supported with a primitive write-ahead log that is checked to
             see which transactions have successfully committed.
Consistent - Provided mostly by accident since there's not much that can be
             inconsistent right now.
Isolation  - A rough MVCC implementation offers snapshot isolation. Remaining
             work here is adding a 'PENDING' transaction state and maybe storing
             the transaction info itself on disk rather than keeping it in memory.
Durability - Uses RocksDB for the storage engine to make writes durable.

## Rationale
The project is mainly educational, allowing me to play with different database
implementation concepts and learn Rust at the same time.

## Current Design
Currently all keys and values need to fit in memory as they are placed in an
in-memory hashmap for fast access. Each write operation is written ahead to
a basic write-ahead-log (WAL). Only after RocksDB has confirmed that each key
has been written is a write transaction considered "committed". On startup,
the WAL is scanned to find all committed keys. Then all keys saved by RocksDB
are scanned to determine which ones are committed, before loading them into
the in-memory hashmap.

Eventually I'd like to remove the memory restriction so that you can have more
data than you have RAM. I'd also like to improve the WAL so that it can compress
redundant writes.

## Name
Since this is a primitive log-based database, it's named after the last three letters
of the genus [Wattieza](https://en.wikipedia.org/wiki/Wattieza), which were primitive
woody (i.e. log-structured) trees.
