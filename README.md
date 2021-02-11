# rrrdb: RDB built on top of RocksDB written in Rust

Under construction

See [rust-rocksdb](https://crates.io/crates/rocksdb)

## Design

### Storage Layer

Built on top of RocksDB.
Row-oriented database.
Using ColumnFamily of RocksDB to store:

- Metadata
    - ColumnFamily name: _rrrrdb_metadata
    - key: database name, value: database schema(JSON)
- Records of a table
    - ColumnFamily name: `<database_name>_<table_name>_records`
    - key: primary key, value: record(JSON)

These are obviously too naive, but works.