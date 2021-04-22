# rrrdb: RDB built on top of RocksDB written in Rust

Under construction

See [rust-rocksdb](https://crates.io/crates/rocksdb)

## How to Use

```rust
let rrrdb = RrrDB::new(path)
rrrdb.execute("test_db", "CREATE TABLE users (id integer, name varchar)").unwrap(); // should handle properly
rrrdb.execute("test_db", "INSERT INTO users VALUES (1, 'Alice')").unwrap();
rrrdb.execute("test_db", "INSERT INTO users VALUES (2, 'Bob')").unwrap();
let result = rrrdb.execute("test_db", "SELECT name FROM users WHERE id = 2").unwrap();
result == 
    OkDBResult::SelectResult(ResultSet::new(
        vec![
            Record::new(vec![
                FieldValue::Text("Bob".to_string()),
            ]),
        ],
        ResultMetadata::new(vec![
            FieldMetadata::new("name", "varchar")
        ])
    ))
```

## Feature

### SQL

Suported SQLs are like:

- `CREATE TABLE users (id integer, name varchar)`
- `INSERT INTO users VALUES (1, 'Alice')`
- `SELECT * FROM users`
- `SELECT name FROM users WHERE id = 2`

So, I'd say it's a tiny subset of SQL supported.

### Supported Type

Basically, Int and String are supported.
Boolean will be there soon, hopefully.

## Design

### Layered

- SQL layer
    - tokenize and parse given SQLs
- Planner
    - compile a given SQL into a Plan that represents how to fetch data from storage layer, which is RocksDB
- Executor
    - fetch data based on a given Plan
- Storage
    - can be considered RocksDB wrapper
    - get a record by a key
    - get a iterator on a ColumnFamily
    - put a record with a key

### Storage Layer

Use RocksDB as a backend storage layer to persist actual data.
RocksDB is a Key-Value store, and RrrDB is row-oriented database on top of RocksDB.

Using ColumnFamily of RocksDB to store:

- Metadata
    - ColumnFamily name: "metadata"
    - key: database name, value: database schema(JSON)
- Database
    - not in used
- Table
    - Stores records in a particular table
    - ColumnFamily name: `<database_name>_<table_name>`
    - key: primary key, value: record(JSON)

These are obviously too naive, but works.

## License

Apache License 2.0
