use parser::Parser;

use crate::rrrdb::storage::Storage;

use self::{parser::ParserError, sql::executor::Executor, sql::planner::Planner};

mod parser;
mod schema;
mod sql;
mod storage;

pub struct RrrDB {
    pub(crate) underlying: Storage,
}

impl RrrDB {
    pub fn new(path: &str) -> Self {
        Self {
            underlying: Storage::new(path),
        }
    }

    pub fn execute(&mut self, database_name: &str, query: &str) -> DBResult {
        let plan = {
            let statement = Parser::parse_sql(query).map_err(|pe: ParserError| pe.to_string())?;
            let mut planner: Planner = Planner::new(database_name, &mut self.underlying, statement);
            planner.plan()
        };
        let mut executor = Executor::new(&mut self.underlying, plan);
        executor.execute()
    }
}

pub type DBResult = Result<ResultSet, DBError>;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBError {
    pub(crate) message: String,
}
impl DBError {
    pub(crate) fn namespace_not_found(namespace: &storage::Namespace) -> Self {
        Self {
            message: format!("ColumnFamily({}) not found", namespace.cf_name()),
        }
    }
}

impl From<rocksdb::Error> for DBError {
    fn from(e: rocksdb::Error) -> Self {
        Self {
            message: e.into_string(),
        }
    }
}
impl From<String> for DBError {
    fn from(e: String) -> Self {
        Self { message: e }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultSet {
    records: Vec<Record>,
    metadata: ResultMetadata,
}
impl ResultSet {
    pub fn new(records: Vec<Record>, metadata: ResultMetadata) -> Self {
        assert_eq!(
            records.len(),
            metadata.fields.len(),
            "records and metadata are inconsistent. records = {:?}, metadata = {:?}",
            records,
            metadata
        );
        Self { records, metadata }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Record {
    values: Vec<FieldValue>,
}
impl Record {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }
}
pub type FieldValue = Vec<u8>;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultMetadata {
    fields: Vec<FieldMetadata>,
}
impl ResultMetadata {
    pub fn new(field_metadatas: Vec<FieldMetadata>) -> Self {
        Self {
            fields: field_metadatas,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldMetadata {
    pub(crate) field_name: String,
    pub(crate) field_type: String,
}

impl FieldMetadata {
    pub fn new(name: &str, _type: &str) -> Self {
        Self {
            field_name: name.to_string(),
            field_type: _type.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DBError, DBResult, RrrDB, schema::{store::SchemaStore, *}};

    #[test]
    fn run() {
        assertion_execute_select("SELECT id FROM users", Err(DBError { message: "hoge".to_string() }))
    }

    fn assertion_execute_select(sql: &str, expected: DBResult) {
        let mut db = RrrDB::new("./tmp/database");
        let mut store = SchemaStore::new(&mut db.underlying);
        let database = Database {
            name: String::from("test_db"),
            tables: vec![Table {
                name: String::from("users"),
                columns: vec![Column {
                    name: String::from("id"),
                    column_type: ColumnType::Integer,
                }],
            }],
        };
        store.save_schema(database).expect("failed to save schema");
        let result = db.execute("test_db", sql);
        assert_eq!(result, expected);
    }
}
