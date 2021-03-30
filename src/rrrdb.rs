use parser::Parser;

use crate::rrrdb::schema::ColumnType;
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
            let statement = Parser::parse_sql(Some(database_name.to_string()), query)
                .map_err(|pe: ParserError| pe.to_string())?;
            let mut planner: Planner = Planner::new(database_name, &mut self.underlying, statement);
            planner.plan()
        };
        let mut executor = Executor::new(&mut self.underlying, plan);
        executor.execute()
    }
}

pub type DBResult = Result<OkDBResult, DBError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OkDBResult {
    SelectResult(ResultSet),
    ExecutionResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBError {
    pub(crate) message: String,
}
impl DBError {
    pub(crate) fn new(message: String) -> Self {
        Self { message }
    }
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
        Self { records, metadata }
    }
    pub fn get(&self, index: usize) -> Option<&Record> {
        self.records.get(index)
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
    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldValue {
    Bytes(Vec<u8>),
    Int(i64),
    Text(String),
}

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
    field_type: String,
}

impl FieldMetadata {
    pub fn new(name: &str, _type: &str) -> Self {
        Self {
            field_name: name.to_string(),
            field_type: _type.to_string(),
        }
    }
    pub(crate) fn field_type(&self) -> ColumnType {
        ColumnType::from(self.field_type.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        thread::{self, sleep},
        time,
    };

    use super::{
        schema::{store::SchemaStore, *},
        *,
    };

    #[test]
    fn run() {
        let mut rrrdb = build_crean_database();
        rrrdb
            .execute("test_db", "CREATE TABLE users (id integer, name varchar)")
            .unwrap();
        rrrdb
            .execute("test_db", "INSERT INTO users VALUES (1, 'Alice')")
            .unwrap();
        let result = rrrdb.execute("test_db", "SELECT * FROM users").unwrap();
        assert_eq!(
            result,
            OkDBResult::SelectResult(ResultSet::new(
                vec![],
                ResultMetadata::new(vec![
                    FieldMetadata::new("id", "integer"),
                    FieldMetadata::new("name", "varchar")
                ])
            ))
        );
    }

    fn build_crean_database() -> RrrDB {
        let path = "./test_tmp_database";
        if Path::new(path).exists() {
            std::fs::remove_dir_all(path).unwrap();
            thread::sleep(time::Duration::from_millis(100));
        }
        std::fs::create_dir_all(path).unwrap();
        RrrDB::new(path)
    }
}
