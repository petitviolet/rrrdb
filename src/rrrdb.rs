use parser::Parser;

use crate::rrrdb::storage::Storage;

use self::{parser::ParserError, sql::planner::Planner};

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
        let statement = Parser::parse_sql(query).map_err(|pe: ParserError| pe.to_string())?;
        let mut planner: Planner = Planner::new(database_name, &self.underlying, &statement);
        let plan = planner.plan();
        Err(format!(
            "query: {}\nstatement: {:?}\nplan: {:?}",
            query, statement, plan
        ))
    }
}

pub type DBResult = Result<ResultSet, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultSet {
    records: Vec<Record>,
    metadata: ResultMetadata,
}
impl ResultSet {
    pub fn new(records: Vec<Record>, metadata: ResultMetadata) -> Self {
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
    field_name: String,
    field_type: String,
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
    use super::{
        schema::{store::SchemaStore, *},
        storage::Storage,
        DBResult, RrrDB,
    };

    #[test]
    fn run() {
        assertion_execute_select("SELECT id FROM users", Err("hoge".to_string()))
    }

    fn assertion_execute_select(sql: &str, expected: DBResult) {
        let mut db = RrrDB::new("./tmp/database");
        let store = SchemaStore::new(&db.underlying);
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
