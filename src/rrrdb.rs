use crate::rrrdb::underlying::Underlying;

mod parser;
mod underlying;

pub struct RrrDB {
    underlying: Underlying,
}

impl RrrDB {
    pub fn new(path: &str) -> Self {
        Self {
            underlying: Underlying::new(path),
        }
    }

    pub fn execute<T>(query: &str) -> DBResult {
        Ok(())
    }
}

pub type DBResult = Result<(), String>;

pub struct ResultSet {
    records: Vec<Record>,
    metadata: ResultMetadata,
}
pub struct Record {
    values: Vec<ColumnValue>,
}
pub type ColumnValue = Vec<u8>;

pub struct ResultMetadata {
    columns: Vec<ColumnMetadata>,
}
pub struct ColumnMetadata {
    column_name: String,
    column_type: String,
}
