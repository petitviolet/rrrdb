use parser::Parser;

use crate::rrrdb::underlying::Underlying;

use self::{parser::ParserError, sql::planner::Planner};

mod parser;
mod schema;
mod sql;
mod underlying;

pub struct RrrDB {
    pub(crate) underlying: Underlying,
}

impl RrrDB {
    pub fn new(path: &str) -> Self {
        Self {
            underlying: Underlying::new(path),
        }
    }

    pub fn execute(&mut self, database_name: &str, query: &str) -> DBResult {
      let statement = Parser::parse_sql(query).map_err(|pe: ParserError| { pe.to_string() })?;
      let mut planner: Planner = Planner::new(database_name, &self.underlying, &statement);
      let plan = planner.plan();
      Err(format!("query: {}\nstatement: {:?}\nplan: {:?}", query, statement, plan))
    }
}

pub type DBResult = Result<ResultSet, String>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultSet {
    records: Vec<Record>,
    metadata: ResultMetadata,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Record {
    values: Vec<ColumnValue>,
}
pub type ColumnValue = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultMetadata {
    columns: Vec<ColumnMetadata>,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnMetadata {
    column_name: String,
    column_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run() {
      assertion_execute_select(
        "SELECT id FROM users", 
        Err("hoge".to_string())
      )
    }


    fn assertion_execute_select(sql: &str, expected: DBResult) {
      let mut db = RrrDB::new("./tmp/database");
      let result = db.execute("test_db", sql);
        assert_eq!(result, expected);
    }
}
