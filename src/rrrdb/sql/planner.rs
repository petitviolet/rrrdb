use std::todo;

use crate::rrrdb::parser::*;
use crate::rrrdb::schema::*;

// SQL -> KVS requests
pub(crate) struct Planner {
    sql: Statement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Plan {
    SelectPlan {
        plans: Vec<SelectTablePlan>,
        projections: Vec<ProjectionPlan>,
    },
    InsertPlan {},
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionPlan {
    table: Table,
    column: String,
    // expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SelectTablePlan {
    table: Table,
    select_columns: Vec<String>,
    filter: Filter,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Filter {
    table: Table,
    f: fn() -> bool,
}

impl Planner {
    pub fn new(sql: Statement) -> Self {
        Self { sql }
    }
    pub fn build(self) -> Plan {
        match self.sql {
            Statement::Select(query) => Self::build_select_query_plan(query),
            Statement::Insert(insert) => todo!(),
        }
    }

    fn build_select_query_plan(query: Query) -> Plan {
        let table_schemas = query.froms.into_iter().map(|table| table);
        todo!()
    }
}
