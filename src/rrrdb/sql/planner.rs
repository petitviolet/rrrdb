use std::todo;

use crate::rrrdb::{parser::*, schema::store::SchemaStore, FieldMetadata};
use crate::rrrdb::{schema::*, storage::Storage};

// SQL -> KVS requests
pub(crate) struct Planner<'a> {
    database: Database,
    schema_store: SchemaStore<'a>,
    sql: &'a Statement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Plan {
    SelectPlan(SelectPlan),
    InsertPlan {},
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SelectPlan {
    pub(crate) database: Database,
    pub(crate) plans: Vec<SelectTablePlan>,
    pub(crate) projections: Vec<ProjectionPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ProjectionPlan {
    pub(crate) table: Table,
    pub(crate) column: Column,
    // expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SelectTablePlan {
    pub(crate) table: Table,
    pub(crate) select_columns: Vec<Column>,
    pub(crate) filter: Option<Filter>,
}

impl SelectPlan {
    pub fn result_metadata(&self) -> Vec<FieldMetadata> {
        let mut field_metadatas = (&self.plans).into_iter().fold(
            vec![],
            |metadatas,
             SelectTablePlan {
                 table,
                 select_columns,
                 filter,
             }| {
                let found = select_columns.into_iter().find_map(|column| {
                    self.projections
                        .into_iter()
                        .enumerate()
                        .find_map(|(idx, p)| {
                            if p.table.name == table.name && p.column.name == column.name {
                                let metadata = FieldMetadata::new(
                                    &column.name,
                                    &column.column_type.to_string(),
                                );
                                Some((idx, metadata))
                            } else {
                                None
                            }
                        })
                });
                match found {
                    Some(found) => {
                        metadatas.push(found);
                    }
                    None => panic!(
                        "SelectTablePlans({:?}) don't match ProjectionPlans({:?})",
                        self.plans, self.projections
                    ),
                }
                metadatas
            },
        );
        field_metadatas.sort_by(|(idx_0, _), (idx_1, _)| idx_0.cmp(idx_1));
        field_metadatas
            .into_iter()
            .map(|(_, metadata)| metadata)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Filter {
    f: fn(Table) -> bool,
}

impl<'a> Planner<'a> {
    pub fn new(database_name: &str, underlying: &'a Storage, sql: &'a Statement) -> Self {
        let schema_store = SchemaStore::new(underlying);
        let database = match schema_store.find_schema(database_name) {
            Ok(Some(database)) => database,
            Ok(None) => todo!("database {} doesn't exist", database_name),
            Err(err) => panic!("Unexpected error failed {:?}", err),
        };

        Self {
            database,
            schema_store,
            sql,
        }
    }

    pub fn plan(&mut self) -> Plan {
        match &self.sql {
            Statement::Select(query) => self.build_select_query_plan(query.clone()),
            Statement::Insert(insert) => todo!(),
        }
    }

    fn build_select_query_plan(&mut self, query: Query) -> Plan {
        let mut tables = (&query.froms)
            .into_iter()
            .flat_map(|table_name| self.database.table(&table_name));
        let mut select_plan = SelectPlan {
            database: self.database.clone(),
            plans: vec![],
            projections: vec![],
        };
        (&query.projections)
            .into_iter()
            .for_each(|projection| match projection {
                Projection::Expression(expr) => match expr {
                    Expression::Ident(ident) => {
                        let (table, column) = tables
                            .find_map(|t| t.column(ident).map(|c| (t, c)))
                            .expect(&format!("Unknown identifier: {}", ident));
                        let projection_plan = ProjectionPlan {
                            table: table.clone(),
                            column: column.clone(),
                        };
                        let select_table_plan = SelectTablePlan {
                            table,
                            select_columns: vec![column],
                            filter: None,
                        };
                        select_plan.projections.push(projection_plan);
                        select_plan.plans.push(select_table_plan);
                    }
                    Expression::Value(value) => {
                        // not supported yet
                        todo!()
                    }
                    Expression::BinOperator { lhs, rhs, op } => {
                        // not supported yet
                        todo!()
                    }
                },
                Projection::Wildcard => {
                    assert!(query.froms.len() == 1);
                    let table = tables
                        .next()
                        .expect(&format!("table not found for {}", query.froms[0]));
                    let projection_plans = (&table.columns).into_iter().map(|c| ProjectionPlan {
                        table: table.clone(),
                        column: c.clone(),
                    });
                    let columns = table.columns.clone();
                    let select_table_plan = SelectTablePlan {
                        table: table.clone(),
                        select_columns: columns,
                        filter: None,
                    };
                    select_plan.projections = projection_plans.collect();
                    select_plan.plans.push(select_table_plan);
                }
            });
        Plan::SelectPlan(select_plan)
    }
}
