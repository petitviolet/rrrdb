use std::todo;

use crate::rrrdb::{parser::*, schema::store::SchemaStore, FieldMetadata};
use crate::rrrdb::{schema::*, storage::Storage};

// SQL -> KVS requests
pub(crate) struct Planner<'a> {
    database: Option<Database>,
    schema_store: SchemaStore<'a>,
    sql: Statement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Plan {
    SelectPlan(SelectPlan),
    InsertPlan(InsertPlan),
    CreateDatabasePlan(CreateDatabasePlan),
    CreateTablePlan(CreateTablePlan),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SelectPlan {
    pub(crate) database: Database,
    pub(crate) plans: Vec<SelectTablePlan>,
    pub(crate) projections: Vec<ProjectionPlan>,
    pub(crate) filters: Vec<Filter>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CreateDatabasePlan {
    pub(crate) database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct CreateTablePlan {
    pub(crate) database_name: String,
    pub(crate) table_name: String,
    pub(crate) column_definitions: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct InsertPlan {
    pub(crate) database: Database,
    pub(crate) table: Table,
    pub(crate) values: Vec<RecordValue>,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RecordValue {
    pub(crate) column: Column,
    pub(crate) value: Value,
}

impl SelectPlan {
    pub fn result_metadata(&self) -> Vec<FieldMetadata> {
        let projections = &self.projections;
        let mut field_metadatas = (&self.plans).into_iter().fold(
            vec![],
            |mut metadatas,
             SelectTablePlan {
                 table,
                 select_columns,
                 filter,
             }| {
                select_columns
                    .into_iter()
                    .filter_map(|column| {
                        projections.into_iter().enumerate().find_map(|(idx, p)| {
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
                    })
                    .for_each(|found| {
                        metadatas.push(found);
                    });
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
    pub table_name: String,
    pub column_name: String,
    pub expected_value: Value,
}
impl Filter {
    pub fn new(table_name: String, column_name: String, expected_value: Value) -> Self {
        Self {
            table_name,
            column_name,
            expected_value,
        }
    }
}

impl<'a> Planner<'a> {
    pub fn new(database_name: &str, underlying: &'a mut Storage, sql: Statement) -> Self {
        let schema_store = SchemaStore::new(underlying);
        let database = match schema_store.find_schema(database_name) {
            Ok(database) => database,
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
            Statement::Insert(insert) => self.build_insert_plan(insert.clone()),
            Statement::CreateDatabase(create_database) => {
                self.build_create_database_plan(create_database.clone())
            }
            Statement::CreateTable(create_table) => {
                self.build_create_table_plan(create_table.clone())
            }
        }
    }

    fn build_select_query_plan(&mut self, query: Query) -> Plan {
        let database = self.database.clone().unwrap();
        let mut tables: Vec<Table> = (&query.froms)
            .into_iter()
            .flat_map(|table_name| database.table(&table_name))
            .collect();
        let mut select_plan = SelectPlan {
            database: database.clone(),
            plans: vec![],
            projections: vec![],
            filters: vec![],
        };
        (&query.projections)
            .into_iter()
            .for_each(|projection| match projection {
                Projection::Expression(expr) => match expr {
                    Expression::Ident(ident) => {
                        let (table, column) = (&tables)
                            .into_iter()
                            .find_map(|t| t.column(ident).map(|c| (t, c)))
                            .expect(&format!("Unknown identifier: {}", ident));
                        let projection_plan = ProjectionPlan {
                            table: table.clone(),
                            column: column.clone(),
                        };
                        let select_table_plan = SelectTablePlan {
                            table: table.clone(),
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
                    let table = (&tables)
                        .into_iter()
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
        if let Some(expr) = &query.predicate.expression {
            match expr {
                Expression::BinOperator { lhs, rhs, op } => {
                    match (lhs.as_ref(), rhs.as_ref()) {
                        (Expression::Ident(ident), Expression::Value(value)) => {
                            select_plan.filters.push(self.build_filter(
                                &tables,
                                ident.to_owned(),
                                value.to_owned(),
                            ));
                        }
                        (Expression::Value(value), Expression::Ident(ident)) => {
                            select_plan.filters.push(self.build_filter(
                                &tables,
                                ident.to_owned(),
                                value.to_owned(),
                            ));
                        }
                        _ => todo!("not supported yet expression: {:?}", expr),
                    };
                }
                _ => {
                    // meaningless
                }
            }
        };
        Plan::SelectPlan(select_plan)
    }

    fn build_filter(&self, tables: &Vec<Table>, ident: String, value: Value) -> Filter {
        let x = tables
            .into_iter()
            .find_map(|t| t.column(&ident).map(|c| (t, c)));
        let (table, left_column) = x.expect(&format!(
            "Unknown identifier: {}, value: {:?}",
            ident, value
        ));

        Filter::new(table.name.to_owned(), ident, value)
    }

    fn build_create_database_plan(&mut self, create_database: CreateDatabase) -> Plan {
        Plan::CreateDatabasePlan(CreateDatabasePlan {
            database_name: create_database.name,
        })
    }

    fn build_create_table_plan(&mut self, create_table: CreateTable) -> Plan {
        Plan::CreateTablePlan(CreateTablePlan {
            database_name: create_table.database_name,
            table_name: create_table.table_name,
            column_definitions: create_table.column_definitions,
        })
    }
    fn build_insert_plan(&mut self, insert: Insert) -> Plan {
        let database = self.database.clone().unwrap();
        let table = database
            .table(&insert.table_name)
            .expect(&format!("table {} not found", insert.table_name));
        let values = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                let column = column.to_owned();
                let value = insert
                    .values
                    .get(i)
                    .expect(&format!(
                        "value is missing for column {} in given INSERT INTO statement",
                        &column.name
                    ))
                    .to_owned();
                RecordValue { column, value }
            })
            .collect();
        Plan::InsertPlan(InsertPlan {
            database,
            table,
            values,
        })
    }
}
