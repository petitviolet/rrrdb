use std::{collections::HashMap, iter::Map, ops::Deref, todo};

use storage::Namespace;

use crate::rrrdb::{storage::Storage, *};

use super::super::schema::store::SchemaStore;
use super::super::schema::*;
use super::planner::*;

pub(crate) struct Executor<'a> {
    storage: &'a mut Storage,
    plan: Plan,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a mut Storage, plan: Plan) -> Self {
        Self { storage, plan }
    }

    pub fn execute(&mut self) -> DBResult {
        match &self.plan {
            Plan::SelectPlan(select_plan) => self.execute_select(select_plan.clone()),
            Plan::InsertPlan(insert_plan) => self.execute_insert(insert_plan.clone()),
            Plan::CreateDatabasePlan(create_database_plan) => {
                self.execute_create_database(create_database_plan.clone())
            }
            Plan::CreateTablePlan(create_table_plan) => {
                self.execute_create_table(create_table_plan.clone())
            }
        }
    }

    fn execute_select(&mut self, select_plan: SelectPlan) -> DBResult {
        let field_metadatas: Vec<FieldMetadata> = select_plan.result_metadata();
        println!("[execute_select] field_metadatas: {:?}", field_metadatas);
        let table = &select_plan.plans.get(0).unwrap().table;
        let namespace = Namespace::table(&select_plan.database.name, &table.name);
        // support only one table
        // let namespaces: Vec<Namespace> = (&select_plan.plans)
        //     .clone()
        //     .into_iter()
        //     .map(|p| Namespace::table(&select_plan.database.name, &p.table.name))
        //     .collect();

        // TODO: concurrent
        let filters: Vec<Filter> = select_plan
            .filters
            .into_iter()
            .filter(|f| f.table_name == table.name)
            .collect();
        let iterator = self.storage.iterator(&namespace)?; // iterate over given namespace(table)

        let records = iterator
            .filter_map(|(key, value_bytes)| {
                match String::from_utf8(value_bytes.into_vec())
                    .map(|j| serde_json::from_str::<serde_json::Value>(&j))
                {
                    Ok(Ok(json)) => {
                        let record = Self::parse_single_row(&table, json);
                        println!(
                            "field_metadatas: {:?}, record: {:?}",
                            field_metadatas, record
                        );
                        if let Some(filter) = Self::apply_filter(&filters, &record) {
                            println!("skipped by filter {:?}. record = {:?}", filter, record);
                            return None;
                        };

                        Some(Self::build_record(&field_metadatas, &record))
                    }
                    Ok(Err(err)) => {
                        panic!(
                            "unexpected formatted row for key({:?}). err = {:?}",
                            key, err
                        );
                    }
                    Err(err) => {
                        panic!(
                            "unexpected formatted row for key({:?}). err = {:?}",
                            key, err
                        );
                    }
                }
            })
            .collect();
        let result_set = ResultSet::new(records, ResultMetadata::new(field_metadatas));
        Ok(OkDBResult::SelectResult(result_set))
    }

    fn parse_single_row(table: &Table, json: serde_json::Value) -> HashMap<String, FieldValue> {
        let row: &serde_json::Map<String, serde_json::Value> =
            &json.as_object().unwrap().to_owned();
        row.into_iter()
            .fold(HashMap::new(), |mut map, (column_name, column_value)| {
                let bytes = column_value.as_str().unwrap().as_bytes().to_vec();
                match table.column(column_name).unwrap().column_type {
                    ColumnType::Integer => {
                        let int = String::from_utf8(bytes).unwrap().parse::<i64>().unwrap();
                        map.insert(column_name.to_owned(), FieldValue::Int(int));
                    }
                    ColumnType::Varchar => {
                        let s = String::from_utf8(bytes).unwrap();
                        map.insert(column_name.to_owned(), FieldValue::Text(s));
                    }
                }
                map
            })
    }

    fn apply_filter(filters: &Vec<Filter>, record: &HashMap<String, FieldValue>) -> Option<Filter> {
        filters
            .into_iter()
            .find(|filter| {
                let value: &FieldValue = record
                    .get(&filter.column_name)
                    .expect(&format!("column was not found in record {:?}", record));
                match (value, &filter.expected_value) {
                    (FieldValue::Int(i), parser::Value::Number(n)) => {
                        &n.parse::<i64>().unwrap() != i
                    }
                    (FieldValue::Text(t), parser::Value::QuotedString(s)) => t != s,
                    _ => {
                        println!(
                            "unexpected combination. value: {:?}, expected: {:?}",
                            value, filter.expected_value
                        );
                        true
                    }
                }
            })
            .map(|f| f.to_owned())
    }

    fn build_record(
        field_metadatas: &Vec<FieldMetadata>,
        record: &HashMap<String, FieldValue>,
    ) -> Record {
        let field_values: Vec<FieldValue> = field_metadatas
            .into_iter()
            .filter_map(|meta| {
                record.into_iter().find_map(|(column_name, field_value)| {
                    if column_name.deref() == meta.field_name {
                        Some(field_value.to_owned())
                    } else {
                        None
                    }
                })
            })
            .collect();
        Record::new(field_values)
    }

    fn execute_create_database(&mut self, create_database: CreateDatabasePlan) -> DBResult {
        // nothing to do
        Ok(OkDBResult::ExecutionResult)
    }

    fn execute_create_table(&mut self, create_table: CreateTablePlan) -> DBResult {
        let database_name = create_table.database_name;

        // create a dedicated column family
        let cf_name = format!("{}_{}", database_name, create_table.table_name);
        self.storage.create_column_family(cf_name.as_ref())?;

        // store the schema
        let columns: Vec<Column> = create_table
            .column_definitions
            .into_iter()
            .map(|column| Column::new(column.name, ColumnType::from(column.column_type)))
            .collect();
        let table = Table::new(create_table.table_name.to_string(), columns);
        let mut store = SchemaStore::new(&mut self.storage);
        store
            .create_table(database_name.as_ref(), table)
            .map(|_| OkDBResult::ExecutionResult)
    }

    fn execute_insert(&mut self, insert_plan: InsertPlan) -> DBResult {
        let InsertPlan {
            database,
            table,
            values,
        } = insert_plan;

        let namespace = &Namespace::table(&database.name, &table.name);
        let id = values
            .iter()
            .find(|value| value.column.name == Column::ID)
            .expect(&format!(
                "value for 'id' column is missing. table: {:?}, values: {:?}",
                table, values
            ));

        match id.value.to_string_opt() {
            Some(ref id) => {
                let mut map = HashMap::with_capacity(values.len());
                values.into_iter().for_each(|v| {
                    map.insert(v.column.name, v.value.to_string());
                });
                let serialized = serde_json::to_string(&map)
                    .map_err(|err| DBError::new(format!("failed to serialize. err: {:?}", err)))?;
                self.storage.put(namespace, id, serialized.into_bytes())?;
                Ok(OkDBResult::ExecutionResult)
            }
            None => Err(DBError::new(format!(
                "id not found in the given INSERT INTO statement. table: {:?}, values: {:?}",
                table, values
            ))),
        }
    }
}
