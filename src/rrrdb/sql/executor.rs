use std::{ops::Deref, todo};

use storage::Namespace;

use crate::rrrdb::{
    storage::{self, Storage},
    DBResult, FieldMetadata, Record, ResultMetadata, ResultSet,
};

use super::planner::{Plan, ProjectionPlan, SelectPlan, SelectTablePlan};

pub(crate) struct Executor<'a> {
    storage: &'a Storage,
    plan: Plan,
}

impl<'a> Executor<'a> {
    pub fn new(storage: &'a Storage, plan: Plan) -> Self {
        Self { storage, plan }
    }

    pub fn execute(&mut self) -> DBResult {
        match &self.plan {
            Plan::SelectPlan(select_plan) => self.execute_select(select_plan.clone()),
            Plan::InsertPlan {} => todo!(""),
        }
    }

    fn execute_select(&mut self, select_plan: SelectPlan) -> DBResult {
        let field_metadatas: Vec<FieldMetadata> = select_plan.result_metadata();
        let namespaces: Vec<Namespace> = (&select_plan.plans)
            .clone()
            .into_iter()
            .map(|p| Namespace::table(&select_plan.database.name, &p.table.name))
            .collect();
        // TODO: concurrent
        let records = self
            .storage
            .iterator(&namespaces[0]) // iterate over given namespace(table)
            .map(|(key, value_bytes)| {
                match String::from_utf8(value_bytes.into_vec())
                    .map(|j| serde_json::from_str::<serde_json::Value>(&j))
                {
                    Ok(Ok(json)) => {
                        let row: &serde_json::Map<String, serde_json::Value> =
                            &json.as_object().unwrap().to_owned();
                        let field_values: Vec<Vec<u8>> = (&field_metadatas)
                            .into_iter()
                            .map(|meta| {
                                let found =
                                    row.into_iter().find_map(|(column_name, column_value)| {
                                        if column_name.deref() == meta.field_name {
                                            Some(column_value.as_str().unwrap().as_bytes().to_vec())
                                        } else {
                                            None
                                        }
                                    });
                                found.expect(&format!(
                                    "field({:?}) not found in a row({:?})",
                                    meta, row
                                ))
                            })
                            .collect();
                        Record::new(field_values)
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
        Ok(result_set)
    }
}
