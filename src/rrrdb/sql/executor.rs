use std::todo;

use storage::Namespace;

use crate::rrrdb::{
    storage::{self, Storage},
    DBResult,
};

use super::planner::{Plan, SelectPlan, SelectTablePlan};

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
        let projection_plans = &select_plan.projections;
        (&select_plan.plans).into_iter().map(
            |SelectTablePlan {
                 table,
                 select_columns,
                 filter,
             }| {
                let values = select_columns.into_iter().flat_map(|column| {
                    let namespace = Namespace::table(&select_plan.database.name, &table.name);
                    // TODO: concurrent
                    self.storage
                        .iterator(&namespace)
                        .map(|(key, value_bytes)| {});
                    vec![1]
                });
                todo!()
            },
        );
        todo!()
    }
}
