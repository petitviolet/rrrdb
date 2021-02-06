use std::todo;

use crate::rrrdb::{schema::*, underlying::Underlying};

pub(crate) struct SchemaStore<'a> {
    db: &'a Underlying,
}

impl<'a> SchemaStore<'a> {
    const METADATA_PREFIX: &'static str = "rrrdb_metadata_";

    pub fn new(db: &'a Underlying) -> SchemaStore<'a> {
        Self { db }
    }

    pub fn find_schema(&self, database_name: &str) -> Option<Database> {
        let result = self
            .db
            .get(format!("{}{}", Self::METADATA_PREFIX, database_name).as_ref());
        match result {
            Ok(Some(bytes)) => {}
            Ok(None) => {}
            Err(err) => {}
        }
        todo!()
    }
}
