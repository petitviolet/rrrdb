use std::error::Error;

use crate::rrrdb::{
    schema::*,
    underlying::{DBError, Underlying},
};

pub(crate) struct SchemaStore<'a> {
    db: &'a Underlying,
}

impl<'a> SchemaStore<'a> {
    const METADATA_SUFFIX: &'static str = "_rrrdb_metadata";

    pub fn new(db: &'a Underlying) -> SchemaStore<'a> {
        Self { db }
    }

    pub fn find_schema(&self, database_name: &str) -> Result<Option<Database>, DBError> {
        self.db.get_serialized::<Database>(
            format!("{}{}", database_name, Self::METADATA_SUFFIX).as_ref(),
        )
    }
}
