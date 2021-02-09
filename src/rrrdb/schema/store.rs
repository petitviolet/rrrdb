use std::error::Error;

use crate::rrrdb::{
    schema::*,
    storage::{DBError, Storage},
};

pub(crate) struct SchemaStore<'a> {
    db: &'a Storage,
}

impl<'a> SchemaStore<'a> {
    const METADATA_SUFFIX: &'static str = "_rrrdb_metadata";

    pub fn new(db: &'a Storage) -> SchemaStore<'a> {
        Self { db }
    }

    pub fn find_schema(&self, database_name: &str) -> Result<Option<Database>, DBError> {
        self.db.get_serialized::<Database>(
            format!("{}{}", database_name, Self::METADATA_SUFFIX).as_ref(),
        )
    }

    pub fn save_schema(&self, database: Database) -> Result<(), DBError> {
        let key = format!("{}{}", &database.name, Self::METADATA_SUFFIX);
        self.db.put_serialized(&key, database)
    }
}
