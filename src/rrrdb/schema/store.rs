use std::error::Error;

use crate::rrrdb::{
    schema::*,
    storage::{Namespace, Storage},
    DBError,
};

pub(crate) struct SchemaStore<'a> {
    db: &'a mut Storage,
}

impl<'a> SchemaStore<'a> {
    const SCHEMA_SUFFIX: &'static str = "_schema";

    pub fn new(db: &'a mut Storage) -> SchemaStore<'a> {
        Self { db }
    }

    pub fn find_schema(&self, database_name: &str) -> Result<Option<Database>, DBError> {
        self.db.get_serialized::<Database>(
            &Namespace::database(database_name),
            format!("{}{}", database_name, Self::SCHEMA_SUFFIX).as_ref(),
        )
    }

    pub fn save_schema(&mut self, database: Database) -> Result<(), DBError> {
        let key = format!("{}{}", &database.name, Self::SCHEMA_SUFFIX);
        self.db.put_serialized(&Namespace::Metadata, &key, database)
    }

    pub fn create_table(&mut self, database_name: &str, table: Table) -> Result<(), DBError> {
        let mut schema = self
            .find_schema(database_name)?
            .unwrap_or(Database::empty(database_name.to_string()));
        match schema.table(table.name.as_ref()) {
            Some(tbl) => Err(DBError::new(format!(
                "table already exist. table = {:?}",
                tbl
            ))),
            None => {
                schema.tables.push(table);

                let key = format!("{}{}", schema.name, Self::SCHEMA_SUFFIX);
                self.db.put_serialized(&Namespace::Metadata, &key, schema)
            }
        }
    }
}
