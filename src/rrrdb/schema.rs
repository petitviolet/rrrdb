use std::ops::Deref;

pub(crate) mod store;
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Database {
    pub name: String,
    pub tables: Vec<Table>,
}

impl Database {
    pub fn table(&self, table_name: &str) -> Option<Table> {
        (&self.tables).into_iter().find_map(|table| {
            if table.name == table_name {
                Some(table.clone())
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn column(&self, column_name: &str) -> Option<Column> {
        (&self.columns).into_iter().find_map(|column| {
            if column.name == column_name {
                Some(column.clone())
            } else {
                None
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ColumnType {
    Varchar,
    Integer,
}
