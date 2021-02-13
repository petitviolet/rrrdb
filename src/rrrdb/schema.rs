use serde::{Deserialize, Serialize};

pub(crate) mod store;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

macro_rules! define_column_types {
  ($($column_type:ident), *) => {
      #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
      pub(crate) enum ColumnType {
        $($column_type), *
      }
      impl ColumnType {
        pub fn find(s: &str) -> Option<Self> {
          match s {
            $(s if s.to_lowercase() == stringify!($column_type).to_lowercase() => { Some(Self::$column_type) },)
            *
            _ => None,
          }
        }
      }
      impl ToString for ColumnType {
        fn to_string(&self) -> String {
          match self {
            $(ColumnType::$column_type => stringify!($column_type).to_lowercase(),)
            *
          }
        }
      }
  };
}

define_column_types!(Varchar, Integer);
