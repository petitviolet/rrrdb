use serde::{Deserialize, Serialize};

pub(crate) mod store;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Database {
    pub name: String,
    pub tables: Vec<Table>,
}

impl Database {
    pub fn empty(name: String) -> Self {
        Self {
            name,
            tables: vec![],
        }
    }
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
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }

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

impl Column {
    pub const ID: &'static str = "id";
    pub fn new(name: String, column_type: ColumnType) -> Self {
        Self { name, column_type }
    }
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

impl From<String> for ColumnType {
    fn from(s: String) -> Self {
        match s.as_ref() {
            "string" => ColumnType::Varchar,
            "varchar" => ColumnType::Varchar,
            "int" => ColumnType::Integer,
            "integer" => ColumnType::Integer,
            default => panic!("unexpected type({}) was given", default),
        }
    }
}
