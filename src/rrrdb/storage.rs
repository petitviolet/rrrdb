use std::collections::HashMap;

use rocksdb_factory::RocksDBFactory;
use serde::{de::DeserializeOwned, Serialize};
mod rocksdb_factory;

pub struct Storage {
    factory: RocksDBFactory,
    pub(crate) cfs: HashMap<String, rocksdb::DB>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Namespace {
    Metadata,
    Database(String),
    Table { database_name: String, name: String },
}

impl Namespace {
    pub(crate) fn database(name: &str) -> Namespace {
        Namespace::Database(name.to_string())
    }
    pub(crate) fn table(database_name: &str, name: &str) -> Namespace {
        Namespace::Table {
            database_name: database_name.to_string(),
            name: name.to_string(),
        }
    }
    pub(crate) fn cf_name(&self) -> String {
        match self {
            Namespace::Metadata => String::from("metadata"),
            Namespace::Database(name) => name.to_string(),
            Namespace::Table {
                database_name,
                name,
            } => format!("{}_{}", database_name, name),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBError {
    pub(crate) message: String,
}

impl From<rocksdb::Error> for DBError {
    fn from(e: rocksdb::Error) -> Self {
        Self {
            message: e.into_string(),
        }
    }
}
impl From<String> for DBError {
    fn from(e: String) -> Self {
        Self { message: e }
    }
}

impl Storage {
    pub fn new(path: &str) -> Storage {
        let factory = RocksDBFactory::new(path);
        let cfs = HashMap::new();
        Storage { factory, cfs }
    }

    fn open_cf(&mut self, namespace: &Namespace) -> () {
        let cf_name = namespace.cf_name();
        let cf = self.factory.open_column_family(&cf_name);
        self.cfs.insert(cf_name.to_string(), cf);
        self.cfs.get(&cf_name).unwrap();
    }

    fn db(&self, namespace: &Namespace) -> &rocksdb::DB {
        match self.cfs.get(&namespace.cf_name()) {
            Some(cf) => cf,
            None => panic!("There is no open Column Family for {:?}", namespace),
        }
    }

    pub fn get(&self, namespace: &Namespace, key: &str) -> Result<Option<Vec<u8>>, DBError> {
        self.db(namespace).get(key).map_err(|e| DBError::from(e))
    }

    pub fn get_serialized<T: DeserializeOwned>(
        &self,
        namespace: &Namespace,
        key: &str,
    ) -> Result<Option<T>, DBError> {
        self.get(namespace, key).and_then(|opt| match opt {
            Some(found) => match String::from_utf8(found) {
                Ok(s) => match serde_json::from_str::<T>(&s) {
                    Ok(t) => Ok(Some(t)),
                    Err(err) => Err(DBError::from(format!("Failed to deserialize: {:?}", err))),
                },
                Err(err) => Err(DBError::from(format!("Failed to deserialize: {:?}", err))),
            },
            None => Ok(None),
        })
    }

    pub fn put(&self, namespace: &Namespace, key: &str, value: Vec<u8>) -> Result<(), DBError> {
        self.db(namespace)
            .put(key, value)
            .map_err(|e| DBError::from(e))
    }

    pub fn put_serialized<T: Serialize + std::fmt::Debug>(
        &self,
        namespace: &Namespace,
        key: &str,
        value: T,
    ) -> Result<(), DBError> {
        match serde_json::to_string(&value) {
            Ok(serialized) => self.put(namespace, &key, serialized.into_bytes()),
            Err(err) => Err(DBError::from(format!(
                "Failed to serialize to String. T: {:?}, err: {:?}",
                value, err
            ))),
        }
    }
}
