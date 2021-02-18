use std::{collections::HashMap, ops::Deref, todo};

use rocksdb::{ColumnFamily, DBIterator};
use rocksdb_factory::RocksDBFactory;
use serde::{de::DeserializeOwned, Serialize};
mod rocksdb_factory;

pub struct Storage {
    factory: RocksDBFactory,
    rocksdb: rocksdb::DB,
}

pub struct RecordIterator<'a> {
    db_iterator: DBIterator<'a>,
}
impl<'a> Iterator for RecordIterator<'a> {
    type Item = (String, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.db_iterator.next();
        n.map(|(key, value)| {
            let string_key = String::from_utf8(key.to_vec())
                .expect(&format!("Invalid key was found. key: {:?}", key));
            (string_key, value)
        })
    }
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
        let mut default_db = factory.open_default();
        let cf_name = Namespace::Metadata.cf_name();
        factory.open_column_family(&mut default_db, &cf_name);
        Storage { factory, rocksdb: default_db }
    }

    fn column_family(&mut self, namespace: &Namespace) -> &ColumnFamily { 
        let cf_name = namespace.cf_name();
        match (&self.rocksdb).cf_handle(&cf_name) {
          Some(cf) => cf,
          None => {
            self.factory.open_column_family(&mut self.rocksdb, &cf_name);
            // self.rocksdb.create_cf(cf_name, todo!());
            self.rocksdb.cf_handle(&cf_name).unwrap()
          }
        }
    }

    // fn db<'a>(&'a self, namespace: &Namespace) -> &'a rocksdb::DB {
    //     match self.cfs.get(&namespace.cf_name()) {
    //         Some(db) => db,
    //         None => {
    //             panic!("There is no open Column Family for {:?}", namespace)
    //         }
    //     }
    // }

    // fn db<'a>(&'a mut self, namespace: &Namespace) -> &'a rocksdb::DB {
    //     let key = namespace.cf_name();
    //     self.cfs
    //         .entry(key.clone())
    //         .or_insert(self.factory.open_column_family(self.default_db, &key))
    // }

    // pub fn iterate<'a>(&'a self, namespace: &Namespace) -> DBIterator<'a> {
    pub fn iterator<'a>(&'a mut self, namespace: &Namespace) -> RecordIterator<'a> {
        let cf = self.column_family(namespace);
        RecordIterator {
            db_iterator: self.rocksdb.iterator_cf(cf, rocksdb::IteratorMode::Start),
        }
    }

    pub fn get(&mut self, namespace: &Namespace, key: &str) -> Result<Option<Vec<u8>>, DBError> {
        self.rocksdb.get_cf(self.column_family(namespace), key).map_err(|e| DBError::from(e))
    }

    pub fn get_serialized<T: DeserializeOwned>(
        &mut self,
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

    pub fn put(&mut self, namespace: &Namespace, key: &str, value: Vec<u8>) -> Result<(), DBError> {
        self.rocksdb
            .put_cf(self.column_family(namespace), key, value)
            .map_err(|e| DBError::from(e))
    }

    pub fn put_serialized<T: Serialize + std::fmt::Debug>(
        &mut self,
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
