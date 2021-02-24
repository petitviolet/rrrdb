use std::{borrow::{Borrow, BorrowMut}, collections::HashMap, ops::{Deref, DerefMut}, sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard}, todo};

use rocksdb::{ColumnFamily, DBIterator};
use serde::{de::DeserializeOwned, Serialize};

pub struct Storage {
    rocksdb: Arc<RwLock<rocksdb::DB>>,
}

pub(crate) struct Container<'a, T: 'a>(RwLockWriteGuard<'a, T>);

impl <'a, T: 'a> Container<'a, T> {
  fn get(&self) -> &T {
    &self.0
  }
}
impl<'a, T: 'a> DerefMut for Container<'a, T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
      self.0.borrow_mut()
  }
}
impl<'a, T: 'a> Deref for Container<'a, T> {
  type Target = T;
  fn deref(&self) -> &Self::Target {
      &self.0
  }
}

pub struct RecordIterator<'a> {
    db_iterator: Container<'a, DBIterator<'a>>,
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
        let mut rocksdb = rocksdb::DB::open_default(path).unwrap();
        let cf_name = Namespace::Metadata.cf_name();

        Self::create_cf(&mut rocksdb, cf_name.as_ref());

        Storage { rocksdb: Arc::new(RwLock::new(rocksdb)) }
    }

    fn create_cf(rocksdb: &mut rocksdb::DB, cf_name: &str) -> () {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        rocksdb.create_cf(cf_name, &options);
    }

    fn column_family<'a>(&'a self, namespace: &Namespace) -> Container<'a, ColumnFamily> {
        let cf_name = namespace.cf_name();
        if let Some(cf) = self.rocksdb.read().unwrap().cf_handle(&cf_name) {
          Container(cf)
        } else {
          // Self::create_cf(&mut self.rocksdb, cf_name.as_ref());
          let mut options = rocksdb::Options::default();
          options.create_if_missing(true);
          options.create_missing_column_families(true);
          self.rocksdb.write().unwrap().create_cf(cf_name, &options);
          // self.rocksdb.create_cf(cf_name, todo!());
          self.rocksdb.write().unwrap().cf_handle(&cf_name).unwrap()
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
        // let db_iterator = InnerIterator(self.rocksdb.read().unwrap().iterator_cf(cf, rocksdb::IteratorMode::Start));
        RecordIterator {
            db_iterator: todo!(),
        }
    }

    pub fn get(&self, namespace: &Namespace, key: &str) -> Result<Option<Vec<u8>>, DBError> {
        self.rocksdb
            .read()
            .unwrap()
            .get_cf(self.column_family(namespace), key)
            .map_err(|e| DBError::from(e))
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
            .write().unwrap()
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
