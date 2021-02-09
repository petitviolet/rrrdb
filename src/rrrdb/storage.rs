use rocksdb::{ColumnFamilyDescriptor, DB};
use serde::{de::DeserializeOwned, Serialize};

pub struct Storage {
    pub(crate) db: rocksdb::DB,
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
        Storage {
            db: DB::open_default(path).unwrap(),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, DBError> {
        self.db.get(key).map_err(|e| DBError::from(e))
    }

    pub fn get_serialized<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, DBError> {
        self.get(key).and_then(|opt| match opt {
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

    pub fn put(&self, key: &str, value: Vec<u8>) -> Result<(), DBError> {
        self.db.put(key, value).map_err(|e| DBError::from(e))
    }

    pub fn put_serialized<T: Serialize + std::fmt::Debug>(
        &self,
        key: &str,
        value: T,
    ) -> Result<(), DBError> {
        match serde_json::to_string(&value) {
            Ok(serialized) => self.put(&key, serialized.into_bytes()),
            Err(err) => Err(DBError::from(format!(
                "Failed to serialize to String. T: {:?}, err: {:?}",
                value, err
            ))),
        }
    }
}
