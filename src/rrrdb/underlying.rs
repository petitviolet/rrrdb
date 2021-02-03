use crate::rrrdb::DBResult;
use rocksdb::DB;

pub struct Underlying {
    db: rocksdb::DB,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBError {
    message: String,
}

impl From<rocksdb::Error> for DBError {
    fn from(e: rocksdb::Error) -> Self {
        Self {
            message: e.into_string(),
        }
    }
}

impl Underlying {
    pub fn new(path: &str) -> Underlying {
        Underlying {
            db: DB::open_default(path).unwrap(),
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, DBError> {
        self.db.get(key).map_err(|e| DBError::from(e))
    }

    pub fn put(&self, key: &str, value: Vec<u8>) -> Result<(), DBError> {
        self.db.put(key, value).map_err(|e| DBError::from(e))
    }
}
