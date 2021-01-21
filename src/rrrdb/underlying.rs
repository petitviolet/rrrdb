use crate::rrrdb::DBResult;
use rocksdb::DB;

pub struct Underlying {
    db: rocksdb::DB,
}

impl Underlying {
    pub fn new(path: &str) -> Underlying {
        Underlying {
            db: DB::open_default(path).unwrap(),
        }
    }
}
