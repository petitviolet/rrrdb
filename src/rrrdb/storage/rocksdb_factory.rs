use rocksdb::{Options, DB};

pub(crate) struct RocksDBFactory {
    base_path: String,
}

impl RocksDBFactory {
    pub fn new(base_path: &str) -> Self {
        Self {
            base_path: base_path.to_string(),
        }
    }

    pub fn open_default(&self) -> rocksdb::DB {
        DB::open_default(&self.base_path).unwrap()
    }

    pub fn open_column_family<'a>(&'a self, db: &'a mut rocksdb::DB, cf_name: &str) -> () {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        db.create_cf(cf_name, &options);
    }
}
