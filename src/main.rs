#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use]
extern crate rocket;

use rocket::State;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::sync::RwLock;

mod db {
    use rocksdb::{IteratorMode, DB};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, BufRead, BufWriter, Write};
    use std::str;
    use uuid::Uuid;

    pub struct DBState {
        pub map: super::HashMap<String, String>,
        pub db: DB,
        pub wal: super::File,
    }

    pub struct WalTx {
        id: Uuid,
    }

    pub fn wal_new_tx<'a>(db: &'a DBState) -> WalTx {
        let id = Uuid::new_v4();
        let tx = WalTx { id: id };
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:{}", id, false).unwrap();
        w.flush().unwrap();
        tx
    }

    pub fn wal_append_set<'a>(
        db: &DBState,
        tx: &WalTx,
        key: &'a String,
        value: &'a String,
    ) -> io::Result<()> {
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:{}:{}", tx.id, &key, &value).unwrap();
        w.flush()
    }

    pub fn wal_commit<'a>(db: &DBState, tx: &WalTx) -> io::Result<()> {
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:{}", tx.id, true).unwrap();
        w.flush()
    }

    pub fn initialize_db(db_path: String, wal_file: File) -> DBState {
        let mut txs = HashMap::new();
        let wal_buf = io::BufReader::new(&wal_file);
        for line in wal_buf.lines() {
            let entry = line.unwrap();
            let parts: Vec<&str> = entry.split(":").collect();
            if parts.len() == 2 {
                let tx_id = Uuid::parse_str(parts[0]).unwrap();
                if parts[1] == "true" {
                    txs.insert(tx_id, true);
                } else {
                    txs.insert(tx_id, false);
                }
            }
        }

        let db = DB::open_default(db_path).unwrap();
        let db_iter = db.iterator(IteratorMode::Start);
        let mut map: HashMap<String, String> = HashMap::new();
        for (key, value) in db_iter {
            let data = String::from(str::from_utf8(&*value).unwrap());
            let parts: Vec<&str> = data.split(":").collect();
            let tx_id = Uuid::parse_str(parts[0]).unwrap();
            let value = String::from(parts[1]);
            if *txs.get(&tx_id).unwrap() {
                map.insert(String::from(str::from_utf8(&*key).unwrap()), value);
            }
        }
        DBState {
            map: map,
            wal: wal_file,
            db: db,
        }
    }

    fn persist_entry(
        db: &DBState,
        key: &String,
        value: &String,
        tx: &WalTx,
    ) -> Result<(), rocksdb::Error> {
        let data = format!("{}:{}", tx.id, value);
        db.db.put(key.as_bytes(), data.as_bytes())
    }

    pub fn set(db: &mut DBState, key: String, value: String) -> Result<String, String> {
        // Create an uncommitted WAL record and add a entry for each IO change
        // Commit after last entry added
        let tx = wal_new_tx(&db);
        wal_append_set(&db, &tx, &key, &value).unwrap();
        let result_str = format!("Set key: {} to value: {}", key, value);
        let result = match persist_entry(db, &key, &value, &tx) {
            Ok(_) => {
                wal_commit(&db, &tx).unwrap();
                db.map.insert(key, value);
                Ok(result_str)
            }
            _ => Err(String::from("Failed to write")),
        };
        result
    }

    pub fn multi_set(db: &mut DBState, keyvals: HashMap<String, String>) -> Result<String, String> {
        let tx = wal_new_tx(&db);
        let mut result_str = "".to_string();
        for (key, value) in keyvals.iter() {
            wal_append_set(&db, &tx, &key, &value).unwrap();
            persist_entry(db, &key, &value, &tx).unwrap();
            db.map.insert(key.to_string(), value.to_string());
            let partial_result = format!("Set key: {} to value: {};", key, value);
            result_str.push_str(&partial_result);
        }
        wal_commit(&db, &tx).unwrap();
        Ok(result_str)
    }

    pub fn get(db: &DBState, key: String) -> Result<String, String> {
        match db.map.get(&key) {
            Some(s) => Ok(s.to_string()),
            _ => Err(String::from("Not found!")),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs::OpenOptions;

        fn setup() -> DBState {
            let db_path = "test_data".to_string();
            let wal_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open("test_wal.db")
                .unwrap();
            initialize_db(db_path, wal_file)
        }

        fn setup_crashed() -> DBState {
            let db_path = "crashed_test_data".to_string();
            let wal_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open("crashed_test_wal.db")
                .unwrap();
            initialize_db(db_path, wal_file)
        }

        #[test]
        fn test_set() {
            let mut db = setup();
            let res = set(&mut db, String::from("hello"), String::from("world")).unwrap();
            assert_eq!(res, "Set key: hello to value: world");
        }

        #[test]
        fn test_multi_set() {
            let mut db = setup();
            let mut keyvals = HashMap::new();
            keyvals.insert("hello".to_string(), "world".to_string());
            keyvals.insert("foo".to_string(), "bar".to_string());
            multi_set(&mut db, keyvals).unwrap();
            assert_eq!(get(&db, "hello".to_string()).unwrap(), "world".to_string());
            assert_eq!(get(&db, "foo".to_string()).unwrap(), "bar".to_string());
        }

        #[test]
        fn test_get() {
            let mut db = setup();
            set(&mut db, String::from("hello"), String::from("world")).unwrap();
            let db = db;
            let res = get(&db, String::from("hello")).unwrap();
            assert_eq!(res, String::from("world"));
        }

        #[test]
        fn test_crashed() {
            let db = setup_crashed();
            assert_eq!(
                get(&db, String::from("hello")),
                Err("Not found!".to_string())
            );
        }
    }
}

#[get("/")]
fn index() -> &'static str {
    "Eza DB!"
}

#[get("/get/<key>")]
fn get(state: State<RwLock<db::DBState>>, key: String) -> String {
    let db = state.read().unwrap();
    db::get(&db, key).unwrap()
}

#[get("/set/<key>/<value>")]
fn set(state: State<RwLock<db::DBState>>, key: String, value: String) -> String {
    let mut db = state.write().unwrap();
    db::set(&mut db, key, value).unwrap()
}

fn main() {
    let db_path = "data".to_string();
    let wal_file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open("wal.db")
        .unwrap();
    let db = db::initialize_db(db_path, wal_file);

    rocket::ignite()
        .manage(RwLock::new(db))
        .mount("/", routes![index, set, get])
        .launch();
}
