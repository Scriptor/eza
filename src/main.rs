#![feature(proc_macro_hygiene, decl_macro)]
#[macro_use]
extern crate rocket;

use rocket::State;
use std::fs::{File, OpenOptions};
use std::sync::RwLock;

mod db {
    use rocksdb::{Direction, IteratorMode, DB};
    use std::collections::{HashMap, HashSet};
    use std::fs::File;
    use std::io::{self, BufRead, BufWriter, Write};
    use std::str;
    use std::time::{SystemTime, UNIX_EPOCH};
    // Uuid may be reintroduced later with better tx id's
    //use uuid::Uuid;

    pub struct DBState {
        pub map: HashMap<String, String>,
        pub txs: HashMap<String, bool>, // TODO: Commit state should be enum
        pub db: DB,
        pub wal: super::File,
    }

    pub struct WalTx {
        id: String,
    }

    pub fn wal_new_tx(db: &mut DBState) -> WalTx {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards.")
            .as_nanos()
            .to_string();
        let tx = WalTx { id };
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:false", tx.id).unwrap();
        db.txs.insert(tx.id.clone(), false);
        w.flush().unwrap();
        tx
    }

    pub fn wal_append_set(db: &DBState, tx: &WalTx, key: &str, value: &str) -> io::Result<()> {
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:{}:{}", tx.id, &key, &value).unwrap();
        w.flush()
    }

    pub fn wal_commit(db: &mut DBState, tx: &WalTx) -> io::Result<()> {
        let mut w = BufWriter::new(&db.wal);
        writeln!(w, "{}:true", tx.id).unwrap();
        db.txs.insert(tx.id.clone(), true);
        w.flush()
    }

    pub fn initialize_db(db_path: String, wal_file: File) -> DBState {
        let mut txs = HashMap::new();
        let wal_buf = io::BufReader::new(&wal_file);
        for line in wal_buf.lines() {
            let entry = line.unwrap();
            let parts: Vec<&str> = entry.split(':').collect();
            if parts.len() == 2 {
                let tx_id = parts[0].to_owned();
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
            let data = String::from(str::from_utf8(&*key).unwrap());
            let tx_id = data_tx_id(&data);
            let value = bytes_to_string(&value);
            match txs.get(&tx_id) {
                Some(true) => map.insert(String::from(str::from_utf8(&*key).unwrap()), value),
                _ => None,
            };
        }

        DBState {
            map,
            txs,
            wal: wal_file,
            db,
        }
    }

    fn persist_entry(
        db: &DBState,
        key: &str,
        value: &str,
        tx: &WalTx,
    ) -> Result<(), rocksdb::Error> {
        let key = format!("{}:{}", key, tx.id);
        db.db.put(key.as_bytes(), value.as_bytes())
    }

    fn bytes_to_string(v: &[u8]) -> String {
        String::from(str::from_utf8(v).unwrap())
    }

    fn data_tx_id(val: &str) -> String {
        let parts: Vec<&str> = val.split(':').collect();
        parts[1].to_owned()
    }

    fn data_value(val: &str) -> String {
        let parts: Vec<&str> = val.split(':').collect();
        parts[0].to_owned()
    }

    pub fn set(mut db: &mut DBState, key: String, value: String) -> Result<String, String> {
        // Create an uncommitted WAL record and add a entry for each IO change
        // Commit after last entry added
        let tx = wal_new_tx(&mut db);
        wal_append_set(&db, &tx, &key, &value).unwrap();
        let result_str = format!("Set key: {} to value: {}", key, value);
        match persist_entry(db, &key, &value, &tx) {
            Ok(_) => {
                wal_commit(&mut db, &tx).unwrap();
                db.map.insert(key, value);
                Ok(result_str)
            }
            _ => Err(String::from("Failed to write")),
        }
    }

    pub fn multi_set(
        mut db: &mut DBState,
        keyvals: HashMap<String, String>,
    ) -> Result<String, String> {
        let tx = wal_new_tx(db);
        let mut result_str = "".to_string();
        for (key, value) in keyvals.iter() {
            wal_append_set(&db, &tx, &key, &value).unwrap();
            persist_entry(db, &key, &value, &tx).unwrap();
            db.map.insert(key.to_string(), value.to_string());
            let partial_result = format!("Set key: {} to value: {};", key, value);
            result_str.push_str(&partial_result);
        }
        wal_commit(&mut db, &tx).unwrap();
        Ok(result_str)
    }

    pub fn get(mut db: &mut DBState, key: String) -> Result<String, String> {
        let tx = wal_new_tx(db);

        let null_term_key = format!("{}:9", key);
        let db_iter = db.db.iterator(IteratorMode::From(
            null_term_key.as_bytes(),
            Direction::Reverse,
        ));
        for (k, value) in db_iter {
            let k = bytes_to_string(&k);

            let write_tx_id = data_tx_id(&k);
            let k = data_value(&k);
            // TODO: Handle the case where the write tx is PENDING,
            //       may need a mutex
            let is_committed = match db.txs.get(&write_tx_id) {
                Some(b) => *b,
                _ => false,
            };

            if key == k && (write_tx_id < tx.id && is_committed) {
                let value = bytes_to_string(&value);
                return Ok(value);
            }
        }

        wal_commit(&mut db, &tx).unwrap();
        Err(String::from("Not found!"))
    }

    pub fn scan(mut db: &mut DBState, start: String, end: String) -> Vec<String> {
        // Start with the first key found
        // Keep going until the end key found
        let tx = wal_new_tx(db);
        let mut values: Vec<String> = Vec::new();
        let null_term_end = format!("{}:9", end);
        let db_iter = db.db.iterator(IteratorMode::From(
            null_term_end.as_bytes(),
            Direction::Reverse,
        ));
        let mut found_keys = HashSet::new();
        for (k, value) in db_iter {
            let k = bytes_to_string(&k);
            let write_tx_id = data_tx_id(&k);
            let k = data_value(&k);

            if k < start {
                break;
            }

            let is_committed = *db.txs.get(&write_tx_id).unwrap_or(&false);
            if write_tx_id < tx.id && is_committed && !found_keys.contains(&k) {
                let value = bytes_to_string(&value);
                found_keys.insert(k.clone());
                values.push(value)
            }
        }
        wal_commit(&mut db, &tx).unwrap();
        values.reverse();
        values
    }

    pub fn mem_get(db: &DBState, key: String) -> Result<String, String> {
        match db.map.get(&key) {
            Some(s) => Ok(s.to_string()),
            _ => Err(String::from("Not found!")),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rocksdb::Options;
        use std::fs::{self, OpenOptions};

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
            {
                // Create temporary handle to the WAL file so that we can
                // delete it after finishing the setup. This emulates a
                // "crash" since it appears that nothing was written to the WAL.
                let db_path = "crashed_test_data".to_string();
                let setup_wal_file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .append(true)
                    .open("crashed_test_wal.db")
                    .unwrap();
                let mut setup_state = initialize_db(db_path, setup_wal_file);
                // keyvals that are part of the failed (i.e. crashed) write
                let mut crashed_keyvals = HashMap::new();
                crashed_keyvals.insert("hello".to_string(), "world".to_string());
                crashed_keyvals.insert("foo".to_string(), "bar".to_string());
                multi_set(&mut setup_state, crashed_keyvals).expect("Cannot set multiple keys.");
                fs::remove_file("crashed_test_wal.db").expect("Can't delete crashed wal file.");
            }

            let db_path = "crashed_test_data".to_string();
            let wal_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open("crashed_test_wal.db")
                .unwrap();
            let mut state = initialize_db(db_path, wal_file);
            // The following multiset should work fine so these keyvals are
            // considered 'good'
            let mut good_keyvals = HashMap::new();
            good_keyvals.insert("good_hello".to_string(), "good_world".to_string());
            good_keyvals.insert("good_foo".to_string(), "good_bar".to_string());
            multi_set(&mut state, good_keyvals).expect("Cannot set multiple keys.");
            state
        }

        fn cleanup() {
            super::DB::destroy(&Options::default(), "test_data".to_string())
                .expect("Cannot destroy main test db.");
            fs::remove_file("test_wal.db").expect("Can't clean up main test wal file.");
        }
        fn cleanup_crashed() {
            super::DB::destroy(&Options::default(), "crashed_test_data".to_string())
                .expect("Cannot destroy crash test db.");
            fs::remove_file("crashed_test_wal.db").expect("Can't clean up crashed wal file.");
        }

        #[test]
        fn test_set() {
            {
                let mut db = setup();
                let res = set(&mut db, String::from("hello"), String::from("world")).unwrap();
                assert_eq!(res, "Set key: hello to value: world");
            }
            cleanup();
        }

        #[test]
        fn test_multi_set() {
            {
                let mut db = setup();
                let mut keyvals = HashMap::new();
                keyvals.insert("hello".to_string(), "world".to_string());
                keyvals.insert("foo".to_string(), "bar".to_string());
                multi_set(&mut db, keyvals).unwrap();
                assert_eq!(
                    get(&mut db, "hello".to_string()).unwrap(),
                    "world".to_string()
                );
                assert_eq!(get(&mut db, "foo".to_string()).unwrap(), "bar".to_string());
            }
            cleanup();
        }

        #[test]
        fn test_get() {
            {
                let mut db = setup();
                set(&mut db, String::from("hello"), String::from("world")).unwrap();
                let res = get(&mut db, String::from("hello")).unwrap();
                assert_eq!(res, String::from("world"));
            }
            cleanup();
        }

        #[test]
        fn test_scan() {
            {
                let mut db = setup();
                let mut keyvals = HashMap::new();
                set(&mut db, "3".to_string(), "should-be-ignored".to_string()).unwrap();
                keyvals.insert("1".to_string(), "first".to_string());
                keyvals.insert("2".to_string(), "second".to_string());
                keyvals.insert("3".to_string(), "third".to_string());
                keyvals.insert("4".to_string(), "fourth".to_string());
                keyvals.insert("5".to_string(), "fifth".to_string());
                multi_set(&mut db, keyvals).unwrap();
                let res = scan(&mut db, "2".to_string(), "3".to_string());
                assert_eq!(res, vec!["second".to_string(), "third".to_string()]);
            }
            cleanup();
        }

        #[test]
        fn test_crashed() {
            {
                let mut db = setup_crashed();
                assert_eq!(
                    get(&mut db, String::from("hello")),
                    Err("Not found!".to_string())
                );
                let res = get(&mut db, String::from("good_hello")).unwrap();
                assert_eq!(res, String::from("good_world"));
            }
            cleanup_crashed();
        }
    }
}

#[get("/")]
fn index() -> &'static str {
    "Eza DB!"
}

#[get("/get/<key>")]
fn get(state: State<RwLock<db::DBState>>, key: String) -> String {
    let mut db = state.write().unwrap();
    db::get(&mut db, key).unwrap()
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
