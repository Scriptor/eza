#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use rocket::State;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::sync::RwLock;

mod db {
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{self, BufRead, BufWriter, Write};

    pub struct DBState {
        pub map: super::HashMap<String, String>,
        pub file: super::File,
    }

    pub fn initialize_db(file: File) -> DBState {
        let mut map: HashMap<String, String> = HashMap::new();
        let buf = io::BufReader::new(&file);
        for line in buf.lines() {
            let entry = line.unwrap();
            let parts: Vec<&str> = entry.split(":").collect();
            if parts.len() == 2 {
                map.insert(String::from(parts[0]), String::from(parts[1]));
            }
        }
        DBState {
            map: map,
            file: file,
        }
    }

    fn persist_entry(db: &DBState, key: &String, value: &String) -> io::Result<()> {
        let mut w = BufWriter::new(&db.file);
        writeln!(w, "{}:{}", key, value).unwrap();
        w.flush()
    }

    pub fn set(db: &mut DBState, key: String, value: String) -> Result<String, String> {
        let result_str = format!("Set key: {} to value: {}", key, value);
        match persist_entry(db, &key, &value) {
            Ok(_) => {
                db.map.insert(key, value);
                Ok(result_str)
            }
            _ => Err(String::from("Failed to write")),
        }
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
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open("test_data.db")
                .unwrap();
            initialize_db(file)
        }

        #[test]
        fn test_set() {
            let mut db = setup();
            let res = set(&mut db, String::from("hello"), String::from("world")).unwrap();
            assert_eq!(res, "Set key: hello to value: world");
        }

        #[test]
        fn test_get() {
            let mut db = setup();
            set(&mut db, String::from("hello"), String::from("world")).unwrap();
            let db = db;
            let res = get(&db, String::from("hello")).unwrap();
            assert_eq!(res, String::from("world"));
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
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open("data.db")
        .unwrap();
    let db = db::initialize_db(file);

    rocket::ignite()
        .manage(RwLock::new(db))
        .mount("/", routes![index, set, get])
        .launch();
}
