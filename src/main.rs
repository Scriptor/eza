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
    use std::io::{self, BufRead, BufWriter, Result, Write};

    pub struct DBState {
        pub map: super::HashMap<String, String>,
        pub file: super::File,
    }

    pub fn initialize_db(file: File) -> DBState {
        let mut map: HashMap<String, String> = HashMap::new();
        let buf = io::BufReader::new(&file);
        for line in buf.lines() {
            let entry = line.unwrap();
            println!("line: {}", &entry);
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

    fn persist_entry(db: &DBState, key: &String, value: &String) -> Result<()> {
        let mut w = BufWriter::new(&db.file);
        writeln!(w, "{}:{}", key, value).unwrap();
        w.flush()
    }

    pub fn set(db: &mut DBState, key: String, value: String) -> String {
        let result_str = format!("Set key: {} to value: {}", key, value);
        persist_entry(db, &key, &value).unwrap();
        db.map.insert(key, value);
        result_str
    }

    pub fn get(db: &DBState, key: String) -> String {
        match db.map.get(&key) {
            Some(s) => format!("Value is: {}", s),
            _ => String::from("Not found!"),
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
    db::get(&db, key)
}

#[get("/set/<key>/<value>")]
fn set(state: State<RwLock<db::DBState>>, key: String, value: String) -> String {
    let mut db = state.write().unwrap();
    db::set(&mut db, key, value)
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
