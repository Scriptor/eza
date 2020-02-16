#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use] extern crate rocket;

use std::collections::HashMap;
use rocket::State;
use std::sync::RwLock;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Result, Write};

#[get("/")]
fn index() -> &'static str {
    "Eza DB!"
}

struct DBState {
    map: HashMap<String,String>,
    file: File,
}

fn persist_entry(db: &DBState, key: &String, value: &String) -> Result<()> {
    let mut w = BufWriter::new(&db.file);
    writeln!(w, "{}:{}", key, value).unwrap();
    w.flush()
}

fn lookup_entry(db: &DBState, key: &String) {
    let file = &db.file;
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        if let Ok(entry) = line {

        }
    }
}

#[get("/get/<key>")]
fn get(state: State<RwLock<DBState>>, key: String) -> String {
    let state = state.read().unwrap();
    match state.map.get(&key) {
        Some(s) => format!("Value is: {}", s),
        _ => String::from("Not found!")
    }
}

#[get("/set/<key>/<value>")]
fn set(state: State<RwLock<DBState>>, key: String, value: String) -> String {
    let mut db = state.write().unwrap();
    let result_str = format!("Set key: {} to value: {}", key, value);
    persist_entry(&db, &key, &value).unwrap();
    db.map.insert(key, value);
    result_str
}

fn main() {
    let memory_db: HashMap<String, String> = HashMap::new();
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open("data.db")
        .unwrap();

    rocket::ignite()
        .manage(RwLock::new(DBState{map: memory_db, file: file}))
        .mount("/", routes![index, set, get]).launch();
}
