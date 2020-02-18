#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use rocket::State;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufWriter, Result, Write};
use std::sync::RwLock;

#[get("/")]
fn index() -> &'static str {
    "Eza DB!"
}

struct DBState {
    map: HashMap<String, String>,
    file: File,
}

fn persist_entry(db: &DBState, key: &String, value: &String) -> Result<()> {
    let mut w = BufWriter::new(&db.file);
    writeln!(w, "{}:{}", key, value).unwrap();
    w.flush()
}

#[get("/get/<key>")]
fn get(state: State<RwLock<DBState>>, key: String) -> String {
    let state = state.read().unwrap();
    match state.map.get(&key) {
        Some(s) => format!("Value is: {}", s),
        _ => String::from("Not found!"),
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

fn load_db(file: &File) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let buf = io::BufReader::new(file);
    for line in buf.lines() {
        let entry = line.unwrap();
        println!("line: {}", &entry);
        let parts: Vec<&str> = entry.split(":").collect();
        if parts.len() == 2 {
            map.insert(String::from(parts[0]), String::from(parts[1]));
        }
    }
    map
}

fn main() {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open("data.db")
        .unwrap();
    let memory_db = load_db(&file);

    rocket::ignite()
        .manage(RwLock::new(DBState {
            map: memory_db,
            file: file,
        }))
        .mount("/", routes![index, set, get])
        .launch();
}
