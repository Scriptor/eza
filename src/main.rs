#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;

use rocket::State;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufRead, BufWriter, Result, SeekFrom, Write};
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

fn lookup_entry<'a>(db: &'a DBState, key: &String) -> Option<String> {
    let mut file = &db.file;
    file.seek(SeekFrom::Start(0)).unwrap();
    let buf = io::BufReader::new(file);
    let mut result = None;
    for line in buf.lines() {
        let entry = line.unwrap();
        println!("line: {}", &entry);
        let parts: Vec<&str> = entry.split(":").collect();
        let line_key = parts[0];
        if key == line_key {
            result = Some(String::from(parts[1]));
        }
    }
    result
}

#[get("/get/<key>")]
fn get(state: State<RwLock<DBState>>, key: String) -> String {
    let state = state.read().unwrap();
    let val = lookup_entry(&state, &key);
    match val {
        Some(s) => format!("Value is: {}", s),
        None => String::from("Not found!"),
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
        .append(true)
        .open("data.db")
        .unwrap();

    rocket::ignite()
        .manage(RwLock::new(DBState {
            map: memory_db,
            file: file,
        }))
        .mount("/", routes![index, set, get])
        .launch();
}
