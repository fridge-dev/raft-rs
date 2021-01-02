use crate::commitlog::{InMemoryLog, Log, Entry};

pub fn run_it() {
    let mut log = InMemoryLog::new();
    let result = log.append(Entry::new(vec![1, 2, 3, 4]));
    match result {
        Ok(index) => println!("Ok: {:?}", index),
        Err(e) => println!("Err: {:?}", e)
    }
}