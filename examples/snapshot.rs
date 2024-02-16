use std::{
    error::Error,
    io::{stdin, BufRead},
};

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize, Debug)]
pub struct Event {
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub data: Value,
}

fn main() -> Result<(), Box<dyn Error>> {
    let stdin = stdin();
    for line in stdin.lock().lines() {
        let line = &line?;
        let v: Event = serde_json::from_str(line)?;
        println!("{v:?}");
    }

    Ok(())
}
