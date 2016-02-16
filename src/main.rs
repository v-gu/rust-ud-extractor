use std::env;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;
use std::collections::HashMap;
use std::collections::HashSet;

extern crate regex;
use regex::Regex;

macro_rules! stderr {
    ($($arg:tt)*) => (
        // use std::io::Write;
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr (file handle closed?): {}", x),
        }
    )
}

fn main() {
    let mut argv = env::args();
    if argv.len() != 4 {
        panic!("rust-ud-extract <input-file> <MERCHANT_ID> <PRODUCT_ID>");
    }
    // skip arg0
    argv.next();
    let in_filename = argv.next().unwrap();
    let merchant_id = argv.next().unwrap();
    let product_id = argv.next().unwrap();

    let record_map = match read(&in_filename, &merchant_id, &product_id) {
        Ok(value) => value,
        Err(err) => panic!("reading from input error: {}", err),
    };
    let re_status = Regex::new(r#"status[^\d]*"(\d+)""#).unwrap();
    let re_ret_code = Regex::new(r#"retCode[^\d]*(\d+)"#).unwrap();

    // writeout
    println!("ID, STATUS, RETCODE");
    for (id, value) in &record_map {
        let mut status = "nil";
        let mut ret_code = "nil";
        match re_status.captures(value) {
            Some(caps) => {
                status = match caps.at(1) {
                    Some(v) => v,
                    None => "nil",
                };
            },
            None => stderr!("can't find status, id: {}, line: {}", id, value),
        };
        match re_ret_code.captures(value) {
            Some(caps) => {
                ret_code = match caps.at(1) {
                    Some(v) => v,
                    None => "nil",
                };
            },
            None => stderr!("can't find re_ret_code, id: {}, line: {}", id, value),
        };
        println!("{}, {}, {}", id, status, ret_code);
    }
    io::stdout().flush();
    io::stderr().flush();
}

fn read(in_filename: &str, merchant_id: &str, product_id: &str)
        -> Result<HashMap<String, String>, io::Error> {
    let mut f = try!(File::open(in_filename));
    let mut reader = BufReader::with_capacity(20*1024*1024, f);

    let mut id_set: HashSet<String> = HashSet::new();
    let mut pattern = merchant_id.to_string();
    pattern.push_str(".*");
    pattern.push_str(&product_id);
    let re = Regex::new(&pattern).unwrap();
    let re_trace_id = Regex::new(r"traceId: (\d+),").unwrap();

    stderr!("starting 1-pass..");
    for line in reader.lines() {
        let line_string = line.unwrap();
        let line_str = &line_string;
        if re.is_match(line_str) {
            // println!("{}", lineString);
            let caps = re_trace_id.captures(line_str).unwrap();
            // traceIds.push(String::from(caps.at(1).unwrap()));
            id_set.insert(String::from(caps.at(1).unwrap()));

            let length = &id_set.len();
            if length % 1000 == 0 {
                stderr!("1-pass: {} ids stored..", length);
            }
        }
    }
    stderr!("1-pass: {} ids stored", &id_set.len());
    stderr!("1-pass done");

    stderr!("starting 2-pass..");
    let mut record_map: HashMap<String, String> = HashMap::new();
    f = try!(File::open(in_filename));
    reader = BufReader::with_capacity(20*1024*1024, f);
    for line in reader.lines() {
        let line_string = line.unwrap();
        let caps = match re_trace_id.captures(&line_string) {
            Some(v) => v,
            None => continue,
        };
        let in_id = String::from(caps.at(1).unwrap());
        if id_set.contains(&in_id) {
            if record_map.contains_key(&in_id) {
                let mut orig = record_map.get_mut(&in_id).unwrap();
                orig.push_str(&line_string);
            } else {
                record_map.insert(in_id, line_string.clone());
            }
            let length = record_map.len();
            if length % 1000 == 0 {
                stderr!("2-pass: {} recording stored..", length);
            }
        }
    }
    stderr!("2-pass: {} recording stored", &record_map.len());
    stderr!("2-pass done");

    return Ok(record_map);
}

