use crate::global::CONFIG_LIST;
use anyhow::{Error, Ok};
use std::fs::OpenOptions;
use std::mem::uninitialized;
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use super::ExpiringHashMap;

// Magic string + RDB version number (ASCII): "REDIS0011".
const MAGIC_STRING: [u8; 9] = *b"REDIS0011";

async fn init_db() -> anyhow::Result<File, Error> {
    let rdb_dir = CONFIG_LIST
        .get_val(&"dir".into())
        .expect("directory name is empty");

    let rdb_file = CONFIG_LIST
        .get_val(&"dbfilename".into())
        .expect("filename is empty");

    // rdb_file_p.push(&p);
    let p = Path::new(&rdb_dir);
    if !Path::new(&p).exists() {
        fs::create_dir(p)?; // .expect("Directory creation failed");
    }

    let rdb_file_p = PathBuf::from(&rdb_dir).join(rdb_file);
    if Path::new(&rdb_file_p).exists() {
        return Ok(OpenOptions::new().append(true).open(&rdb_file_p)?);
    }
    let mut rdb_file = File::create(rdb_file_p)?; // .expect("RDB File creation failed");

    // MAGIC_STRING
    rdb_file.write_all(&MAGIC_STRING)?;

    // Meta Data
    let op_code: u8 = 0xFA;
    let metadata_inputs: Vec<String> = vec!["redis-ver".to_string(), "6.0.16".to_string()];
    let mut metadata = op_code.to_le_bytes().to_vec();
    for data in metadata_inputs {
        let mut d = (data.len() as u8).to_le_bytes().to_vec();
        d.extend(data.as_bytes().to_vec());
        metadata.extend(d);
        // len_encoded_str.extend(data.as_bytes().to_vec());
    }

    rdb_file.write_all(&metadata)?;
    Ok(rdb_file)
}

pub async fn write_to_disk(mut db: ExpiringHashMap<String, String>) -> anyhow::Result<()> {
    let mut rdb_file = init_db().await?;

    // Database Subsection
    let op_code: u8 = 0xFE;
    let db_index: u8 = 0;
    rdb_file
        .write_all(&[op_code])
        .expect("Writing db subsection failed - op_code");
    rdb_file
        .write_all(&[db_index])
        .expect("Writing db subsection failed - db_index");

    let op_code: u8 = 0xFB;
    let ht_size = db.get_ht_size().await;
    dbg!(&ht_size);
    let ht_expire_size = db.get_ht_expire_size().await;
    dbg!(&ht_expire_size);
    rdb_file
        .write_all(&[op_code])
        .expect("Writing db subsection failed - ht_size - op_code");
    rdb_file
        .write_all(&length_encoded_int(ht_size))
        .expect("Writing db subsection failed - ht_size");
    rdb_file
        .write_all(&length_encoded_int(ht_expire_size))
        .expect("Writing db subsection failed - ht_expire_size");
    for (k, item) in db.iter().await {
        let mut d = Vec::from([0u8]); // 1-byte flag - string encoding
        d.extend((k.len() as u8).to_le_bytes());
        d.extend(k.as_bytes());
        d.extend((item.0.len() as u8).to_le_bytes());
        d.extend(item.0.as_bytes());
        rdb_file
            .write_all(&d)
            .expect("Writing db subsection failed - data");
    }
    Ok(())
}

fn length_encoded_int(n: usize) -> Vec<u8> {
    println!("n = {}", n);
    let mut encoded_int: Vec<u8> = Vec::new();

    if n <= 63usize {
        encoded_int.extend((n as u8).to_le_bytes());
    } else if n >= 64usize && n <= 16383usize {
        encoded_int.extend((n as u16).to_le_bytes());
    } else if n >= 16384usize && n <= 4_294_967_295usize {
        encoded_int.extend((n as u32).to_le_bytes());
    }
    encoded_int
}
