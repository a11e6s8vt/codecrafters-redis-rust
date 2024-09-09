use super::ExpiringHashMap;
use crate::global::CONFIG_LIST;
use anyhow::Error;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use core::str;
use std::any::Any;
use std::borrow::Borrow;
use std::fs::OpenOptions;
use std::io::{self, BufReader, Cursor, Read};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

const CHUNK_SIZE: usize = 1 * 16;

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

pub async fn read_rdb() -> anyhow::Result<Vec<String>, Error> {
    let mut result: Vec<String> = Vec::new();
    let rdb_dir = CONFIG_LIST
        .get_val(&"dir".into())
        .expect("directory name is empty");

    let rdb_file = CONFIG_LIST
        .get_val(&"dbfilename".into())
        .expect("filename is empty");

    let rdb_file_p = PathBuf::from(&rdb_dir).join(rdb_file);
    //let mut f = OpenOptions::new().read(true).open(&rdb_file_p)?;
    let file = File::open(rdb_file_p)?;
    // let file_mmap = unsafe { Mmap::map(&f)? };
    // let bytes: &[u8] = &file_mmap[..];
    // let mut f = File::open("data.bin").unwrap();
    let mut reader = BufReader::new(file);

    let mut header = [0; 9]; // The first 9 bytes contain the header.
    reader.read_exact(&mut header)?;

    println!("MAGIC STRING: {:?}", std::str::from_utf8(&header));
    // Validate the RDB version (example for RDB version 9).
    let version = &header[5..9];
    // if version != b"0009" {
    //     return Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported RDB version").into());
    // }

    println!("RDB Version: {:?}", std::str::from_utf8(version));

    while let Ok(byte) = reader.read_u8() {
        if faster_hex::hex_string(&[byte]) == "fb" {
            break;
        }
    }

    // -> 00	The next 6 bits represent the length
    // -> 01	Read one additional byte. The combined 14 bits represent the length
    // -> 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
    // -> 11	The next object is encoded in a special format. The remaining 6 bits indicate the format.
    //          May be used to store numbers or Strings, see String Encoding:
    //              If the value of those 6 bits is:
    //                  0 indicates that an 8 bit integer follows
    //                  1 indicates that a 16 bit integer follows
    //                  2 indicates that a 32 bit integer follows
    let byte = reader.read_u8().unwrap();
    let mut ht_size: Vec<Box<dyn Any>> = Vec::new();
    match ((byte & 1 << 7) > 0, (byte & 1 << 6) > 0) {
        (false, false) => ht_size.push(Box::new(byte)),
        (false, true) => {
            let number = ((byte as u16) << 8) | reader.read_u8().unwrap() as u16;
            ht_size.push(Box::new(number));
        }
        (true, false) => ht_size.push(Box::new(reader.read_u32::<BigEndian>().unwrap())),
        (true, true) => {
            match (
                (byte & 1 << 5) > 0,
                (byte & 1 << 4) > 0,
                (byte & 1 << 3) > 0,
                (byte & 1 << 2) > 0,
                (byte & 1 << 1) > 0,
                (byte & 1 << 0) > 0,
            ) {
                (false, false, false, false, false, false) => {
                    ht_size.push(Box::new(reader.read_u8().unwrap()))
                }
                (false, false, false, false, false, true) => {
                    ht_size.push(Box::new(reader.read_u16::<BigEndian>().unwrap()))
                }
                (false, false, false, false, true, false) => {
                    ht_size.push(Box::new(reader.read_u32::<BigEndian>().unwrap()))
                }
                (_, _, _, _, _, _) => {}
            }
        }
    };

    let ht_size = ht_size.pop().unwrap();
    let ht_size = if let Some(value) = ht_size.downcast_ref::<u8>() {
        value.clone() as usize
    } else if let Some(value) = ht_size.downcast_ref::<u16>() {
        value.clone() as usize
    } else if let Some(value) = ht_size.downcast_ref::<u32>() {
        value.clone() as usize
    } else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Hash Table Size").into());
    };

    // ht_expire_size
    let byte = reader.read_u8().unwrap();
    let mut ht_expire_size: Vec<Box<dyn Any>> = Vec::new();
    match ((byte & 1 << 7) > 0, (byte & 1 << 6) > 0) {
        (false, false) => ht_expire_size.push(Box::new(byte)),
        (false, true) => {
            let number = ((byte as u16) << 8) | reader.read_u8().unwrap() as u16;
            ht_expire_size.push(Box::new(number));
        }
        (true, false) => ht_expire_size.push(Box::new(reader.read_u32::<BigEndian>().unwrap())),
        (true, true) => {
            match (
                (byte & 1 << 5) > 0,
                (byte & 1 << 4) > 0,
                (byte & 1 << 3) > 0,
                (byte & 1 << 2) > 0,
                (byte & 1 << 1) > 0,
                (byte & 1 << 0) > 0,
            ) {
                (false, false, false, false, false, false) => {
                    ht_expire_size.push(Box::new(reader.read_u8().unwrap()))
                }
                (false, false, false, false, false, true) => {
                    ht_expire_size.push(Box::new(reader.read_u16::<BigEndian>().unwrap()))
                }
                (false, false, false, false, true, false) => {
                    ht_expire_size.push(Box::new(reader.read_u32::<BigEndian>().unwrap()))
                }
                (_, _, _, _, _, _) => {}
            }
        }
    };

    let ht_expire_size = ht_expire_size.pop().unwrap();
    let ht_expire_size = if let Some(value) = ht_expire_size.downcast_ref::<u8>() {
        value.clone() as usize
    } else if let Some(value) = ht_expire_size.downcast_ref::<u16>() {
        value.clone() as usize
    } else if let Some(value) = ht_expire_size.downcast_ref::<u32>() {
        value.clone() as usize
    } else {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid Hash Table Size").into());
    };

    println!("{} {}", ht_size, ht_expire_size);

    // Value Type
    // ==========
    // A one byte flag indicates encoding used to save the Value.
    // 0 = String Encoding
    // 1 = List Encoding
    // 2 = Set Encoding
    // 3 = Sorted Set Encoding
    // 4 = Hash Encoding
    // 9 = Zipmap Encoding
    // 10 = Ziplist Encoding
    // 11 = Intset Encoding
    // 12 = Sorted Set in Ziplist Encoding
    // 13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
    // 14 = List in Quicklist encoding (Introduced in RDB version 7)
    // Key
    // ===
    // The key is simply encoded as a Redis string. See the section String Encoding to learn how the key is encoded.
    //
    // Value
    // =====
    // The value is parsed according to the previously read Value Type
    let byte = reader.read_u8().unwrap();
    if byte == 0u8 {
        let key_len = reader.read_u8().unwrap() as usize;
        let mut buffer: Vec<u8> = vec![0; key_len]; // Vec::with_capacity(key_len);
        reader.read_exact(&mut buffer).unwrap();
        let key = String::from_utf8(buffer).unwrap();

        result.push(key);
        let value_len = reader.read_u8().unwrap() as usize;
        let mut buffer: Vec<u8> = vec![0; value_len]; // Vec::with_capacity(value_len);
        reader.read_exact(&mut buffer[..]).unwrap();
        let value = str::from_utf8(&buffer).unwrap();
    }

    Ok(result)
}
