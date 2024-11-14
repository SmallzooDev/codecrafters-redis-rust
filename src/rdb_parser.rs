use crate::protocol_constants::{MAGIC_NUMBER, OPCODE_EOF, OPCODE_META, OPCODE_START_DB};
use crate::{Db, ValueEntry};
use byteorder::{LittleEndian, ReadBytesExt};
use crc::{Crc, CRC_64_ECMA_182};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(" ")
}

fn read_length_or_integer<R: Read>(reader: &mut R, first_byte: u8) -> io::Result<usize> {
    match first_byte >> 6 {
        0b00 => Ok(read_6bit_length(first_byte)),
        0b01 => read_14bit_length(reader, first_byte),
        0b10 => read_32bit_length(reader),
        0b11 => read_encoded_integer(reader, first_byte & 0x3F),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid length encoding")),
    }
}

fn read_6bit_length(first_byte: u8) -> usize {
    (first_byte & 0x3F) as usize
}

fn read_14bit_length<R: Read>(reader: &mut R, first_byte: u8) -> io::Result<usize> {
    let second_byte = reader.read_u8()?;
    Ok((((first_byte & 0x3F) as usize) << 8) | (second_byte as usize))
}

fn read_32bit_length<R: Read>(reader: &mut R) -> io::Result<usize> {
    reader.read_u32::<LittleEndian>().map(|len| len as usize)
}

fn read_encoded_integer<R: Read>(reader: &mut R, encoding_type: u8) -> io::Result<usize> {
    match encoding_type {
        0 => reader.read_u8().map(|val| val as usize),
        1 => reader.read_u16::<LittleEndian>().map(|val| val as usize),
        2 => reader.read_u32::<LittleEndian>().map(|val| val as usize),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported encoding type")),
    }
}

pub async fn run_rdb_handler(db: Db, rdb_file_path: String) -> io::Result<()> {
    let file = File::open(&rdb_file_path)?;
    let mut reader = BufReader::new(file);

    let mut magic = [0; 5];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC_NUMBER {
        eprintln!("Invalid RDB file format.");
        return Ok(());
    }
    println!("Valid Redis RDB file detected.");

    let mut version = [0; 4];
    reader.read_exact(&mut version)?;
    println!("RDB Version: {}", String::from_utf8_lossy(&version));

    loop {
        let mut marker = [0; 1];
        if reader.read_exact(&mut marker).is_err() {
            println!("Reached the end of file.");
            break;
        }
        match marker[0] {
            OPCODE_META => {
                let first_key_byte = reader.read_u8()?;
                let key_length = read_length_or_integer(&mut reader, first_key_byte)?;

                let mut key_bytes = vec![0; key_length];
                reader.read_exact(&mut key_bytes)?;
                let key = String::from_utf8_lossy(&key_bytes);
                println!("Metadata key: {}", key);

                let first_value_byte = reader.read_u8()?;
                if first_value_byte >> 6 == 0b11 {
                    let value = read_length_or_integer(&mut reader, first_value_byte)?;
                    println!("Metadata value : {}", value);
                } else {
                    let value_length = read_length_or_integer(&mut reader, first_value_byte)?;
                    let mut value_bytes = vec![0; value_length];
                    reader.read_exact(&mut value_bytes)?;

                    match String::from_utf8(value_bytes.clone()) {
                        Ok(value) => println!("Metadata value: {}", value),
                        Err(_) => {
                            let hex_value = bytes_to_hex(&value_bytes);
                            println!("Invalid UTF-8 in metadata value, raw HEX bytes: {}", hex_value);
                        }
                    }
                }
                continue;
            }
            OPCODE_START_DB => {
                println!("Detected start of new database.");
                let db_index = reader.read_u8()?;
                println!("Database index: {}", db_index);
                continue;
            }
            0xFB => {
                println!("Resizedb field detected.");
                let total_size = reader.read_u8()?;
                let expires_size = reader.read_u8()?;
                println!("Hash table size: {}, Expires table size: {}", total_size, expires_size);
                continue;
            }
            0xFD | 0xFC => {
                let expiry_type = if marker[0] == 0xFD { "seconds" } else { "milliseconds" };

                let expiration_ms = if expiry_type == "seconds" {
                    Some((reader.read_u32::<LittleEndian>()? as u64) * 1000)
                } else {
                    Some(reader.read_u64::<LittleEndian>()?)
                };

                let value_type = reader.read_u8()?;
                let key_length = reader.read_u8()? as usize;
                let mut key = vec![0; key_length];
                reader.read_exact(&mut key)?;
                let key_str = String::from_utf8_lossy(&key).to_string();

                let value_length = reader.read_u8()? as usize;
                let mut value = vec![0; value_length];
                reader.read_exact(&mut value)?;
                let value_str = String::from_utf8_lossy(&value).to_string();

                let entry = ValueEntry::new_absolute(value_str.clone(), expiration_ms);
                db.write().await.insert(key_str.clone(), entry);
                println!("Inserted key: {} with value: {} and expiration: {:?}", key_str, value_str, expiration_ms);
            }
            0x00 => {
                let key_length = reader.read_u8()? as usize;

                let mut key = vec![0; key_length];
                reader.read_exact(&mut key)?;
                let key_str = String::from_utf8_lossy(&key).to_string();

                let value_length = reader.read_u8()? as usize;

                let mut value = vec![0; value_length];
                reader.read_exact(&mut value)?;
                let value_str = String::from_utf8_lossy(&value).to_string();

                let entry = ValueEntry::new_absolute(value_str.clone(), None);
                db.write().await.insert(key_str.clone(), entry);

                println!("Inserted key: {} with value: {} without expiration", key_str, value_str);
            }
            OPCODE_EOF => {
                println!("Reached end of RDB file.");

                let mut checksum_bytes = [0; 8];
                reader.read_exact(&mut checksum_bytes)?;
                let read_checksum = u64::from_le_bytes(checksum_bytes);
                println!("Read checksum (Little-Endian): {:X}", read_checksum);

                reader.seek(SeekFrom::Start(0))?;
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;

                let data_to_hash = &buffer[..buffer.len() - 8];

                let crc = Crc::<u64>::new(&CRC_64_ECMA_182);
                let calculated_checksum = crc.checksum(data_to_hash);
                println!("Calculated checksum: {:X}", calculated_checksum);

                if calculated_checksum == read_checksum {
                    println!("Checksum valid.");
                } else {
                    eprintln!("Checksum invalid! Expected: {:X}, Got: {:X}", read_checksum, calculated_checksum);
                }
                break;
            }
            _ => {
                eprintln!("Unknown or unsupported marker: 0x{:02X}", marker[0]);
                break;
            }
        }
    }

    Ok(())
}