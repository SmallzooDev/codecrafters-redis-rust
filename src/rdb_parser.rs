use crate::protocol_constants::{MAGIC_NUMBER, OPCODE_EOF, OPCODE_META, OPCODE_START_DB};
use crate::{Config, Db, ValueEntry};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use crc::{Crc, CRC_64_ECMA_182};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<String>>().join(" ")
}

fn read_length_or_integer<R: Read>(reader: &mut R, first_byte: u8) -> io::Result<usize> {
    match first_byte >> 6 {
        0b00 => Ok((first_byte & 0x3F) as usize),
        0b01 => {
            let second_byte = reader.read_u8()?;
            Ok((((first_byte & 0x3F) as usize) << 8) | (second_byte as usize))
        }
        0b10 => reader.read_u32::<BigEndian>().map(|len| len as usize),
        0b11 => match first_byte & 0x3F {
            0 => Ok(reader.read_u8()? as usize),
            1 => Ok(reader.read_u16::<BigEndian>()? as usize),
            2 => Ok(reader.read_u32::<BigEndian>()? as usize),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported encoding type")),
        },
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid length encoding")),
    }
}

pub async fn run(db: Db, config: Config) -> io::Result<()> {
    let config_read = config.read().await;
    let dir = config_read.get("dir").cloned().unwrap_or_else(|| "".to_string());
    let db_file_name = config_read.get("dbfilename").cloned().unwrap_or_else(|| "".to_string());
    let mut path = "".to_string();

    if !dir.is_empty() && !db_file_name.is_empty() {
        println!("Initiating Redis with Data File");
        path = format!("{}/{}", dir, db_file_name);
    } else {
        println!("Initiating Redis without Data File");
        return Ok(());
    }

    let file = File::open(&path)?;
    let mut reader = BufReader::new(file);

    // Check magic number
    let mut magic = [0; 5];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC_NUMBER {
        eprintln!("Invalid RDB file format.");
        return Ok(());
    }
    println!("Valid Redis RDB file detected.");

    // Check RDB version
    let mut version = [0; 4];
    reader.read_exact(&mut version)?;
    println!("RDB Version: {}", String::from_utf8_lossy(&version));

    loop {
        let marker_position = reader.stream_position()?;
        let mut marker = [0; 1];
        if reader.read_exact(&mut marker).is_err() {
            println!("Reached the end of file.");
            break;
        }
        println!("Marker 0x{:02X} detected at position {}", marker[0], marker_position);

        match marker[0] {
            OPCODE_META => {
                println!("Detected metadata marker 0xFA.");
                let first_key_byte = reader.read_u8()?;
                let key_length = read_length_or_integer(&mut reader, first_key_byte)?;

                let mut key_bytes = vec![0; key_length];
                reader.read_exact(&mut key_bytes)?;
                let key = String::from_utf8_lossy(&key_bytes);
                println!("Metadata key: {}", key);

                let first_value_byte = reader.read_u8()?;
                if first_value_byte >> 6 == 0b11 {
                    let value = read_length_or_integer(&mut reader, first_value_byte)?;
                    println!("Metadata value (interpreted as integer): {}", value);
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
                let expiration = if expiry_type == "seconds" {
                    Some(reader.read_u32::<LittleEndian>()? as u64)
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

                let entry = ValueEntry::new(
                    value_str.clone(),
                    if marker[0] == 0xFD { expiration } else { None },
                    if marker[0] == 0xFC { expiration } else { None },
                );
                db.write().await.insert(key_str.clone(), entry);
                println!("Inserted key: {} with value: {}", key_str, value_str);
                continue;
            }
            0x00 => {
                println!("Processing key-value pair without expiration.");

                // Interpreting next byte(s) as key length rather than `value_type`
                let key_length_position = reader.stream_position()?;
                let key_length = reader.read_u8()? as usize;
                println!("Key length: {} at position {}", key_length, key_length_position);

                let mut key = vec![0; key_length];
                reader.read_exact(&mut key)?;
                let key_str = String::from_utf8_lossy(&key).to_string();
                let after_key_position = reader.stream_position()?;
                println!("Read key: {} at position {}", key_str, after_key_position);

                // Reading value length next
                let value_length_position = reader.stream_position()?;
                let value_length = reader.read_u8()? as usize;
                println!("Value length: {} at position {}", value_length, value_length_position);

                let mut value = vec![0; value_length];
                reader.read_exact(&mut value)?;
                let value_str = String::from_utf8_lossy(&value).to_string();
                let after_value_position = reader.stream_position()?;
                println!("Read value: {} at position {}", value_str, after_value_position);

                let entry = ValueEntry::new(value_str.clone(), None, None);
                db.write().await.insert(key_str.clone(), entry);
                println!("Inserted key: {} with value: {}", key_str, value_str);
            }
            OPCODE_EOF => {
                println!("Reached end of RDB file.");

                // Read the checksum bytes in Big-Endian format to check if this resolves the issue
                let mut checksum_bytes = [0; 8];
                reader.read_exact(&mut checksum_bytes)?;
                let read_checksum = u64::from_be_bytes(checksum_bytes);  // Change to Big-Endian
                println!("Read checksum (Big-Endian): {:X}", read_checksum);

                // Calculate checksum on the file content excluding the last 8 bytes
                reader.seek(SeekFrom::Start(0))?;
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer)?;

                let data_to_hash = &buffer[..buffer.len() - 8];
                println!("Data length for checksum calculation: {}", data_to_hash.len());
                println!("Data for checksum calculation (first 64 bytes): {}", bytes_to_hex(&data_to_hash[..64.min(data_to_hash.len())]));

                // CRC64 checksum calculation with variations
                let crc = Crc::<u64>::new(&CRC_64_ECMA_182);
                let calculated_checksum = crc.checksum(data_to_hash);
                println!("Calculated checksum: {:X}", calculated_checksum);

                // Compare calculated checksum with the one in the file
                if calculated_checksum == read_checksum {
                    println!("Checksum valid.");
                } else {
                    eprintln!("Checksum invalid! Expected: {:X}, Got: {:X}", read_checksum, calculated_checksum);
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid checksum"));
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