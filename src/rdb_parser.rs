use crate::config_handler::Db;
use crate::protocol_constants::{MAGIC_NUMBER, OPCODE_EOF, OPCODE_META, OPCODE_START_DB};
use crate::ValueEntry;
use std::io::{self};
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct RdbParser<'a, R> {
    reader: R,
    db: &'a mut Db,  // Db 타입 별칭 사용
}

impl<'a, R> RdbParser<'a, R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R, db: &'a mut Db) -> Self {
        Self { reader, db }
    }

    pub async fn parse(&mut self) -> io::Result<()> {
        self.verify_magic_number().await?;
        self.read_version().await?;
        self.process_entries().await?;
        // self.verify_checksum().await?;
        Ok(())
    }

    async fn verify_magic_number(&mut self) -> io::Result<()> {
        let mut magic = [0; 5];
        self.reader.read_exact(&mut magic).await?;
        if &magic != MAGIC_NUMBER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid RDB file format."));
        }
        println!("Valid Redis RDB file detected.");
        Ok(())
    }

    async fn read_version(&mut self) -> io::Result<()> {
        let mut version = [0; 4];
        self.reader.read_exact(&mut version).await?;
        println!("RDB Version: {}", String::from_utf8_lossy(&version));
        Ok(())
    }

    async fn process_entries(&mut self) -> io::Result<()> {
        loop {
            let mut marker = [0; 1];
            if self.reader.read_exact(&mut marker).await.is_err() {
                println!("End of file reached.");
                break;
            }
            println!("Processing marker: 0x{:02X}", marker[0]);
            match marker[0] {
                OPCODE_META => {
                    println!("Detected OPCODE_META");
                    self.process_metadata().await?;
                }
                OPCODE_START_DB => {
                    println!("Detected OPCODE_START_DB");
                    self.process_start_db().await?;
                }
                0xFB => {
                    println!("Detected Resize DB Opcode");
                    self.process_resize_db().await?;
                }
                0xFD | 0xFC => {
                    println!("Detected Expiry Opcode: {}", if marker[0] == 0xFD { "seconds" } else { "milliseconds" });
                    self.process_expiry(marker[0]).await?;
                }
                0x00 => {
                    println!("Detected Key without Expiration Opcode");
                    self.process_key_without_expiration().await?;
                }
                OPCODE_EOF => {
                    println!("Detected EOF Opcode");
                    break;
                }
                _ => eprintln!("Unknown or unsupported marker: 0x{:02X}", marker[0]),
            }
        }
        Ok(())
    }

    async fn process_metadata(&mut self) -> io::Result<()> {
        let first_key_byte = self.reader.read_u8().await?;
        let key_length = self.read_length_or_integer(first_key_byte).await?;

        let mut key_bytes = vec![0; key_length];
        self.reader.read_exact(&mut key_bytes).await?;
        let key = String::from_utf8_lossy(&key_bytes).to_string();

        let first_value_byte = self.reader.read_u8().await?;
        if first_value_byte >> 6 == 0b11 {
            let value = self.read_length_or_integer(first_value_byte).await?;
            println!("Metadata key: {}, value: {}", key, value);
        } else {
            let value_length = self.read_length_or_integer(first_value_byte).await?;
            let mut value_bytes = vec![0; value_length];
            self.reader.read_exact(&mut value_bytes).await?;

            match String::from_utf8(value_bytes.clone()) {
                Ok(value) => println!("Metadata key: {}, value: {}", key, value),
                Err(_) => {
                    let hex_value = value_bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ");
                    println!("Metadata key: {}, value (raw hex): {}", key, hex_value);
                }
            }
        }
        Ok(())
    }

    async fn process_start_db(&mut self) -> io::Result<()> {
        let db_index = self.reader.read_u8().await?;
        println!("Starting new database with index: {}", db_index);
        Ok(())
    }

    async fn process_resize_db(&mut self) -> io::Result<()> {
        let total_size = self.reader.read_u8().await?;
        let expires_size = self.reader.read_u8().await?;
        println!("Resize database: hash table size = {}, expires table size = {}", total_size, expires_size);
        Ok(())
    }

    async fn process_expiry(&mut self, marker: u8) -> io::Result<()> {
        let expiry_type = if marker == 0xFD { "seconds" } else { "milliseconds" };

        let expiration_ms = if expiry_type == "seconds" {
            let secs = self.reader.read_u32_le().await?;
            Some((secs as u64) * 1000)
        } else {
            let ms = self.reader.read_u64_le().await?;
            Some(ms)
        };

        let _value_type = self.reader.read_u8().await?;

        let key_length = self.reader.read_u8().await? as usize;
        let mut key = vec![0; key_length];
        self.reader.read_exact(&mut key).await?;
        let key_str = String::from_utf8_lossy(&key).to_string();

        let value_length = self.reader.read_u8().await? as usize;
        let mut value = vec![0; value_length];
        self.reader.read_exact(&mut value).await?;
        let value_str = String::from_utf8_lossy(&value).to_string();

        let entry = ValueEntry::new_absolute(value_str.clone(), expiration_ms);
        self.db.insert(key_str.clone(), entry);
        println!("Inserted key: {} with value: {} and expiration: {:?}", key_str, value_str, expiration_ms);
        Ok(())
    }

    async fn process_key_without_expiration(&mut self) -> io::Result<()> {
        let key_length = self.reader.read_u8().await? as usize;

        let mut key = vec![0; key_length];
        self.reader.read_exact(&mut key).await?;
        let key_str = String::from_utf8_lossy(&key).to_string();

        let value_length = self.reader.read_u8().await? as usize;

        let mut value = vec![0; value_length];
        self.reader.read_exact(&mut value).await?;
        let value_str = String::from_utf8_lossy(&value).to_string();

        let entry = ValueEntry::new_absolute(value_str.clone(), None);
        self.db.insert(key_str.clone(), entry);
        println!("Inserted key: {} with value: {} without expiration", key_str, value_str);
        Ok(())
    }

    // async fn verify_checksum(&mut self) -> io::Result<()> {
    //     let mut checksum_bytes = [0; 8];
    //     self.reader.read_exact(&mut checksum_bytes).await?;
    //     let read_checksum = u64::from_le_bytes(checksum_bytes);
    //
    //     self.reader.seek(SeekFrom::Start(0)).await?;
    //     let mut buffer = Vec::new();
    //     self.reader.read_to_end(&mut buffer).await?;
    //     let data_to_hash = &buffer[..buffer.len() - 8];
    //
    //     let crc = Crc::<u64>::new(&CRC_64_ECMA_182);
    //     let calculated_checksum = crc.checksum(data_to_hash);
    //
    //     if calculated_checksum == read_checksum {
    //         println!("Checksum is valid.");
    //         Ok(())
    //     } else {
    //         eprintln!("Invalid checksum!");
    //         Err(io::Error::new(io::ErrorKind::InvalidData, "Checksum mismatch"))
    //     }
    // }

    async fn read_length_or_integer(&mut self, first_byte: u8) -> io::Result<usize> {
        match first_byte >> 6 {
            0b00 => Ok((first_byte & 0x3F) as usize),
            0b01 => self.read_14bit_length(first_byte).await,
            0b10 => self.read_32bit_length().await,
            0b11 => self.read_encoded_integer(first_byte & 0x3F).await,
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid length encoding")),
        }
    }

    async fn read_14bit_length(&mut self, first_byte: u8) -> io::Result<usize> {
        let second_byte = self.reader.read_u8().await?;
        Ok((((first_byte & 0x3F) as usize) << 8) | (second_byte as usize))
    }

    async fn read_32bit_length(&mut self) -> io::Result<usize> {
        let len = self.reader.read_u32_le().await?;
        Ok(len as usize)
    }

    async fn read_encoded_integer(&mut self, encoding_type: u8) -> io::Result<usize> {
        match encoding_type {
            0 => {
                let val = self.reader.read_u8().await?;
                Ok(val as usize)
            }
            1 => {
                let val = self.reader.read_u16_le().await?;
                Ok(val as usize)
            }
            2 => {
                let val = self.reader.read_u32_le().await?;
                Ok(val as usize)
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unsupported encoding type")),
        }
    }
}