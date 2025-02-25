use crate::value_entry::ValueEntry;
use std::collections::HashMap;
use std::io::{self};
use tokio::io::{AsyncRead, AsyncReadExt};
use crate::protocol_constants::{MAGIC_NUMBER, OPCODE_EOF, OPCODE_META, OPCODE_START_DB};

pub struct RdbParser<R> {
    reader: R,
    db: HashMap<String, ValueEntry>,
}

impl<R> RdbParser<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R, db: HashMap<String, ValueEntry>) -> Self {
        Self { reader, db }
    }

    pub async fn parse(mut self) -> io::Result<HashMap<String, ValueEntry>> {
        self.verify_magic_number().await?;
        self.read_version().await?;
        self.process_entries().await?;
        Ok(self.db)
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
        println!("RDB version: {}.{}", version[0], version[1]);
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
        let key_length = self.reader.read_u8().await? as usize;
        let mut key = vec![0; key_length];
        self.reader.read_exact(&mut key).await?;
        let key = String::from_utf8_lossy(&key).to_string();

        let value_length = self.reader.read_u8().await? as usize;
        let mut value_bytes = vec![0; value_length];
        self.reader.read_exact(&mut value_bytes).await?;

        match String::from_utf8(value_bytes.clone()) {
            Ok(value) => println!("Metadata key: {}, value: {}", key, value),
            Err(_) => {
                let hex_value = value_bytes.iter().map(|b| format!("{:02X}", b)).collect::<Vec<_>>().join(" ");
                println!("Metadata key: {}, value (raw hex): {}", key, hex_value);
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
}