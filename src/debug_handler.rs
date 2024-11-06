use crate::Config;
use std::fs::File;
use std::io;
use std::io::{BufReader, Read};

pub async fn debug_file_structure(config: Config) -> io::Result<()> {
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

    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut buffer = [0; 16];
    let mut offset = 0;

    println!("--- RDB File Structure ---");
    while let Ok(bytes_read) = reader.read(&mut buffer) {
        if bytes_read == 0 {
            break;
        }

        print!("{:08X}: ", offset);
        for byte in &buffer[..bytes_read] {
            print!("{:02X} ", byte);
        }

        print!("  |");
        for byte in &buffer[..bytes_read] {
            if *byte >= 0x20 && *byte <= 0x7E {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");

        offset += bytes_read;
    }
    println!("--- End of RDB File ---");

    Ok(())
}