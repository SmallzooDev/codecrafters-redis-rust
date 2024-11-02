use crate::{Config, Db, ValueEntry};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub enum Command {
    PING,
    ECHO(String),
    GET(String),
    SET { key: String, value: String, px: Option<u64>, ex: Option<u64> },
    CONFIG(ConfigCommand),
}

pub enum ConfigCommand {
    GET(String),
}

impl Command {
    pub fn parse_message(message: &str) -> Result<Command, String> {
        let mut lines = message.lines();
        let first_line = lines.next().ok_or("Argument Error : Empty message")?;

        if first_line.starts_with('*') {
            let num_args: usize = first_line[1..].parse().map_err(|_| "Invalid array size")?;
            let mut args = Vec::new();

            for _ in 0..num_args {
                let bulk_len_line = lines.next().ok_or("Missing bulk length")?;
                if !bulk_len_line.starts_with('$') {
                    return Err("Invalid bulk string format".into());
                }
                let bulk_len: usize = bulk_len_line[1..].parse().map_err(|_| "Invalid bulk length")?;
                let bulk_string = lines.next().ok_or("Missing bulk string")?;

                if bulk_string.len() != bulk_len {
                    return Err("Bulk string length mismatch".into());
                }
                args.push(bulk_string.to_string());
            }

            if let Some(command_name) = args.get(0).map(|s| s.as_str()) {
                match command_name {
                    "PING" => Command::parse_ping(&args),
                    "ECHO" => Command::parse_echo(&args),
                    "GET" => Command::parse_get(&args),
                    "SET" => Command::parse_set(&args),
                    "CONFIG" => Command::parse_config(&args),
                    _ => Err(format!("Unknown command: {}", command_name)),
                }
            } else {
                Err("Empty command".into())
            }
        } else {
            Err("Unsupported protocol type".into())
        }
    }

    pub async fn handle_command(&self, stream: &mut TcpStream, db: Db, config: Config) -> std::io::Result<()> {
        let response = self.execute(db, config).await;
        stream.write_all(response.as_bytes()).await?;
        Ok(())
    }

    pub async fn execute(&self, db: Db, config: Config) -> String {
        match self {
            Command::PING => "+PONG\r\n".to_string(),
            Command::ECHO(echo_message) => format!("${}\r\n{}\r\n", echo_message.len(), echo_message),
            Command::GET(key) => Self::execute_get(&key, db).await,
            Command::SET { key, value, ex, px } => Self::execute_set(key, value, *ex, *px, db).await,
            Command::CONFIG(command) => Self::execute_config(command, config).await,
        }
    }

    async fn execute_get(key: &String, db: Db) -> String {
        match db.read().await.get(key) {
            Some(value_entry) => {
                if value_entry.is_expired() {
                    "$-1\r\n".to_string()
                } else {
                    format!("${}\r\n{}\r\n", value_entry.value.len(), value_entry.value.clone())
                }
            }
            None => "$-1\r\n".to_string(),
        }
    }

    async fn execute_set(key: &String, value: &String, ex: Option<u64>, px: Option<u64>, db: Db) -> String {
        db.write().await.insert(key.clone(), ValueEntry::new(value.clone(), ex, px));
        "+OK\r\n".to_string()
    }

    async fn execute_config(command: &ConfigCommand, config: Config) -> String {
        match command {
            ConfigCommand::GET(key) => {
                match config.read().await.get(key.as_str()) {
                    Some(value) => {
                        format!("*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value)
                    }
                    None => "$-1\r\n".to_string(),
                }
            }
        }
    }

    fn parse_ping(args: &[String]) -> Result<Command, String> {
        if !(args.len() == 1) {
            return Err("Argument Error : PING command takes no arguments".into());
        }
        Ok(Command::PING)
    }


    fn parse_echo(args: &[String]) -> Result<Command, String> {
        if !(args.len() == 2) {
            return Err("Argument Error : ECHO command takes only one argument".into());
        }
        Ok(Command::ECHO(args[1].clone()))
    }

    fn parse_get(args: &[String]) -> Result<Command, String> {
        if !(args.len() == 2) {
            return Err("Argument Error : GET command takes only one argument".into());
        }
        Ok(Command::GET(args[1].clone()))
    }

    fn parse_set(args: &[String]) -> Result<Command, String> {
        if args.len() < 3 {
            return Err("Argument Error : SET requires at least key value argument".into());
        }

        let key = args[1].clone();
        let value = args[2].clone();
        let mut ex = None;
        let mut px = None;

        let mut arg_index = 3;
        while arg_index < args.len() {
            match args[arg_index].to_uppercase().as_str() {
                "PX" => {
                    if arg_index + 1 < args.len() {
                        px = Some(args[arg_index + 1].parse::<u64>().map_err(|_| "Argument Error : Invalid px value")?);
                        arg_index += 2;
                    } else {
                        return Err("Argument Error : Px option argument err".into());
                    }
                }
                "EX" => {
                    if arg_index + 1 < args.len() {
                        ex = Some(args[arg_index + 1].parse::<u64>().map_err(|_| "Argument Error : Invalid ex value")?);
                        arg_index += 2;
                    } else {
                        return Err("Argument Error : Ex option argument err".into());
                    }
                }
                _ => return Err(format!("Argument Error: {} unknown option", args[arg_index]))
            }
        }

        Ok(Command::SET { key, value, ex, px })
    }

    fn parse_config(args: &[String]) -> Result<Command, String> {
        match args[1].to_uppercase().as_str() {
            "GET" => {
                Ok(Command::CONFIG(ConfigCommand::GET(args[2].clone())))
            }
            _ => Err("Argument Error : Unsupported CONFIG subcommand!".into()),
        }
    }
}