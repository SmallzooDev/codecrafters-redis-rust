use crate::command::{Command, ConfigCommand};
use crate::errors::ArgumentError;
use crate::protocol_constants::*;

pub fn parse_message(message: &str) -> Result<Command, ArgumentError> {
    let mut lines = message.lines();
    let first_line = lines.next().ok_or(ArgumentError::General(EMPTY_MESSAGE_ERROR.into()))?;

    if first_line.starts_with(ARRAY_PREFIX) {
        let num_args: usize = first_line[1..].parse().map_err(|_| ArgumentError::General(INVALID_ARRAY_SIZE_ERROR.into()))?;
        let mut args = Vec::new();

        for _ in 0..num_args {
            let bulk_len_line = lines.next().ok_or(ArgumentError::General(MISSING_BULK_LENGTH_ERROR.into()))?;
            if !bulk_len_line.starts_with(BULK_STRING_PREFIX) {
                return Err(ArgumentError::General(INVALID_BULK_STRING_FORMAT_ERROR.into()));
            }
            let bulk_len: usize = bulk_len_line[1..].parse().map_err(|_| ArgumentError::General(INVALID_BULK_LENGTH_ERROR.into()))?;
            let bulk_string = lines.next().ok_or(ArgumentError::General(MISSING_BULK_STRING_ERROR.into()))?;

            if bulk_string.len() != bulk_len {
                return Err(ArgumentError::General(BULK_STRING_LENGTH_MISMATCH_ERROR.into()));
            }
            args.push(bulk_string.to_string());
        }

        if let Some(command_name) = args.get(0).map(|s| s.as_str()) {
            match command_name {
                PING_COMMAND => parse_ping(&args),
                ECHO_COMMAND => parse_echo(&args),
                GET_COMMAND => parse_get(&args),
                SET_COMMAND => parse_set(&args),
                CONFIG_COMMAND => parse_config(&args),
                _ => Err(ArgumentError::General(format!("{}: {}", UNKNOWN_COMMAND_ERROR, command_name))),
            }
        } else {
            Err(ArgumentError::General(EMPTY_COMMAND_ERROR.into()))
        }
    } else {
        Err(ArgumentError::General(UNSUPPORTED_PROTOCOL_ERROR.into()))
    }
}

fn check_args_len(args: &[String], expected_len: usize, command_name: &str) -> Result<(), ArgumentError> {
    if args.len() != expected_len {
        Err(ArgumentError::General(format!("{}: {} {}", ARGUMENT_ERROR, command_name, expected_len - 1)))
    } else {
        Ok(())
    }
}

fn parse_ping(args: &[String]) -> Result<Command, ArgumentError> {
    check_args_len(args, 1, PING_COMMAND)?;
    Ok(Command::PING)
}

fn parse_echo(args: &[String]) -> Result<Command, ArgumentError> {
    check_args_len(args, 2, ECHO_COMMAND)?;
    Ok(Command::ECHO(args[1].clone()))
}

fn parse_get(args: &[String]) -> Result<Command, ArgumentError> {
    check_args_len(args, 2, GET_COMMAND)?;
    Ok(Command::GET(args[1].clone()))
}

fn parse_set(args: &[String]) -> Result<Command, ArgumentError> {
    if args.len() < 3 {
        return Err(ArgumentError::General(SET_ARGUMENTS_ERROR.into()));
    }

    let key = args[1].clone();
    let value = args[2].clone();
    let mut ex = None;
    let mut px = None;

    let mut arg_index = 3;
    while arg_index < args.len() {
        match args[arg_index].to_uppercase().as_str() {
            PX_OPTION => {
                px = Some(parse_option_value(&args, arg_index, PX_OPTION)?);
                arg_index += 2;
            }
            EX_OPTION => {
                ex = Some(parse_option_value(&args, arg_index, EX_OPTION)?);
                arg_index += 2;
            }
            _ => return Err(ArgumentError::General(format!("{}: '{}'", UNKNOWN_OPTION_ERROR, args[arg_index]))),
        }
    }

    Ok(Command::SET { key, value, ex, px })
}

fn parse_option_value(args: &[String], index: usize, option: &str) -> Result<u64, ArgumentError> {
    if index + 1 < args.len() {
        args[index + 1].parse::<u64>().map_err(|_| ArgumentError::General(format!("{}: {}", INVALID_OPTION_VALUE_ERROR, option)))
    } else {
        Err(ArgumentError::General(format!("{}: {}", OPTION_ARGUMENT_MISSING_ERROR, option)))
    }
}

fn parse_config(args: &[String]) -> Result<Command, ArgumentError> {
    if args.len() < 3 {
        return Err(ArgumentError::General(CONFIG_ARGUMENTS_ERROR.into()));
    }

    match args[1].to_uppercase().as_str() {
        CONFIG_GET_OPTION => Ok(Command::CONFIG(ConfigCommand::GET(args[2].clone()))),
        _ => Err(ArgumentError::General(UNSUPPORTED_CONFIG_SUBCOMMAND_ERROR.into())),
    }
}
