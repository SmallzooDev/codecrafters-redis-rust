use crate::protocol_constants::*;

pub fn construct_redis_command(args: &[&str]) -> String {
    let mut command = format!("{}{}{}", ARRAY_PREFIX, args.len(), CRLF);
    for arg in args {
        command.push_str(&format!("{}{}{}{}", BULK_STRING_PREFIX, arg.len(), CRLF, arg));
        command.push_str(CRLF);
    }
    command
}