pub const ARRAY_PREFIX: &str = "*";
pub const BULK_STRING_PREFIX: &str = "$";
pub const SIMPLE_STRING_PREFIX: &str = "+";
pub const CRLF: &str = "\r\n";

pub const PING_COMMAND: &str = "PING";
pub const ECHO_COMMAND: &str = "ECHO";
pub const GET_COMMAND: &str = "GET";
pub const SET_COMMAND: &str = "SET";
pub const CONFIG_COMMAND: &str = "CONFIG";

pub const KEYS_COMMAND: &str = "KEYS";
pub const INFO_COMMAND: &str = "INFO";

pub const PX_OPTION: &str = "PX";
pub const EX_OPTION: &str = "EX";

pub const CONFIG_GET_OPTION: &str = "GET";

pub const OPCODE_START_DB: u8 = 0xFE;
pub const OPCODE_EXPIRETIME_MS: u8 = 0xFC;
pub const OPCODE_EXPIRETIME_S: u8 = 0xFD;
pub const OPCODE_META: u8 = 0xFA;

pub const OPCODE_SIZE: u8 = 0xFB;
pub const OPCODE_EOF: u8 = 0xFF;
pub const OPCODE_STRING: u8 = 0x00;
pub const OPCODE_LIST: u8 = 0x01;
pub const OPCODE_HASH: u8 = 0x04;
pub const MAGIC_NUMBER: &[u8] = b"REDIS";

// Error messages
pub const EMPTY_MESSAGE_ERROR: &str = "Empty message";
pub const INVALID_ARRAY_SIZE_ERROR: &str = "Invalid array size";
pub const MISSING_BULK_LENGTH_ERROR: &str = "Missing bulk length";
pub const INVALID_BULK_STRING_FORMAT_ERROR: &str = "Invalid bulk string format";
pub const INVALID_BULK_LENGTH_ERROR: &str = "Invalid bulk length";
pub const MISSING_BULK_STRING_ERROR: &str = "Missing bulk string";
pub const BULK_STRING_LENGTH_MISMATCH_ERROR: &str = "Bulk string length mismatch";
pub const EMPTY_COMMAND_ERROR: &str = "Empty command";
pub const UNSUPPORTED_PROTOCOL_ERROR: &str = "Unsupported protocol type";
pub const UNKNOWN_COMMAND_ERROR: &str = "Unknown command";

pub const ARGUMENT_ERROR: &str = "Argument Error";
pub const SET_ARGUMENTS_ERROR: &str = "SET requires at least key and value arguments";
pub const UNKNOWN_OPTION_ERROR: &str = "Unknown option";
pub const INVALID_OPTION_VALUE_ERROR: &str = "Invalid option value";
pub const OPTION_ARGUMENT_MISSING_ERROR: &str = "Option requires an argument";

pub const CONFIG_ARGUMENTS_ERROR: &str = "CONFIG subcommand requires at least 2 arguments";
pub const UNSUPPORTED_CONFIG_SUBCOMMAND_ERROR: &str = "Unsupported CONFIG subcommand";

pub const UNSUPPORTED_PATTERN_ERROR: &str = "Unsupported KEY command args pattern";