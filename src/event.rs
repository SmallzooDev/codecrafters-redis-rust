use crate::command::Command;
use std::net::SocketAddr;
use tokio::net::tcp::OwnedWriteHalf;

pub enum RedisEvent {
    Command {
        client_id: u64,
        command: Command,
    },
    ClientConnected {
        client_id: u64,
        writer: OwnedWriteHalf,
        addr: SocketAddr,
    },
    ClientDisconnected {
        client_id: u64,
    },

    CommandReceived {
        client_id: u64,
        command: Command,
    },

    SlaveConnected {
        addr: SocketAddr,
    },
    SlaveDisconnected {
        addr: SocketAddr,
    },
    PropagateSlave {
        message: String,
    },
} 