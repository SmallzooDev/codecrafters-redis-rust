use crate::command::Command;
use std::net::SocketAddr;
use tokio::net::tcp::OwnedWriteHalf;

pub enum RedisEvent {
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
        writer: OwnedWriteHalf,
    },
    SlaveDisconnected {
        addr: SocketAddr,
    },
    PropagateSlave {
        addr: SocketAddr,
        message: String,
    },
} 