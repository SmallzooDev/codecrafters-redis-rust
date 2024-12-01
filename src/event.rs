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
        command: String,
    },

    SlaveConnected {
        addr: SocketAddr,
    },
    SlaveDisconnected {
        addr: SocketAddr,
    },
    SlaveOffsetUpdated {
        addr: SocketAddr,
        offset: i64,
    },
} 