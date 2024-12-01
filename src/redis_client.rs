use std::net::SocketAddr;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    pub writer: OwnedWriteHalf,
    pub connected_at: Instant,
    pub request_count: u64,
    pub addr: SocketAddr,
}

impl Client {
    pub fn new(id: u64, writer: OwnedWriteHalf, addr: SocketAddr) -> Self {
        Self {
            id,
            writer,
            connected_at: Instant::now(),
            request_count: 0,
            addr,
        }
    }

    pub fn get_writer(&mut self) -> &mut OwnedWriteHalf {
        &mut self.writer
    }

    pub fn increment_request_count(&mut self) {
        self.request_count += 1;
    }

    pub fn get_request_count(&self) -> u64 {
        self.request_count
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
}