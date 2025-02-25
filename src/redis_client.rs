use tokio::net::tcp::OwnedWriteHalf;
use std::net::SocketAddr;

pub struct Client {
    writer: OwnedWriteHalf,
    addr: SocketAddr,
}

impl Client {
    pub fn new(writer: OwnedWriteHalf, addr: SocketAddr) -> Self {
        Self { writer, addr }
    }

    pub fn get_writer_mut(&mut self) -> &mut OwnedWriteHalf {
        &mut self.writer
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
}