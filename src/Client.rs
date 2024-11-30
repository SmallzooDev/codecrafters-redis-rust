use tokio::net::TcpStream;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::time::Instant;
use std::net::SocketAddr;

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
    pub connected_at: Instant,
    pub request_count: u64,
    addr: SocketAddr,
}

impl Client {
    pub fn new(id: u64, stream: TcpStream, addr: SocketAddr) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            id,
            reader,
            writer,
            connected_at: Instant::now(),
            request_count: 0,
            addr,
        }
    }

    pub fn get_reader(&mut self) -> &mut ReadHalf<TcpStream> {
        &mut self.reader
    }

    pub fn get_writer(&mut self) -> &mut WriteHalf<TcpStream> {
        &mut self.writer
    }

    pub async fn increment_request_count(&mut self) {
        self.request_count += 1;
    }

    pub async fn get_request_count(&mut self) -> u64 {
        self.request_count
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }
}