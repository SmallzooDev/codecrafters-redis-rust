use tokio::net::TcpStream;
use tokio::time::Instant;

#[derive(Debug)]
pub struct Client {
    pub id: u64,
    pub stream: TcpStream,
    pub connected_at: Instant,
    pub request_count: u64,
}

impl Client {
    pub fn new(id: u64, stream: TcpStream) -> Self {
        Self {
            id,
            stream,
            connected_at: Instant::now(),
            request_count: 0,
        }
    }

    pub async fn increment_request_count(&mut self) {
        self.request_count += 1;
    }

    pub async fn get_request_count(&mut self) -> u64 {
        self.request_count
    }
}