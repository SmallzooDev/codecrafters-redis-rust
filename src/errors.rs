use thiserror::Error;

#[derive(Error, Debug)]
pub enum ArgumentError {
    #[error("Argument Error: {0}")]
    General(String),
}