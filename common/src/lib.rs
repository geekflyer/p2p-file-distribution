pub mod types;

pub mod proto {
    tonic::include_proto!("file_transfer");
}

pub use types::*;
