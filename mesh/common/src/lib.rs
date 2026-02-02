pub mod types;

pub mod proto {
    tonic::include_proto!("mesh");
}

pub use types::*;
