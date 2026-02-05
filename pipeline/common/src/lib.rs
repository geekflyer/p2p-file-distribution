pub mod types;

pub mod proto {
    tonic::include_proto!("pipeline");
}

pub use types::*;
