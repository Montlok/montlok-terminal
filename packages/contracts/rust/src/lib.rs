#![forbid(unsafe_code)]

pub mod v2 {
    include!(concat!(env!("OUT_DIR"), "/montlok.v2.rs"));
}
pub mod columnar;
pub mod wire_json;

pub const PROTOCOL_VERSION: u32 = 2;
pub const MAX_SNAPSHOT_ROWS: usize = 2_048;
pub const MAX_SNAPSHOT_BYTES: usize = 1_048_576;
