pub mod record;
pub use record::{MAX_RECORD_SIZE, Operation, Record, WalError};

pub mod writer;
pub use writer::WalWriter;
