//! OpenClaw channel transport primitives for Robrix runtimes.

pub mod transport;

pub use transport::{
    OpenClawEvent, OpenClawEventStream, OpenClawStreamRequest, OpenClawTransport,
    OpenClawTransportError, OpenClawWsTransport,
};
