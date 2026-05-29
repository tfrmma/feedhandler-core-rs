#![warn(clippy::pedantic, clippy::nursery)]
#![allow(clippy::inline_always, clippy::module_name_repetitions)]

pub mod book_engine;
pub mod ring_buffer;
pub mod timer;
pub mod types;

pub use book_engine::BookEngine;
pub use ring_buffer::SpscDisruptor;
pub use types::{
    BookSide, DeltaResult, Exchange, Level, NormalizedTick,
    OrderBook, Price, Qty, Side, Symbol,
};
