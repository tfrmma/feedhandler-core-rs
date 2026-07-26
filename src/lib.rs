#![warn(clippy::pedantic, clippy::nursery)]
// inline_always/module_name_repetitions: already curated out, this is a
// small crate with an intentionally flat module layout.
//
// must_use_candidate/use_self/missing_const_for_fn: pedantic style opinions,
// not correctness. Applying them means slapping #[must_use] on ~30 getters
// and swapping every struct-literal name for Self, a mechanical diff across
// the whole crate for zero behavior change. Not worth the churn.
//
// cast_precision_loss/cast_possible_truncation/cast_lossless: this crate
// does raw hardware-timer and latency-stats math (RDTSC ticks, ns
// conversion, percentile printing). The precision loss is understood and
// intentional; wrapping every conversion in try_from().unwrap() would add
// defensive boilerplate with no real safety benefit here.
//
// option_if_let_else: the one spot this fires reads better as a plain match
// with an inline comment than as a map_or() chain, not worth losing that.
//
// inconsistent_digit_grouping: price/qty literals group digits by the 1e-8
// fixed-point scale (1_0000_0000 reads as "1.00000000"), not by thousands,
// that's intentional throughout this crate, not a typo.
#![allow(
    clippy::inline_always,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::use_self,
    clippy::missing_const_for_fn,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::option_if_let_else,
    clippy::inconsistent_digit_grouping
)]

pub mod book_engine;
pub mod capture;
pub mod metrics;
pub mod ring_buffer;
mod sync;
pub mod timer;
pub mod types;

pub use book_engine::BookEngine;
pub use ring_buffer::SpscDisruptor;
pub use types::{
    BookSide, DeltaResult, Exchange, Level, NormalizedTick,
    OrderBook, Price, Qty, Side, Symbol,
};
