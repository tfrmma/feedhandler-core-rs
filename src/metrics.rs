// Plain-text Prometheus exposition format for the counters this crate
// already tracks. No metrics client library, no background thread, no HTTP
// server, just string formatting against fields that were already public.
// Wire the output into whatever endpoint your service already exposes
// (warp, axum, actix, a bare TcpListener, whatever).
use crate::ring_buffer::SpscDisruptor;
use crate::types::OrderBook;
use std::fmt::Write;

// TYPE/HELP lines are metric-level metadata, Prometheus wants them declared
// once, not once per sample. Call this once regardless of how many books or
// rings you're exporting, then append write_metrics() for each.
pub fn write_metrics_header(out: &mut String) {
    let metrics: &[(&str, &str, &str)] = &[
        ("feedhandler_symbol_mismatches_total", "counter", "Ticks discarded for carrying the wrong Symbol"),
        ("feedhandler_sequence_gaps_total",     "counter", "Sequence numbers that jumped by more than one"),
        ("feedhandler_sequence_reorders_total", "counter", "Sequence numbers that went backward or repeated"),
        ("feedhandler_checksum_failures_total", "counter", "Adapter-reported checksum mismatches"),
        ("feedhandler_book_last_update_ns",     "gauge",   "ts_recv_ns of the last tick actually applied"),
        ("feedhandler_book_crossed",            "gauge",   "1 if best bid trades through best ask, else 0"),
        ("feedhandler_ring_dropped_total",      "counter", "Ticks lost because the ring was full on publish"),
        ("feedhandler_ring_pending",            "gauge",   "Approximate ticks currently queued in the ring"),
    ];
    for (name, kind, help) in metrics {
        let _ = writeln!(out, "# HELP {name} {help}.");
        let _ = writeln!(out, "# TYPE {name} {kind}");
    }
}

impl OrderBook {
    /// Appends this book's health counters as Prometheus samples. Doesn't
    /// write the TYPE/HELP block, call `write_metrics_header()` once for that.
    pub fn write_metrics(&self, out: &mut String) {
        let sym = self.symbol.as_str();
        let ex  = self.exchange.as_str();
        let _ = writeln!(out, r#"feedhandler_symbol_mismatches_total{{symbol="{sym}",exchange="{ex}"}} {}"#, self.symbol_mismatches);
        let _ = writeln!(out, r#"feedhandler_sequence_gaps_total{{symbol="{sym}",exchange="{ex}"}} {}"#, self.sequence_gaps);
        let _ = writeln!(out, r#"feedhandler_sequence_reorders_total{{symbol="{sym}",exchange="{ex}"}} {}"#, self.sequence_reorders);
        let _ = writeln!(out, r#"feedhandler_checksum_failures_total{{symbol="{sym}",exchange="{ex}"}} {}"#, self.checksum_failures);
        let _ = writeln!(out, r#"feedhandler_book_last_update_ns{{symbol="{sym}",exchange="{ex}"}} {}"#, self.last_update_ns);
        let _ = writeln!(out, r#"feedhandler_book_crossed{{symbol="{sym}",exchange="{ex}"}} {}"#, u8::from(self.is_crossed()));
    }
}

impl SpscDisruptor {
    /// `ring_name` is your label, "binance-btcusdt" or whatever identifies
    /// this ring in your setup, this crate has no opinion on naming.
    pub fn write_metrics(&self, out: &mut String, ring_name: &str) {
        let _ = writeln!(out, r#"feedhandler_ring_dropped_total{{ring="{ring_name}"}} {}"#, self.dropped_count());
        let _ = writeln!(out, r#"feedhandler_ring_pending{{ring="{ring_name}"}} {}"#, self.pending());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, Symbol};

    #[test]
    fn header_declares_every_metric_exactly_once() {
        let mut out = String::new();
        write_metrics_header(&mut out);

        for name in [
            "feedhandler_symbol_mismatches_total",
            "feedhandler_sequence_gaps_total",
            "feedhandler_sequence_reorders_total",
            "feedhandler_checksum_failures_total",
            "feedhandler_book_last_update_ns",
            "feedhandler_book_crossed",
            "feedhandler_ring_dropped_total",
            "feedhandler_ring_pending",
        ] {
            assert_eq!(out.matches(&format!("# TYPE {name}")).count(), 1, "{name} TYPE line missing or duplicated");
            assert_eq!(out.matches(&format!("# HELP {name}")).count(), 1, "{name} HELP line missing or duplicated");
        }
    }

    #[test]
    fn book_metrics_reflect_live_counters() {
        let mut book = OrderBook::new(Symbol::from_bytes(b"BTCUSDT"), Exchange::Binance);
        book.symbol_mismatches = 3;
        book.sequence_gaps = 7;
        book.record_checksum_failure();

        let mut out = String::new();
        book.write_metrics(&mut out);

        assert!(out.contains(r#"feedhandler_symbol_mismatches_total{symbol="BTCUSDT",exchange="binance"} 3"#));
        assert!(out.contains(r#"feedhandler_sequence_gaps_total{symbol="BTCUSDT",exchange="binance"} 7"#));
        assert!(out.contains(r#"feedhandler_checksum_failures_total{symbol="BTCUSDT",exchange="binance"} 1"#));
        assert!(out.contains(r#"feedhandler_book_crossed{symbol="BTCUSDT",exchange="binance"} 0"#));
    }

    #[test]
    fn ring_metrics_use_the_given_label() {
        let ring = SpscDisruptor::new();
        let mut out = String::new();
        ring.write_metrics(&mut out, "binance-btcusdt");

        assert!(out.contains(r#"feedhandler_ring_dropped_total{ring="binance-btcusdt"} 0"#));
        assert!(out.contains(r#"feedhandler_ring_pending{ring="binance-btcusdt"} 0"#));
    }
}
