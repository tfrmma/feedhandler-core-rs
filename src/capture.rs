// Bit-exact capture/replay for NormalizedTick streams. Point TickRecorder at
// a file while a session runs, feed a TickReader's output back into a fresh
// OrderBook later to reproduce an incident exactly instead of approximating
// it with a simulator. NormalizedTick is repr(C), 64 bytes, and has no
// invalid bit patterns except in its three restricted-range fields
// (exchange, side, is_snapshot), that's what makes this safe to build on raw
// bytes instead of needing a serialization framework.
//
// No framing, no compression, no schema version. If NormalizedTick's layout
// ever changes, old capture files stop being readable with the new binary.
// That's a real limitation, not an oversight, tag your capture files by
// crate version or git commit externally if that matters to you.
use crate::types::{Exchange, NormalizedTick, Side};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::mem::{offset_of, size_of};
use std::path::Path;

const TICK_SIZE: usize = size_of::<NormalizedTick>();

// Computed from the real field layout, not hand-counted, so this can't drift
// out of sync with types.rs if a field ever moves.
const EXCHANGE_OFFSET: usize = offset_of!(NormalizedTick, exchange);
const SIDE_OFFSET: usize = offset_of!(NormalizedTick, side);
const IS_SNAPSHOT_OFFSET: usize = offset_of!(NormalizedTick, is_snapshot);

pub struct TickRecorder {
    out: BufWriter<File>,
}

impl TickRecorder {
    /// Appends to `path`, creating it if it doesn't exist. Safe to reopen
    /// an existing capture file across process restarts, new records land
    /// after whatever's already there.
    ///
    /// # Errors
    /// Whatever the underlying `OpenOptions::open` returns, permissions,
    /// disk full, the usual.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(TickRecorder { out: BufWriter::new(file) })
    }

    #[inline]
    /// # Errors
    /// Whatever the underlying `write_all` returns, disk full, permissions,
    /// the usual.
    pub fn record(&mut self, tick: &NormalizedTick) -> io::Result<()> {
        // SAFETY: NormalizedTick is repr(C) with every byte, including the
        // explicit align pad, set by a real field, so this never reads
        // uninitialized memory. It's Copy, so nothing here can be
        // double-freed or left in a moved-from state either.
        let bytes = unsafe {
            std::slice::from_raw_parts((tick as *const NormalizedTick).cast::<u8>(), TICK_SIZE)
        };
        self.out.write_all(bytes)
    }

    /// `BufWriter` buffers internally, call this before your process exits
    /// or the tail of the session never makes it to disk.
    ///
    /// # Errors
    /// Whatever the underlying flush returns.
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

pub struct TickReader {
    input: BufReader<File>,
}

impl TickReader {
    /// # Errors
    /// Whatever the underlying `File::open` returns, missing file,
    /// permissions, the usual.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Ok(TickReader { input: BufReader::new(File::open(path)?) })
    }
}

impl Iterator for TickReader {
    type Item = io::Result<NormalizedTick>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = [0u8; TICK_SIZE];
        let mut got = 0;

        loop {
            match self.input.read(&mut buf[got..]) {
                Ok(0) => break,
                Ok(n) => {
                    got += n;
                    if got == TICK_SIZE {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Some(Err(e)),
            }
        }

        if got == 0 {
            return None; // clean end of file, no partial record left dangling
        }
        if got != TICK_SIZE {
            return Some(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("capture file truncated mid-record: got {got} of {TICK_SIZE} bytes"),
            )));
        }

        if let Err(e) = validate_record(&buf) {
            return Some(Err(e));
        }

        // SAFETY: validate_record already rejected any out-of-range
        // discriminant for exchange/side/is_snapshot, the only three fields
        // in NormalizedTick with restricted bit patterns. Every other field
        // is a plain integer or byte array, no bit pattern is invalid for
        // those. buf is exactly TICK_SIZE bytes read verbatim. read_unaligned,
        // not read: a stack [u8; 64] only guarantees 1-byte alignment, not
        // the 64-byte alignment NormalizedTick's repr demands, a plain
        // ptr::read on a misaligned pointer is UB regardless of the bytes
        // being otherwise valid.
        Some(Ok(unsafe { std::ptr::read_unaligned(buf.as_ptr().cast::<NormalizedTick>()) }))
    }
}

fn validate_record(buf: &[u8; TICK_SIZE]) -> io::Result<()> {
    let bad = |field: &str, val: u8| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt capture record: {field} discriminant {val} is out of range"),
        )
    };

    // Bump these if Exchange/Side ever grow a variant with a higher
    // discriminant, they're tied to the real enum, not a bare number, but
    // still need the new variant added to the max.
    let exchange = buf[EXCHANGE_OFFSET];
    if exchange > Exchange::Hyperliquid as u8 {
        return Err(bad("exchange", exchange));
    }

    let side = buf[SIDE_OFFSET];
    if side > Side::Ask as u8 {
        return Err(bad("side", side));
    }

    let is_snapshot = buf[IS_SNAPSHOT_OFFSET];
    if is_snapshot > 1 {
        return Err(bad("is_snapshot", is_snapshot));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Price, Qty, Symbol};
    use std::io::Seek;

    fn tick(seq: u64) -> NormalizedTick {
        NormalizedTick::new(
            Price::new(100 + seq),
            Qty::new(1),
            seq,
            0,
            0,
            Symbol::from_bytes(b"BTCUSDT"),
            Exchange::Binance,
            Side::Bid,
            false,
            0,
        )
    }

    #[test]
    fn round_trips_a_session_bit_for_bit() {
        let dir = std::env::temp_dir().join(format!("fh-capture-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.tick");

        let written: Vec<NormalizedTick> = (0..50).map(tick).collect();
        {
            let mut rec = TickRecorder::create(&path).unwrap();
            for t in &written {
                rec.record(t).unwrap();
            }
            rec.flush().unwrap();
        }

        let read: Vec<NormalizedTick> =
            TickReader::open(&path).unwrap().collect::<io::Result<_>>().unwrap();

        assert_eq!(written, read);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncated_trailing_record_is_an_error_not_a_silent_stop() {
        let dir = std::env::temp_dir().join(format!("fh-capture-trunc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("truncated.tick");

        {
            let mut rec = TickRecorder::create(&path).unwrap();
            rec.record(&tick(1)).unwrap();
            rec.record(&tick(2)).unwrap();
            rec.flush().unwrap();
        }
        // Chop the last record down to a partial write, the kind of thing a
        // crash mid-fwrite leaves behind.
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(TICK_SIZE as u64 + 10).unwrap();
        drop(file);

        let mut reader = TickReader::open(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), tick(1));
        let second = reader.next().expect("truncated record must surface, not be swallowed");
        assert!(second.is_err());
        assert!(reader.next().is_none(), "reader must stop cleanly after reporting the truncation");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_discriminant_is_rejected() {
        let dir = std::env::temp_dir().join(format!("fh-capture-corrupt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("corrupt.tick");

        {
            let mut rec = TickRecorder::create(&path).unwrap();
            rec.record(&tick(1)).unwrap();
        }
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(std::io::SeekFrom::Start(EXCHANGE_OFFSET as u64)).unwrap();
        file.write_all(&[200u8]).unwrap(); // no Exchange variant has this discriminant
        drop(file);

        let mut reader = TickReader::open(&path).unwrap();
        let result = reader.next().unwrap();
        assert!(result.is_err(), "an out-of-range enum discriminant must never reach ptr::read");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_file_yields_no_records() {
        let dir = std::env::temp_dir().join(format!("fh-capture-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.tick");
        TickRecorder::create(&path).unwrap().flush().unwrap();

        let mut reader = TickReader::open(&path).unwrap();
        assert!(reader.next().is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
