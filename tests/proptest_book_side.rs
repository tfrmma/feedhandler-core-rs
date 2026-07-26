// BookSide's insert/remove do raw ptr::copy memmoves and a hand-rolled
// eviction path at MAX_LEVELS. The unit tests in src/book_engine.rs and
// src/types.rs only ever poke a handful of hand-picked prices, they don't
// exercise the memmove logic under the kind of chaotic, colliding,
// over-capacity input a real feed produces during a bad reconnect.
//
// This drives BookSide with random delta sequences and checks it against a
// dumb, obviously-correct reference model (a plain Vec, insert/remove by
// index, no unsafe anywhere) built from the same rules apply_delta claims
// to follow. Divergence between the two means the fast path is wrong, not
// that Vec is.
use feedhandler::types::MAX_LEVELS;
use feedhandler::{BookSide, Price, Qty};
use proptest::prelude::*;

// MAX_LEVELS is 64. A range that stays under that would never fill the book,
// which means insert()'s eviction and discard branches would never run.
// Going comfortably past it (100 distinct prices) forces the book to
// capacity and keeps testing what happens to the next insert after that.
const PRICE_RANGE: std::ops::Range<u64> = 0..100;
const QTY_RANGE:   std::ops::Range<u64> = 0..6; // 0 means cancel/remove

fn levels_of(side: &BookSide) -> Vec<(u64, u64)> {
    side.levels[..side.count].iter().map(|l| (l.price.raw(), l.qty.raw())).collect()
}

// Mirrors apply_delta's documented rules exactly, using safe Vec ops instead
// of raw memmove. qty=0 removes a tracked price (no-op if untracked). qty>0
// updates in place if tracked; otherwise finds the sorted insertion point,
// inserts if there's room, evicts the worst level if the book's already at
// MAX_LEVELS and this price still beats it, or discards if it doesn't.
fn apply_to_model(model: &mut Vec<(u64, u64)>, price: u64, qty: u64, descending: bool) {
    let pos = model.iter().position(|&(p, _)| p == price);

    if qty == 0 {
        if let Some(idx) = pos {
            model.remove(idx);
        }
        return;
    }

    if let Some(idx) = pos {
        model[idx].1 = qty;
        return;
    }

    let insert_at = model.iter().position(|&(p, _)| {
        if descending { p < price } else { p > price }
    }).unwrap_or(model.len());

    if model.len() < MAX_LEVELS {
        model.insert(insert_at, (price, qty));
    } else if insert_at < MAX_LEVELS {
        model.insert(insert_at, (price, qty));
        model.truncate(MAX_LEVELS);
    }
    // else: worse than everything tracked and the book's full, discard
}

fn is_sorted(levels: &[(u64, u64)], descending: bool) -> bool {
    levels.windows(2).all(|w| if descending { w[0].0 > w[1].0 } else { w[0].0 < w[1].0 })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn book_side_matches_reference_model(
        descending in any::<bool>(),
        ops in prop::collection::vec((PRICE_RANGE, QTY_RANGE), 0..400),
    ) {
        let mut side = BookSide::new();
        let mut model: Vec<(u64, u64)> = Vec::new();

        for (price, qty) in ops {
            side.apply_delta(Price::new(price), Qty::new(qty), descending);
            apply_to_model(&mut model, price, qty, descending);

            prop_assert!(side.count <= MAX_LEVELS);
            prop_assert!(is_sorted(&levels_of(&side), descending), "book side lost its sort order");
            prop_assert_eq!(levels_of(&side), model.clone(), "diverged from the reference model");
        }
    }

    #[test]
    fn no_duplicate_prices_ever(
        descending in any::<bool>(),
        ops in prop::collection::vec((PRICE_RANGE, QTY_RANGE), 0..400),
    ) {
        let mut side = BookSide::new();
        for (price, qty) in ops {
            side.apply_delta(Price::new(price), Qty::new(qty), descending);
            let levels = levels_of(&side);
            let mut prices: Vec<u64> = levels.iter().map(|&(p, _)| p).collect();
            let before = prices.len();
            prices.dedup();
            prop_assert_eq!(prices.len(), before, "same price tracked twice");
        }
    }
}
