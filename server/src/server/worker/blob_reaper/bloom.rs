// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Bloom filter helpers (using `bloomfilter` crate)
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU8, Ordering};

use bloomfilter::Bloom;

/// Create an empty bloom filter with the same dimensions and hash seeds as
/// `template`, for parallel construction followed by merging into a
/// `BloomAccumulator` or via `bloom_union`.
pub(crate) fn bloom_empty_like(template: &Bloom<[u8]>) -> Bloom<[u8]> {
    let bits = template.number_of_bits();
    Bloom::from_existing(
        &vec![0u8; (bits / 8) as usize],
        bits,
        template.number_of_hash_functions(),
        template.sip_keys(),
    )
}

/// Merge `source` into `target` via bitwise OR. Both filters must have
/// identical dimensions (created via `bloom_empty_like`).
///
/// Allocates a fresh bitmap per call. For tight merge loops in hot paths,
/// prefer `BloomAccumulator::or_from` which ORs atomically in place.
pub(crate) fn bloom_union(target: &mut Bloom<[u8]>, source: &Bloom<[u8]>) {
    let merged: Vec<u8> = target
        .bitmap()
        .iter()
        .zip(source.bitmap().iter())
        .map(|(a, b)| a | b)
        .collect();
    *target = Bloom::from_existing(
        &merged,
        target.number_of_bits(),
        target.number_of_hash_functions(),
        target.sip_keys(),
    );
}

/// Lock-free accumulator that folds shard-local bloom filters into one
/// combined filter via atomic bitwise-OR. Designed so each shard task can
/// `or_from` its local filter (behind `Arc<Self>`, `&self`) and drop that
/// local filter immediately, keeping peak memory bounded by the number of
/// concurrent scan tasks rather than the total number of shards.
///
/// The `bloomfilter` crate has no atomic view of its bitmap, so the
/// accumulator owns its own `Vec<AtomicU8>` and materialises a plain
/// `Bloom<[u8]>` once via `finalize`.
pub(crate) struct BloomAccumulator {
    bits: Vec<AtomicU8>,
    num_bits: u64,
    num_hashes: u32,
    sip_keys: [(u64, u64); 2],
}

impl BloomAccumulator {
    /// Create an all-zero accumulator matching `template`'s dimensions.
    pub(crate) fn empty_like(template: &Bloom<[u8]>) -> Self {
        let num_bits = template.number_of_bits();
        let byte_count = (num_bits / 8) as usize;
        let mut bits = Vec::with_capacity(byte_count);
        bits.resize_with(byte_count, || AtomicU8::new(0));
        Self {
            bits,
            num_bits,
            num_hashes: template.number_of_hash_functions(),
            sip_keys: template.sip_keys(),
        }
    }

    /// OR `source`'s bitmap into this accumulator via per-byte atomic OR.
    ///
    /// Safe to call concurrently from many tasks with `&self`; each byte
    /// is updated independently with `Relaxed` ordering (sufficient because
    /// OR is commutative and idempotent, and the final read happens after
    /// every contributor has observed its `or_from` return).
    ///
    /// `source` must have been built from a template with identical
    /// dimensions (typically via `bloom_empty_like`). A mismatch would
    /// cause `zip` to silently truncate and drop bits from one side, which
    /// would corrupt the live-blob filter and cause real blobs to be
    /// treated as orphans. Guarded with a runtime assertion.
    pub(crate) fn or_from(&self, source: &Bloom<[u8]>) {
        let src = source.bitmap();
        assert_eq!(
            self.bits.len(),
            src.len(),
            "BloomAccumulator::or_from dimension mismatch: accumulator has {} bytes, source has {} bytes",
            self.bits.len(),
            src.len()
        );
        assert_eq!(
            self.num_bits,
            source.number_of_bits(),
            "BloomAccumulator::or_from num_bits mismatch"
        );
        assert_eq!(
            self.num_hashes,
            source.number_of_hash_functions(),
            "BloomAccumulator::or_from num_hashes mismatch"
        );
        assert_eq!(
            self.sip_keys,
            source.sip_keys(),
            "BloomAccumulator::or_from sip_keys mismatch"
        );
        for (dst, s) in self.bits.iter().zip(src.iter()) {
            // Avoid the atomic RMW for bytes whose src contribution is 0 —
            // the common case for sparse shard filters. Saves bus traffic
            // on the dense final fold.
            if *s != 0 {
                dst.fetch_or(*s, Ordering::Relaxed);
            }
        }
    }

    /// Materialise the accumulated bits as a `Bloom<[u8]>` ready for `check`.
    /// Consumes `self` so the temporary atomic storage is freed.
    pub(crate) fn finalize(self) -> Bloom<[u8]> {
        let bytes: Vec<u8> = self.bits.into_iter().map(|b| b.into_inner()).collect();
        Bloom::from_existing(&bytes, self.num_bits, self.num_hashes, self.sip_keys)
    }
}
