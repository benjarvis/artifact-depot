// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Bloom filter helpers (using `bloomfilter` crate)
// ---------------------------------------------------------------------------

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
/// prefer `BloomAccumulator::or_from` which ORs in place.
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

/// Owned accumulator that folds many shard-local bloom filters into one
/// combined filter via bitwise-OR without reallocating per merge.
///
/// The `bloomfilter` crate only exposes `bitmap() -> Vec<u8>` (copy) and no
/// mutable bitmap accessor, so the accumulator owns the raw bits directly and
/// materialises a `Bloom<[u8]>` once via `finalize`.
pub(crate) struct BloomAccumulator {
    bits: Vec<u8>,
    num_bits: u64,
    num_hashes: u32,
    sip_keys: [(u64, u64); 2],
}

impl BloomAccumulator {
    /// Create an all-zero accumulator matching `template`'s dimensions.
    pub(crate) fn empty_like(template: &Bloom<[u8]>) -> Self {
        let num_bits = template.number_of_bits();
        Self {
            bits: vec![0u8; (num_bits / 8) as usize],
            num_bits,
            num_hashes: template.number_of_hash_functions(),
            sip_keys: template.sip_keys(),
        }
    }

    /// OR `source`'s bitmap into this accumulator in place.
    ///
    /// `source` must have been built from a template with identical
    /// dimensions (typically via `bloom_empty_like`). A mismatch would cause
    /// `zip` to silently truncate and drop bits from one side — an assert
    /// here is a runtime guard against a future refactor breaking the
    /// template-sharing invariant and corrupting the live-blob filter
    /// (which would then cause *real* blobs to be treated as orphans).
    pub(crate) fn or_from(&mut self, source: &Bloom<[u8]>) {
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
        for (dst, s) in self.bits.iter_mut().zip(src.iter()) {
            *dst |= *s;
        }
    }

    /// Materialise the accumulated bits as a `Bloom<[u8]>` ready for `check`.
    pub(crate) fn finalize(self) -> Bloom<[u8]> {
        Bloom::from_existing(&self.bits, self.num_bits, self.num_hashes, self.sip_keys)
    }
}
