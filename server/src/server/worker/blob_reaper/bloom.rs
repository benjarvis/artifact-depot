// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// Bloom filter helpers (using `bloomfilter` crate)
// ---------------------------------------------------------------------------

use bloomfilter::Bloom;

/// Create an empty bloom filter with the same dimensions and hash seeds as
/// `template`, for parallel construction followed by `bloom_union`.
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
