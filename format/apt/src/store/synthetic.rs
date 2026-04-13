// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use depot_core::error;
use flate2::write::GzEncoder;
use flate2::Compression;

/// Build a synthetic .deb file for testing/demo purposes.
pub fn build_synthetic_deb(
    package: &str,
    version: &str,
    arch: &str,
    description: &str,
) -> error::Result<Vec<u8>> {
    // Build control file
    let control = format!(
        "Package: {package}\n\
         Version: {version}\n\
         Architecture: {arch}\n\
         Maintainer: Depot Demo <demo@depot>\n\
         Section: misc\n\
         Priority: optional\n\
         Installed-Size: 1\n\
         Description: {description}\n"
    );

    // Build control.tar.gz
    let mut control_tar = tar::Builder::new(Vec::new());
    let control_bytes = control.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_path("./control")?;
    header.set_size(control_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    control_tar.append(&header, control_bytes)?;
    let control_tar_data = control_tar.into_inner()?;

    let mut gz_encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut gz_encoder, &control_tar_data)?;
    let control_tar_gz = gz_encoder.finish()?;

    // Build data.tar.gz (empty)
    let data_tar = tar::Builder::new(Vec::new());
    let data_tar_data = data_tar.into_inner()?;
    let mut gz_encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut gz_encoder, &data_tar_data)?;
    let data_tar_gz = gz_encoder.finish()?;

    // Build debian-binary
    let debian_binary = b"2.0\n";

    // Build ar archive
    let mut ar_buf = Vec::new();
    {
        let mut builder = ar::Builder::new(&mut ar_buf);

        let mut header = ar::Header::new(b"debian-binary".to_vec(), debian_binary.len() as u64);
        header.set_mode(0o100644);
        builder.append(&header, &debian_binary[..])?;

        let mut header = ar::Header::new(b"control.tar.gz".to_vec(), control_tar_gz.len() as u64);
        header.set_mode(0o100644);
        builder.append(&header, control_tar_gz.as_slice())?;

        let mut header = ar::Header::new(b"data.tar.gz".to_vec(), data_tar_gz.len() as u64);
        header.set_mode(0o100644);
        builder.append(&header, data_tar_gz.as_slice())?;
    }

    Ok(ar_buf)
}
