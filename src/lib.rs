pub mod data_transfer;
pub mod encryption;
pub mod utility;

pub fn progress_style() -> indicatif::ProgressStyle {
    indicatif::ProgressStyle::with_template(" {msg}\n{wide_bar} {pos}/{len} [{elapsed_precise}]")
        .expect("Failed to generate progress style")
}
