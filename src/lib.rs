use diesel_models::enums::ApplicationError;
use error_stack::ResultExt;
use router::db::errors::ApplicationResult;

pub mod data_transfer;
pub mod encryption;
pub mod utility;

pub fn progress_style() -> ApplicationResult<indicatif::ProgressStyle> {
    indicatif::ProgressStyle::with_template(" {msg}\n{wide_bar} {pos}/{len} [{elapsed_precise}]")
        .attach_printable("Failed to generate progress style")
        .change_context(ApplicationError::ConfigurationError)
}
