use async_bb8_diesel::AsyncRunQueryDsl;
use common_utils::types::keymanager::KeyManagerState;
use diesel::{associations::HasTable, ExpressionMethods, QueryDsl};
use diesel_models::dispute::Dispute;
use diesel_models::schema::dispute::dispute_id;
use diesel_models::{
    schema::dispute::{created_at, merchant_id},
    PgPooledConn,
};
use error_stack::ResultExt;
use hyperswitch_domain_models::merchant_key_store::MerchantKeyStore;
use indicatif::MultiProgress;
use router::{
    core::errors::{ApplicationError, ApplicationResult},
    db::{kafka_store::TenantID, KafkaProducer},
};
use time::PrimitiveDateTime;

pub async fn dump_disputes(
    kafka_producer: &KafkaProducer,
    conn: &PgPooledConn,
    multi_progress_bar: &MultiProgress,
    tenant_id: TenantID,
    _key_manager_state: &KeyManagerState,
    merchant_key_store: &MerchantKeyStore,
    batch_size: u32,
    start_date: PrimitiveDateTime,
    end_date: PrimitiveDateTime,
) -> ApplicationResult<()> {
    let diesel_objects_count: i64 = Dispute::table()
        .count()
        .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
        .filter(created_at.between(start_date, end_date))
        .get_result_async(conn)
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to get disputes count")?;
    // println!("{:?}", diesel_objects_count);
    let dispute_progress_bar = multi_progress_bar.add(
        indicatif::ProgressBar::new(
            u64::try_from(diesel_objects_count)
                .change_context(ApplicationError::ConfigurationError)
                .attach_printable("Failed to convert dispute count to u64")?,
        )
        .with_style(crate::progress_style()?)
        .with_message(format!(
            "{} Disputes:",
            merchant_key_store.merchant_id.get_string_repr()
        )),
    );

    for batch_offset in (0..diesel_objects_count).step_by(batch_size as usize) {
        let disputes = Dispute::table()
            .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
            .filter(created_at.between(start_date, end_date))
            .limit(batch_size as i64)
            .offset(batch_offset)
            .order(dispute_id)
            .get_results_async::<Dispute>(conn)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to get disputes")?;
        let batch_progress_bar = multi_progress_bar.add(
            indicatif::ProgressBar::new(batch_size as u64)
                .with_style(crate::progress_style()?)
                .with_message(format!(
                    "{} Disputes Batch:",
                    merchant_key_store.merchant_id.get_string_repr()
                )),
        );

        for dispute in disputes {
            dispute_progress_bar.inc(1);
            batch_progress_bar.inc(1);
            kafka_producer
                .log_dispute(&dispute, None, tenant_id.clone())
                .await
                .change_context(ApplicationError::ConfigurationError)?;
        }
    }

    Ok(())
}
