use common_utils::types::keymanager::KeyManagerState;
use diesel::{associations::HasTable, ExpressionMethods, QueryDsl};
use diesel_models::{
    schema::refund::{created_at, merchant_id, refund_id},
    PgPooledConn,
};
use indicatif::MultiProgress;

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel_models::refund::Refund;
use error_stack::ResultExt;
use hyperswitch_domain_models::merchant_key_store::MerchantKeyStore;
use router::db::{
    errors::{ApplicationError, ApplicationResult},
    kafka_store::TenantID,
    KafkaProducer,
};
use time::PrimitiveDateTime;

pub async fn dump_refunds(
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
    let diesel_objects_count: i64 = Refund::table()
        .count()
        .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
        .filter(created_at.between(start_date, end_date))
        .get_result_async(conn)
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to get refunds count")?;
    // println!("{:?}", diesel_objects_count);
    let refund_progress_bar = multi_progress_bar.add(
        indicatif::ProgressBar::new(
            u64::try_from(diesel_objects_count)
                .change_context(ApplicationError::ConfigurationError)
                .attach_printable("Failed to convert refund count to u64")?,
        )
        .with_style(crate::progress_style()?)
        .with_message(format!(
            "{} Refunds:",
            merchant_key_store.merchant_id.get_string_repr()
        )),
    );
    for batch_offset in (0..diesel_objects_count).step_by(batch_size as usize) {
        let refunds = Refund::table()
            .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
            .filter(created_at.between(start_date, end_date))
            .limit(batch_size as i64)
            .offset(batch_offset)
            .order(refund_id)
            .get_results_async::<Refund>(conn)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to get refunds")?;
        let batch_progress_bar = multi_progress_bar.add(
            indicatif::ProgressBar::new(batch_size as u64)
                .with_style(crate::progress_style()?)
                .with_message(format!(
                    "{} Refunds Batch:",
                    merchant_key_store.merchant_id.get_string_repr()
                )),
        );
        for refund in refunds {
            // tokio::time::sleep(Duration::from_secs(1)).await;
            refund_progress_bar.inc(1);
            batch_progress_bar.inc(1);
            kafka_producer
                .log_refund(&refund, None, tenant_id.clone())
                .await
                .change_context(ApplicationError::ConfigurationError)?;
        }
    }
    Ok(())
}
