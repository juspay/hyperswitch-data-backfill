use diesel::{associations::HasTable, ExpressionMethods, QueryDsl};
use diesel_models::{
    schema::payment_attempt::{created_at, merchant_id, payment_id},
    PgPooledConn,
};
use indicatif::MultiProgress;

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel_models::payment_attempt::PaymentAttempt as DieselPaymentAttempt;
use error_stack::ResultExt;
use hyperswitch_domain_models::{
    merchant_key_store::MerchantKeyStore,
    payments::payment_attempt::PaymentAttempt as DomainPaymentAttempt,
};
use router::{
    core::errors::{ApplicationError, ApplicationResult},
    db::{kafka_store::TenantID, KafkaProducer},
};
use storage_impl::DataModelExt;
use time::PrimitiveDateTime;

pub async fn dump_payment_attempts(
    kafka_producer: &KafkaProducer,
    conn: &PgPooledConn,
    multi_progress_bar: &MultiProgress,
    tenant_id: TenantID,
    mks: &MerchantKeyStore,
    batch_size: u32,
    start_date: PrimitiveDateTime,
    end_date: PrimitiveDateTime,
) -> ApplicationResult<()> {
    let diesel_objects_count: i64 = DieselPaymentAttempt::table()
        .filter(merchant_id.eq(mks.merchant_id.clone()))
        .filter(created_at.between(start_date, end_date))
        .count()
        .get_result_async(conn)
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to get payment attempts count")?;
    let pa_progress_bar = multi_progress_bar.add(
        indicatif::ProgressBar::new(
            u64::try_from(diesel_objects_count)
                .change_context(ApplicationError::ConfigurationError)
                .attach_printable("Failed to convert payment attempts count to u64")?,
        )
        .with_style(crate::progress_style()?)
        .with_message(format!("{:?} Payment Attempts:", mks.merchant_id)),
    );

    for batch_offset in (0..diesel_objects_count).step_by(batch_size as usize) {
        let payment_attempts = DieselPaymentAttempt::table()
            .filter(merchant_id.eq(mks.merchant_id.clone()))
            .filter(created_at.between(start_date, end_date))
            .limit(batch_size as i64)
            .offset(batch_offset)
            .order_by(payment_id)
            .get_results_async::<DieselPaymentAttempt>(conn)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to get payment attempts")?;
        let batch_progress_bar = multi_progress_bar.add(
            indicatif::ProgressBar::new(batch_size as u64)
                .with_style(crate::progress_style()?)
                .with_message(format!("{:?} Payment Attempts Batch:", mks.merchant_id)),
        );
        for pa in payment_attempts {
            pa_progress_bar.inc(1);
            batch_progress_bar.inc(1);
            let domain_pa = DomainPaymentAttempt::from_storage_model(pa);
            kafka_producer
                .log_payment_attempt(&domain_pa, None, tenant_id.clone())
                .await
                .change_context(ApplicationError::ConfigurationError)?;
        }
    }
    Ok(())
}
