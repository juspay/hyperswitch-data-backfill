use std::sync::Arc;

use common_utils::types::keymanager::KeyManagerState;
use diesel::{associations::HasTable, ExpressionMethods, QueryDsl};
use diesel_models::{schema::payment_intent::merchant_id, PgPooledConn};
use indicatif::MultiProgress;

use async_bb8_diesel::AsyncRunQueryDsl;
use diesel_models::payment_intent::PaymentIntent as DieselPaymentIntent;
use error_stack::ResultExt;
use hyperswitch_domain_models::{
    behaviour::Conversion, merchant_key_store::MerchantKeyStore,
    payments::PaymentIntent as DomainPaymentIntent,
};
use router::db::{
    errors::{ApplicationError, ApplicationResult},
    kafka_store::TenantID,
    KafkaProducer,
};

pub async fn dump_payment_intents(
    kafka_producer: &KafkaProducer,
    conn: &PgPooledConn,
    multi_progress_bar: &MultiProgress,
    tenant_id: TenantID,
    key_manager_state: &KeyManagerState,
    merchant_key_store: &MerchantKeyStore,
    batch_size: u32,
) -> ApplicationResult<()> {
    let diesel_objects_count: i64 = DieselPaymentIntent::table()
        .count()
        .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
        .get_result_async(conn)
        .await
        .change_context(ApplicationError::ConfigurationError)
        .unwrap();
    let pi_progress_bar = Arc::new(
        multi_progress_bar.add(
            indicatif::ProgressBar::new(diesel_objects_count.try_into().unwrap())
                .with_style(crate::progress_style())
                .with_message(format!("{:?} Payment Intents:", merchant_key_store.merchant_id)),
        ),
    );
    let shared_kafka = Arc::new(kafka_producer.clone());
    let shared_kms = Arc::new(key_manager_state.clone());
    let shared_mks = Arc::new(merchant_key_store.clone());
    for batch_offset in (0..diesel_objects_count).step_by(batch_size as usize) {
        let payment_intents = DieselPaymentIntent::table()
            .filter(merchant_id.eq(merchant_key_store.merchant_id.clone()))
            .limit(batch_size as i64)
            .offset(batch_offset)
            .load_async::<DieselPaymentIntent>(conn)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .unwrap();
        let batch_progress_bar = Arc::new(
            multi_progress_bar.add(
                indicatif::ProgressBar::new(batch_size as u64)
                    .with_style(crate::progress_style())
                    .with_message(format!("{:?} Payment Intents Batch:", merchant_key_store.merchant_id)),
            ),
        );
        let mut task_set: tokio::task::JoinSet<ApplicationResult<()>> = tokio::task::JoinSet::new();

        for pi in payment_intents {
            let kafkap = shared_kafka.clone();
            let kmsp = shared_kms.clone();
            let mksp = shared_mks.clone();
            let pip = pi_progress_bar.clone();
            let bpip = batch_progress_bar.clone();
            let tenant_p = tenant_id.clone();
            task_set.spawn(async move {
                pip.inc(1);
                bpip.inc(1);
                let domain_pi = DomainPaymentIntent::convert_back(
                    kmsp.as_ref(),
                    pi,
                    mksp.key.get_inner(),
                    mksp.merchant_id.clone().into(),
                )
                .await
                .change_context(ApplicationError::ConfigurationError)?;
                kafkap
                    .log_payment_intent(&domain_pi, None, tenant_p.clone())
                    .await
                    .change_context(ApplicationError::ConfigurationError)
            });
        }
        task_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<()>, _>>()?;
    }
    Ok(())
}
