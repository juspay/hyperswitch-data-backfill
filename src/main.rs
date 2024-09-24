use std::{path::PathBuf, sync::Arc};

use backfill_scripe::{
    data_transfer::{
        disputes::dump_disputes, payment_attempt::dump_payment_attempts,
        payment_intent::dump_payment_intents, refunds::dump_refunds,
    },
    encryption::fetch_raw_secrets,
};
use common_utils::types::keymanager::KeyManagerState;
use diesel::{associations::HasTable, QueryDsl};
use error_stack::ResultExt;
use indicatif::ProgressBar;
use router::{
    configs::settings::Settings,
    connection::PgPooledConn,
    core::errors::{ApplicationError, ApplicationResult},
    db::{
        kafka_store::TenantID, merchant_key_store::MerchantKeyStoreInterface, KafkaProducer,
        MasterKeyInterface,
    },
    logger,
    pii::Secret,
    routes::metrics,
    services::get_store,
};
use storage_impl::{connection::pg_connection_read, redis::RedisStore};

use async_bb8_diesel::AsyncRunQueryDsl;

#[derive(clap::Parser, Default)]
pub struct UtilityOptions {
    /// Config file.
    /// Application will look for "config/config.toml" if this option isn't specified.
    #[arg(short = 'f', long, value_name = "FILE")]
    pub config_path: Option<PathBuf>,

    #[arg(short = 't', long)]
    pub tenant_id: Option<String>,

    #[arg(short = 'b', long, default_value_t = 1000)]
    pub batch_size: u32
}

#[tokio::main]
async fn main() -> ApplicationResult<()> {
    // get commandline config before initializing config
    let cmd_line = <UtilityOptions as clap::Parser>::parse();

    #[allow(clippy::expect_used)]
    let secret_conf = Settings::with_config_path(cmd_line.config_path)
        .expect("Unable to construct application configuration");
    #[allow(clippy::expect_used)]
    secret_conf
        .validate()
        .expect("Failed to validate router configuration");
    let secret_management_client = secret_conf
        .secrets_management
        .get_secret_management_client()
        .await
        .expect("Failed to create secret management client");

    let conf = fetch_raw_secrets(secret_conf, &*secret_management_client).await;

    let _guard = router_env::setup(
        &conf.log,
        router_env::service_name!(),
        [router_env::service_name!()],
    )
    .change_context(ApplicationError::ConfigurationError)?;

    logger::info!("Application started [{:?}] [{:?}]", conf.server, conf.log);

    // Spawn a thread for collecting metrics at fixed intervals
    metrics::bg_metrics_collector::spawn_metrics_collector(
        &conf.log.telemetry.bg_metrics_collection_interval_in_secs,
    );

    // #[allow(clippy::expect_used)]
    // let server = Box::pin(router::start_server(conf))
    //     .await
    //     .expect("Failed to create the server");
    // let _ = server.await;

    #[allow(clippy::expect_used)]
    // let tenant_config = conf.multitenancy.get_tenant(&cmd_line.tenant_id).expect("tenant not found");
    #[allow(clippy::expect_used)]
    let _encryption_client = conf
        .encryption_management
        .get_encryption_management_client()
        .await
        .expect("Failed to create encryption client");

    let cache_store = Arc::new(
        RedisStore::new(&conf.redis)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to create cache store")?,
    );

    let tenant;
    #[allow(clippy::expect_used)]
    let pq_store = if let Some(tenant_id) = cmd_line.tenant_id {
        println!("Tenant ID: {:?}", tenant_id);
        let tenant_config = conf
            .multitenancy
            .get_tenant(&tenant_id)
            .expect("tenant not found");
        println!("Tenant Config: {:?}", tenant_config);
        tenant = TenantID(tenant_config.clickhouse_database.clone());
        get_store(&conf, tenant_config, Arc::clone(&cache_store), false)
            .await
            .expect("Failed to create store")
    } else {
        tenant = TenantID(conf.multitenancy.global_tenant.clickhouse_database.clone());
        get_store(
            &conf,
            &conf.multitenancy.global_tenant,
            Arc::clone(&cache_store),
            false,
        )
        .await
        .expect("Failed to create store")
    };
    let kafka_producer = match conf.events {
        router::events::EventsConfig::Kafka { kafka } => Ok(KafkaProducer::create(&kafka)
            .await
            .change_context(ApplicationError::ConfigurationError)?),
        router::events::EventsConfig::Logs => {
            Err(ApplicationError::ConfigurationError).attach_printable("Kafka is not enabled")
        }
    }?;

    let kmc = conf.key_manager.get_inner();

    let kms = KeyManagerState {
        client_idle_timeout: None,
        enabled: kmc.enabled,
        url: kmc.url.clone(),
        request_id: None,
    };
    let pg_connection = pg_connection_read(&pq_store).await.unwrap();
    let multi_progress_bar = indicatif::MultiProgress::new();
    let batch_size = cmd_line.batch_size;
    let merchant_stores_count = get_merchant_stores(&pg_connection).await?;

    let merchant_progress_bar = multi_progress_bar.add(
        ProgressBar::new(merchant_stores_count as u64)
            .with_style(backfill_scripe::progress_style())
            .with_message("Merchants:"),
    );
    for batch_offset in (0..merchant_stores_count as u32).step_by(batch_size as usize) {
        let merchant_stores = pq_store
            .get_all_key_stores(
                &kms,
                &Secret::new(pq_store.get_master_key().to_vec()),
                batch_offset,
                batch_offset + batch_size,
            )
            .await
            .change_context(ApplicationError::ConfigurationError)?;
        for mks in merchant_stores {
            merchant_progress_bar.inc(1);
            merchant_progress_bar
                .set_message(format!("Merchant: {}", mks.merchant_id.get_string_repr()));

            dump_payment_attempts(
                &kafka_producer,
                &pg_connection,
                &multi_progress_bar,
                tenant.clone(),
                &mks,
                batch_size,
            )
            .await?;
            dump_payment_intents(
                &kafka_producer,
                &pg_connection,
                &multi_progress_bar,
                tenant.clone(),
                &kms,
                &mks,
                batch_size,
            )
            .await?;
            dump_refunds(
                &kafka_producer,
                &pg_connection,
                &multi_progress_bar,
                tenant.clone(),
                &kms,
                &mks,
                batch_size,
            )
            .await?;
            dump_disputes(
                &kafka_producer,
                &pg_connection,
                &multi_progress_bar,
                tenant.clone(),
                &kms,
                &mks,
                batch_size,
            )
            .await?;
        }
    }

    // Get Payment Counts from Payment Table
    // For each payment_batch
    // For each pament

    Ok(())
}

async fn get_merchant_stores(conn: &PgPooledConn) -> ApplicationResult<i64> {
    diesel_models::merchant_key_store::MerchantKeyStore::table()
        .count()
        .get_result_async(conn)
        .await
        .change_context(ApplicationError::ConfigurationError)
}
