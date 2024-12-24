use std::{path::PathBuf, sync::Arc};

use backfill_scripe::{
    data_transfer::{
        disputes::dump_disputes, payment_attempt::dump_payment_attempts,
        payment_intent::dump_payment_intents, refunds::dump_refunds,
    },
    encryption::fetch_raw_secrets,
    utility::parse_to_primitive_datetime,
};
use common_utils::{id_type::MerchantId, types::keymanager::KeyManagerState};
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
use storage_impl::{connection::pg_connection_read, redis::RedisStore, DatabaseStore};

use async_bb8_diesel::AsyncRunQueryDsl;
use time::PrimitiveDateTime;

#[derive(clap::Parser, Default)]
pub struct UtilityOptions {
    /// Config file.
    /// Application will look for "config/config.toml" if this option isn't specified.
    #[arg(short = 'f', long, value_name = "FILE")]
    pub config_path: Option<PathBuf>,

    #[arg(short = 't', long)]
    pub tenant_id: Option<TenantID>,

    #[arg(short = 'b', long, default_value_t = 10000)]
    pub batch_size: u32,

    #[arg(short = 'p', long, default_value_t = 5)]
    pub parallel: usize,

    #[arg(short = 'm', long)]
    pub merchant_id: Vec<String>,

    #[arg(short = 's', long, default_value_t = String::from("0000-01-01 00:00:00.0"))]
    pub start_date: String,

    #[arg(short = 'e', long, default_value_t = (&PrimitiveDateTime::MAX).to_string())]
    pub end_date: String,
}

#[tokio::main]
async fn main() -> ApplicationResult<()> {
    // get commandline config before initializing config
    let cmd_line = <UtilityOptions as clap::Parser>::parse();

    #[allow(clippy::expect_used)]
    let secret_conf = Settings::with_config_path(cmd_line.config_path)
        .attach_printable("Unable to construct application configuration")?;
    #[allow(clippy::expect_used)]
    secret_conf
        .validate()
        .attach_printable("Failed to validate router configuration")?;
    let secret_management_client = secret_conf
        .secrets_management
        .get_secret_management_client()
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to create secret management client")?;

    let conf = fetch_raw_secrets(secret_conf, &*secret_management_client)
        .await
        .change_context(ApplicationError::ConfigurationError)?;

    let _guard = router_env::setup(
        &conf.log,
        router_env::service_name!(),
        [router_env::service_name!()],
    )
    .change_context(ApplicationError::ConfigurationError)?;

    logger::info!("Application started [{:?}] [{:?}]", conf.server, conf.log);

    // Spawn a thread for collecting metrics at fixed intervals
    metrics::bg_metrics_collector::spawn_metrics_collector(
        conf.log.telemetry.bg_metrics_collection_interval_in_secs,
    );

    // #[allow(clippy::expect_used)]
    // let server = Box::pin(router::start_server(conf))
    //     .await
    //     .attach_printable("Failed to create the server")?;
    // let _ = server.await;

    #[allow(clippy::expect_used)]
    // let tenant_config = conf.multitenancy.get_tenant(&cmd_line.tenant_id).attach_printable("tenant not found")?;
    #[allow(clippy::expect_used)]
    let _encryption_client = conf
        .encryption_management
        .get_encryption_management_client()
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to create encryption client")?;

    let cache_store = Arc::new(
        RedisStore::new(&conf.redis)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to create cache store")?,
    );

    let tenant;
    #[allow(clippy::expect_used)]
    let pq_store = if let Some(id_type) = cmd_line.tenant_id {
        println!("Tenant ID: {:?}", tenant_id);
        let tenant_config = conf
            .multitenancy
            .get_tenant(&tenant_id)
            .ok_or(ApplicationError::ConfigurationError)
            .attach_printable("tenant not found")?;
        println!("Tenant Config: {:?}", tenant_config);
        tenant = TenantID(tenant_config.clickhouse_database.clone());
        get_store(&conf, tenant_config, Arc::clone(&cache_store), false)
            .await
            .change_context(ApplicationError::ConfigurationError)
            .attach_printable("Failed to create store")?
    } else {
        tenant = TenantID(conf.multitenancy.global_tenant.clickhouse_database.clone());
        get_store(
            &conf,
            &conf.multitenancy.global_tenant,
            Arc::clone(&cache_store),
            false,
        )
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to create store")?
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
        #[cfg(feature = "release")]
        ca: kmc.ca.clone(),
        #[cfg(feature = "release")]
        cert: kmc.cert.clone(),
    };
    let pg_connection = pg_connection_read(&pq_store)
        .await
        .change_context(ApplicationError::ConfigurationError)
        .attach_printable("Failed to get pg connection")?;
    let multi_progress_bar = indicatif::MultiProgress::new();
    let batch_size = cmd_line.batch_size;
    println!("{},  {}", &cmd_line.start_date, &cmd_line.end_date);
    let start_date = parse_to_primitive_datetime(&cmd_line.start_date)
        .attach_printable("Failed to parse start_date")?;
    let end_date = parse_to_primitive_datetime(&cmd_line.end_date)
        .attach_printable("Failed to parse end_date")?;
    let merchant_stores_count = if cmd_line.merchant_id.is_empty() {
        get_merchant_stores(&pg_connection).await?
    } else {
        cmd_line.merchant_id.len() as i64
    };

    let merchant_progress_bar = multi_progress_bar.add(
        ProgressBar::new(merchant_stores_count as u64)
            .with_style(backfill_scripe::progress_style()?)
            .with_message("Merchants:"),
    );
    let replica_pool = pq_store.get_replica_pool().to_owned();
    for batch_offset in (0..merchant_stores_count as u32).step_by(batch_size as usize) {
        let merchant_stores = if !cmd_line.merchant_id.is_empty() {
            pq_store
                .list_multiple_key_stores(
                    &kms,
                    cmd_line
                        .merchant_id
                        .iter()
                        .map(|s| {
                            MerchantId::wrap(s.clone())
                                .attach_printable("failed to parse merchant id")
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .change_context(ApplicationError::ConfigurationError)?,
                    &Secret::new(pq_store.get_master_key().to_vec()),
                )
                .await
        } else {
            pq_store
                .get_all_key_stores(
                    &kms,
                    &Secret::new(pq_store.get_master_key().to_vec()),
                    batch_offset,
                    batch_offset + batch_size,
                )
                .await
        }
        .change_context(ApplicationError::ConfigurationError)?;

        for merchant_stores_batch in merchant_stores.chunks(cmd_line.parallel) {
            let results = merchant_stores_batch
                .iter()
                .cloned()
                .map(|mks| {
                    let pool = replica_pool.clone();
                    let mp_bar = merchant_progress_bar.clone();
                    let tenant_int = tenant.clone();
                    let kafka_producer_int = kafka_producer.clone();
                    let multi_progress_bar_int = multi_progress_bar.clone();
                    let kms_int = kms.clone();
                    (
                        mks,
                        pool,
                        mp_bar,
                        tenant_int,
                        kafka_producer_int,
                        multi_progress_bar_int,
                        kms_int,
                    )
                })
                .map(
                    |(
                        mks,
                        pool,
                        mp_bar,
                        tenant_int,
                        kafka_producer_int,
                        multi_progress_bar_int,
                        kms_int,
                    )| {
                        tokio::spawn(async move {
                            mp_bar.inc(1);
                            let pg_connection = pool
                                .get()
                                .await
                                .change_context(ApplicationError::ConfigurationError)?;

                            dump_payment_intents(
                                &kafka_producer_int,
                                &pg_connection,
                                &multi_progress_bar_int,
                                tenant_int.clone(),
                                &kms_int,
                                &mks,
                                batch_size,
                                start_date,
                                end_date,
                            )
                            .await?;
                            dump_payment_attempts(
                                &kafka_producer_int,
                                &pg_connection,
                                &multi_progress_bar_int,
                                tenant_int.clone(),
                                &mks,
                                batch_size,
                                start_date,
                                end_date,
                            )
                            .await?;
                            dump_refunds(
                                &kafka_producer_int,
                                &pg_connection,
                                &multi_progress_bar_int,
                                tenant_int.clone(),
                                &kms_int,
                                &mks,
                                batch_size,
                                start_date,
                                end_date,
                            )
                            .await?;
                            dump_disputes(
                                &kafka_producer_int,
                                &pg_connection,
                                &multi_progress_bar_int,
                                tenant_int.clone(),
                                &kms_int,
                                &mks,
                                batch_size,
                                start_date,
                                end_date,
                            )
                            .await?;
                            Ok(())
                        })
                    },
                )
                .collect::<Vec<tokio::task::JoinHandle<ApplicationResult<()>>>>();
            for res in futures::future::join_all(results).await {
                res.change_context(ApplicationError::ConfigurationError)?
                    .change_context(ApplicationError::ConfigurationError)?;
            }
        }
    }
    logger::debug!("Flushing Kafka Producer");
    let flush_progress = multi_progress_bar.add(ProgressBar::new_spinner());
    flush_progress.set_message("Flushing Kafka Producer");
    kafka_producer.flush_kafka().change_context(ApplicationError::ConfigurationError)?;
    flush_progress.finish_and_clear();

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
