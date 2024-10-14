use error_stack::ResultExt;
use hyperswitch_interfaces::secrets_interface::{
    secret_handler::SecretsHandler,
    secret_state::{RawSecret, SecuredSecret},
    SecretManagementInterface, SecretsManagementError,
};
use router::configs::settings::{self, Settings};
/// # Panics
///
/// Will panic even if kms decryption fails for at least one field
pub async fn fetch_raw_secrets(
    conf: Settings<SecuredSecret>,
    secret_management_client: &dyn SecretManagementInterface,
) -> error_stack::Result<Settings<RawSecret>, SecretsManagementError> {
    #[allow(clippy::expect_used)]
    let master_database =
        settings::Database::convert_to_raw_secret(conf.master_database, secret_management_client)
            .await
            .attach_printable("Failed to decrypt master database configuration")?;

    #[allow(clippy::expect_used)]
    let analytics = router::analytics::AnalyticsConfig::convert_to_raw_secret(
        conf.analytics,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt analytics configuration")?;

    #[allow(clippy::expect_used)]
    let replica_database =
        settings::Database::convert_to_raw_secret(conf.replica_database, secret_management_client)
            .await
            .attach_printable("Failed to decrypt replica database configuration")?;

    #[allow(clippy::expect_used)]
    let secrets = settings::Secrets::convert_to_raw_secret(conf.secrets, secret_management_client)
        .await
        .attach_printable("Failed to decrypt secrets")?;

    #[allow(clippy::expect_used)]
    let forex_api =
        settings::ForexApi::convert_to_raw_secret(conf.forex_api, secret_management_client)
            .await
            .attach_printable("Failed to decrypt forex api configs")?;

    #[allow(clippy::expect_used)]
    let jwekey = settings::Jwekey::convert_to_raw_secret(conf.jwekey, secret_management_client)
        .await
        .attach_printable("Failed to decrypt jwekey configs")?;

    #[allow(clippy::expect_used)]
    let api_keys =
        settings::ApiKeys::convert_to_raw_secret(conf.api_keys, secret_management_client)
            .await
            .attach_printable("Failed to decrypt api_keys configs")?;

    #[allow(clippy::expect_used)]
    let connector_onboarding = settings::ConnectorOnboarding::convert_to_raw_secret(
        conf.connector_onboarding,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt connector_onboarding configs")?;

    #[allow(clippy::expect_used)]
    let applepay_decrypt_keys = settings::ApplePayDecryptConfig::convert_to_raw_secret(
        conf.applepay_decrypt_keys,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt applepay decrypt configs")?;

    #[allow(clippy::expect_used)]
    let applepay_merchant_configs = settings::ApplepayMerchantConfigs::convert_to_raw_secret(
        conf.applepay_merchant_configs,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt applepay merchant configs")?;

    #[allow(clippy::expect_used)]
    let payment_method_auth = settings::PaymentMethodAuth::convert_to_raw_secret(
        conf.payment_method_auth,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt payment method auth configs")?;

    #[allow(clippy::expect_used)]
    let key_manager = settings::KeyManagerConfig::convert_to_raw_secret(
        conf.key_manager,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt keymanager configs")?;

    #[allow(clippy::expect_used)]
    let user_auth_methods = settings::UserAuthMethodSettings::convert_to_raw_secret(
        conf.user_auth_methods,
        secret_management_client,
    )
    .await
    .attach_printable("Failed to decrypt user_auth_methods configs")?;

    #[allow(clippy::expect_used)]
    let network_tokenization_service = match conf.network_tokenization_service {
        Some(network_tokenization_service) => Some(
            settings::NetworkTokenizationService::convert_to_raw_secret(
                network_tokenization_service,
                secret_management_client,
            )
            .await
            .attach_printable("Failed to decrypt network tokenization service configs")?,
        ),
        None => None,
    };

    Ok(Settings {
        server: conf.server,
        master_database,
        redis: conf.redis,
        log: conf.log,
        drainer: conf.drainer,
        encryption_management: conf.encryption_management,
        secrets_management: conf.secrets_management,
        proxy: conf.proxy,
        env: conf.env,
        key_manager,
        replica_database,
        secrets,
        locker: conf.locker,
        connectors: conf.connectors,
        forex_api,
        refund: conf.refund,
        eph_key: conf.eph_key,
        scheduler: conf.scheduler,
        jwekey,
        webhooks: conf.webhooks,
        pm_filters: conf.pm_filters,
        payout_method_filters: conf.payout_method_filters,
        bank_config: conf.bank_config,
        api_keys,
        file_storage: conf.file_storage,
        tokenization: conf.tokenization,
        connector_customer: conf.connector_customer,
        dummy_connector: conf.dummy_connector,
        user: conf.user,
        mandates: conf.mandates,
        network_transaction_id_supported_connectors: conf
            .network_transaction_id_supported_connectors,
        required_fields: conf.required_fields,
        delayed_session_response: conf.delayed_session_response,
        webhook_source_verification_call: conf.webhook_source_verification_call,
        payment_method_auth,
        connector_request_reference_id_config: conf.connector_request_reference_id_config,
        payouts: conf.payouts,
        applepay_decrypt_keys,
        multiple_api_version_supported_connectors: conf.multiple_api_version_supported_connectors,
        applepay_merchant_configs,
        lock_settings: conf.lock_settings,
        temp_locker_enable_config: conf.temp_locker_enable_config,
        generic_link: conf.generic_link,
        payment_link: conf.payment_link,
        analytics,
        opensearch: conf.opensearch,
        kv_config: conf.kv_config,
        frm: conf.frm,
        #[cfg(feature = "release")]
        email: conf.email,
        report_download_config: conf.report_download_config,
        events: conf.events,
        connector_onboarding,
        cors: conf.cors,
        unmasked_headers: conf.unmasked_headers,
        saved_payment_methods: conf.saved_payment_methods,
        multitenancy: conf.multitenancy,
        user_auth_methods,
        decision: conf.decision,
        locker_based_open_banking_connectors: conf.locker_based_open_banking_connectors,
        grpc_client: conf.grpc_client,
        network_tokenization_supported_card_networks: conf
            .network_tokenization_supported_card_networks,
        network_tokenization_service,
        network_tokenization_supported_connectors: conf.network_tokenization_supported_connectors,
    })
}
