use crate::config::{KafkaAuth, KafkaClusterConfig};
use anyhow::Context;
use rdkafka_autoschematic_fork as rdkafka;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use std::time::Duration;

pub type KafkaAdminClient = AdminClient<DefaultClientContext>;

/// Create a Kafka admin client from cluster configuration
pub fn create_admin_client(cluster_config: &KafkaClusterConfig) -> anyhow::Result<KafkaAdminClient> {
    let mut config = ClientConfig::new();

    config.set("bootstrap.servers", &cluster_config.bootstrap_servers);

    // Set authentication based on config
    match &cluster_config.auth {
        KafkaAuth::None => {
            // No additional auth config needed
        }
        KafkaAuth::SaslPlain { username, password } => {
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanism", "PLAIN");
            config.set("sasl.username", username);
            config.set("sasl.password", password);
        }
        KafkaAuth::SaslScramSha256 { username, password } => {
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanism", "SCRAM-SHA-256");
            config.set("sasl.username", username);
            config.set("sasl.password", password);
        }
        KafkaAuth::SaslScramSha512 { username, password } => {
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanism", "SCRAM-SHA-512");
            config.set("sasl.username", username);
            config.set("sasl.password", password);
        }
        KafkaAuth::SaslGssapi { principal, keytab_path } => {
            config.set("security.protocol", "SASL_PLAINTEXT");
            config.set("sasl.mechanism", "GSSAPI");
            config.set("sasl.kerberos.principal", principal);
            if let Some(keytab) = keytab_path {
                config.set("sasl.kerberos.keytab", keytab);
            }
        }
    }

    // Set TLS configuration if provided
    if let Some(tls) = &cluster_config.tls {
        // Update security protocol to use SSL
        let current_protocol = config.get("security.protocol").unwrap_or("PLAINTEXT");
        let ssl_protocol = if current_protocol.contains("SASL") {
            "SASL_SSL"
        } else {
            "SSL"
        };
        config.set("security.protocol", ssl_protocol);

        if let Some(ca_cert) = &tls.ca_cert_path {
            config.set("ssl.ca.location", ca_cert);
        }

        if let Some(client_cert) = &tls.client_cert_path {
            config.set("ssl.certificate.location", client_cert);
        }

        if let Some(client_key) = &tls.client_key_path {
            config.set("ssl.key.location", client_key);
        }

        if !tls.verify_certificate {
            config.set("enable.ssl.certificate.verification", "false");
        }
    }

    // Apply additional custom configuration
    for (key, value) in &cluster_config.additional_config {
        config.set(key, value);
    }

    // Create admin client
    config.create().context("Failed to create Kafka admin client")
}

pub fn get_operation_timeout(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms)
}
