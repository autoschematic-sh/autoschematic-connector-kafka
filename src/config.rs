use autoschematic_core::{connector::Resource, macros::FieldTypes, util::PrettyConfig, util::RON};
use autoschematic_macros::FieldTypes;
use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Documented, DocumentedFields)]
#[serde(deny_unknown_fields)]
/// Authentication mechanism for Kafka cluster connection
pub enum KafkaAuth {
    /// No authentication (plain connection)
    None,
    /// SASL/PLAIN authentication with username and password
    SaslPlain { username: String, password: String },
    /// SASL/SCRAM-SHA-256 authentication
    SaslScramSha256 { username: String, password: String },
    /// SASL/SCRAM-SHA-512 authentication
    SaslScramSha512 { username: String, password: String },
    /// Kerberos/GSSAPI authentication
    SaslGssapi { principal: String, keytab_path: Option<String> },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// TLS/SSL configuration for secure Kafka connections
pub struct KafkaTlsConfig {
    /// Path to CA certificate file for verifying broker certificates
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file for mutual TLS authentication
    pub client_cert_path: Option<String>,
    /// Path to client private key file for mutual TLS authentication
    pub client_key_path: Option<String>,
    /// Whether to verify the broker's SSL certificate (default: true)
    pub verify_certificate: bool,
}

impl Default for KafkaTlsConfig {
    fn default() -> Self {
        Self {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            verify_certificate: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// Configuration for a single Kafka cluster
pub struct KafkaClusterConfig {
    /// Comma-separated list of Kafka broker addresses (host:port)
    pub bootstrap_servers: String,
    /// Authentication configuration for this cluster
    pub auth: KafkaAuth,
    /// TLS/SSL configuration (optional)
    pub tls: Option<KafkaTlsConfig>,
    /// Additional client configuration properties as key-value pairs
    pub additional_config: HashMap<String, String>,
}

impl Default for KafkaClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from("localhost:9092"),
            auth: KafkaAuth::None,
            tls: None,
            additional_config: HashMap::new(),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Documented, DocumentedFields, Clone, FieldTypes)]
#[serde(deny_unknown_fields)]
/// The primary configuration block for the KafkaConnector
pub struct KafkaConnectorConfig {
    /// Map of cluster names to their connection configurations
    pub clusters: HashMap<String, KafkaClusterConfig>,
    /// Timeout in milliseconds for Kafka admin operations (default: 30000)
    pub operation_timeout_ms: u64,
    /// Maximum number of concurrent requests to Kafka clusters (default: 10)
    pub concurrent_requests: usize,
}

impl Default for KafkaConnectorConfig {
    fn default() -> Self {
        let mut clusters = HashMap::new();
        clusters.insert(String::from("default"), KafkaClusterConfig::default());

        Self {
            clusters,
            operation_timeout_ms: 30000,
            concurrent_requests: 10,
        }
    }
}

impl KafkaConnectorConfig {
    pub fn try_load(prefix: &Path) -> anyhow::Result<Option<Self>> {
        let config_path = prefix.join("kafka").join("config.ron");

        if !config_path.exists() {
            return Ok(None);
        }

        let config_str = std::fs::read_to_string(&config_path)?;
        let config: KafkaConnectorConfig = RON.from_str(&config_str)?;
        Ok(Some(config))
    }
}

impl Resource for KafkaConnectorConfig {
    fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(RON.to_string_pretty(self, PrettyConfig::default())?.into())
    }

    fn from_bytes(_addr: &impl autoschematic_core::connector::ResourceAddress, s: &[u8]) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(RON.from_str(std::str::from_utf8(s)?)?)
    }
}
