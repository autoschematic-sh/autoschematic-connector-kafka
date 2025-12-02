use crate::{KafkaConnector, addr::KafkaResourceAddress, client::get_operation_timeout, resource};
use anyhow::{Context, anyhow, bail};
use autoschematic_core::{
    connector::{GetResourceResponse, Resource, ResourceAddress},
    get_resource_response,
};
use indexmap::IndexMap;
use rdkafka::admin::AdminOptions;
use rdkafka_autoschematic_fork as rdkafka;
use std::collections::HashMap;
use std::path::Path;

impl KafkaConnector {
    pub async fn do_get(&self, addr: &Path) -> anyhow::Result<Option<GetResourceResponse>> {
        let addr = KafkaResourceAddress::from_path(addr)?;

        match addr {
            KafkaResourceAddress::Config => Ok(None),
            KafkaResourceAddress::Topic { cluster, topic } => {
                let clients = self.clients.read().await;
                let config = self.config.read().await;
                let timeout = get_operation_timeout(config.operation_timeout_ms);

                let topic_specifier = rdkafka::admin::ResourceSpecifier::Topic(&topic);
                let opts = AdminOptions::new().operation_timeout(Some(timeout));

                let client = clients
                    .get(&cluster)
                    .ok_or_else(|| anyhow!("Cluster '{}' not found in configuration", cluster))?;

                let config = client.describe_configs([&topic_specifier], &opts).await?;

                let Some(config) = config.first() else { return Ok(None) };
                // Fetch topic metadata
                match client.inner().fetch_metadata(Some(&topic), timeout) {
                    Ok(metadata) => {
                        if let Some(topic_metadata) = metadata.topics().iter().find(|t| t.name() == topic) {
                            if topic_metadata.error().is_some() {
                                return Ok(None);
                            }
                            // Get partition count
                            let partitions = topic_metadata.partitions().len() as i32;

                            // Get replication factor from first partition
                            let replication_factor = topic_metadata
                                .partitions()
                                .first()
                                .map(|p| p.replicas().len() as i16)
                                .unwrap_or(1);

                            match config {
                                Ok(config) => {
                                    let mut config_map = IndexMap::new();

                                    let mut entries = config.entries.clone();

                                    entries.sort_by(|a, b| a.name.cmp(&b.name));

                                    for entry in entries {
                                        if entry.is_read_only {
                                            continue;
                                        }
                                        if entry.is_sensitive {
                                            continue;
                                        }
                                        if let Some(ref value) = entry.value {
                                            config_map.insert(entry.name.to_owned(), value.to_owned());
                                        }
                                    }

                                    // from_iter(config.entries.iter().map(|e| (e.name, e.value)));

                                    let topic_resource = resource::KafkaTopic {
                                        partitions,
                                        replication_factor,
                                        config: config_map,
                                    };

                                    get_resource_response!(resource::KafkaResource::Topic(topic_resource))
                                }
                                Err(e) => {
                                    bail!("failed to describe_configs for topic {}: {:?}", topic, e);
                                }
                            }
                        } else {
                            Ok(None) // Topic doesn't exist
                        }
                    }
                    Err(e) => {
                        tracing::debug!("{e:?}");
                        Ok(None)
                    }
                }
            }
            KafkaResourceAddress::Acl { cluster, acl_id } => {
                // TODO: Implement ACL fetching when rdkafka supports it
                // For now, return None
                tracing::warn!("ACL fetching not yet implemented for cluster '{}', ACL '{}'", cluster, acl_id);
                Ok(None)
            }
            KafkaResourceAddress::Quota { cluster, quota_id } => {
                // TODO: Implement quota fetching when rdkafka supports it
                // For now, return None
                tracing::warn!(
                    "Quota fetching not yet implemented for cluster '{}', quota '{}'",
                    cluster,
                    quota_id
                );
                Ok(None)
            }
        }
    }
}
