use autoschematic_core::connector::ResourceAddress;
use autoschematic_core::glob::addr_matches_filter;
use std::path::{Path, PathBuf};

use crate::{KafkaConnector, addr::KafkaResourceAddress, client::get_operation_timeout};

impl KafkaConnector {
    pub async fn do_list(&self, subpath: &Path) -> anyhow::Result<Vec<PathBuf>> {
        let mut results = Vec::new();
        let clients = self.clients.read().await;
        let config = self.config.read().await;
        let timeout = get_operation_timeout(config.operation_timeout_ms);

        for (cluster_name, client) in clients.iter() {
            let cluster_path = PathBuf::from(format!("kafka/{}", cluster_name));

            if !addr_matches_filter(&cluster_path, subpath) {
                continue;
            }

            // List topics
            match client.inner().fetch_metadata(None, timeout) {
                Ok(metadata) => {
                    for topic in metadata.topics() {
                        let topic_name = topic.name();
                        // Skip internal Kafka topics
                        if topic_name.starts_with("__") {
                            continue;
                        }

                        let addr = KafkaResourceAddress::Topic {
                            cluster: cluster_name.clone(),
                            topic: topic_name.to_string(),
                        };
                        results.push(addr.to_path_buf());
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch metadata for cluster '{}': {}", cluster_name, e);
                }
            }
            
            // TODO list ACLs and Quotas when we have support
        }

        Ok(results)
    }
}
