use crate::{KafkaConnector, addr::KafkaResourceAddress, client::get_operation_timeout, op::KafkaConnectorOp};
use anyhow::{anyhow, bail};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecResponse, ResourceAddress},
    error_util::invalid_op,
};
use rdkafka_autoschematic_fork as rdkafka;
use rdkafka::admin::{AdminOptions, AlterConfig, NewTopic};
use std::{collections::HashMap, path::Path};

impl KafkaConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> anyhow::Result<OpExecResponse> {
        let addr = KafkaResourceAddress::from_path(addr)?;
        let op = KafkaConnectorOp::from_str(op)?;

        match &addr {
            KafkaResourceAddress::Config => Err(invalid_op(&addr, &op)),
            KafkaResourceAddress::Task { .. } => Err(invalid_op(&addr, &op)),
            KafkaResourceAddress::Topic { cluster, topic } => {
                let clients = self.clients.read().await;
                let config = self.config.read().await;
                let timeout = get_operation_timeout(config.operation_timeout_ms);

                let opts = AdminOptions::new().operation_timeout(Some(timeout));

                let client = clients
                    .get(cluster)
                    .ok_or_else(|| anyhow!("Cluster '{}' not found", cluster))?;

                match op {
                    KafkaConnectorOp::CreateTopic(topic_config) => {
                        let new_topic = NewTopic::new(
                            topic,
                            topic_config.partitions,
                            rdkafka::admin::TopicReplication::Fixed(topic_config.replication_factor as i32),
                        );

                        // Apply topic configurations
                        let new_topic = topic_config
                            .config
                            .iter()
                            .fold(new_topic, |nt, (key, value)| nt.set(key, value));

                        match client.create_topics(&[new_topic], &opts).await {
                            Ok(results) => {
                                if results.is_empty() {
                                    bail!("No result returned from create_topics");
                                }

                                match &results[0] {
                                    Ok(_topic_name) => Ok(OpExecResponse {
                                        outputs: None,
                                        friendly_message: Some(format!("Created topic '{}' in cluster '{}'", topic, cluster)),
                                    }),
                                    Err((topic_name, err)) => {
                                        bail!("Failed to create topic '{}': {:?}", topic_name, err)
                                    }
                                }
                            }
                            Err(e) => bail!("Failed to create topic '{}': {:?}", topic, e),
                        }
                    }
                    KafkaConnectorOp::UpdateTopicPartitions { partitions } => {
                        // Use create_partitions to increase partition count
                        use rdkafka::admin::NewPartitions;

                        let new_partitions = NewPartitions::new(topic, partitions as usize);

                        match client.create_partitions(&[new_partitions], &opts).await {
                            Ok(results) => {
                                if results.is_empty() {
                                    bail!("No result returned from create_partitions");
                                }

                                match &results[0] {
                                    Ok(_topic_name) => Ok(OpExecResponse {
                                        outputs: None,
                                        friendly_message: Some(format!(
                                            "Increased partitions for topic '{}' to {} in cluster '{}'",
                                            topic, partitions, cluster
                                        )),
                                    }),
                                    Err((topic_name, err)) => {
                                        bail!("Failed to update partitions for topic '{}': {:?}", topic_name, err)
                                    }
                                }
                            }
                            Err(e) => bail!("Failed to update partitions for topic '{}': {:?}", topic, e),
                        }
                    }
                    KafkaConnectorOp::UpdateTopicConfig { config: topic_config } => {
                        let topic_config = HashMap::from_iter(topic_config.iter().map(|(k, v)| (k.as_str(), v.as_str())));

                        let alter_config = AlterConfig {
                            specifier: rdkafka::admin::ResourceSpecifier::Topic(&topic),
                            entries: topic_config,
                        };

                        match client.alter_configs(&[alter_config], &opts).await {
                            Ok(results) => {
                                if results.is_empty() {
                                    bail!("No result returned from alter_configs");
                                }

                                match &results[0] {
                                    Ok(_topic_name) => Ok(OpExecResponse {
                                        outputs: None,
                                        friendly_message: Some(format!(
                                            "Altered config for topic '{}' in cluster '{}'",
                                            topic, cluster
                                        )),
                                    }),
                                    Err((_topic_name, err)) => {
                                        bail!("Failed to update partitions for topic '{}': {:?}", topic, err)
                                    }
                                }
                            }
                            Err(e) => {
                                bail!("Failed to alter config for topic '{}': {:?}", topic, e)
                            }
                        }

                        // Ok(OpExecResponse {
                        //     outputs: None,
                        //     friendly_message: Some(format!(
                        //         "Topic config update requested for '{}' (not yet implemented)",
                        //         topic
                        //     )),
                        // })
                    }
                    KafkaConnectorOp::DeleteTopic => match client.delete_topics(&[topic], &opts).await {
                        Ok(results) => {
                            if results.is_empty() {
                                bail!("No result returned from delete_topics");
                            }

                            match &results[0] {
                                Ok(_topic_name) => Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!("Deleted topic '{}' from cluster '{}'", topic, cluster)),
                                }),
                                Err((topic_name, err)) => {
                                    bail!("Failed to delete topic '{}': {:?}", topic_name, err)
                                }
                            }
                        }
                        Err(e) => bail!("Failed to delete topic '{}': {:?}", topic, e),
                    },
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            KafkaResourceAddress::Acl { cluster, acl_id } => {
                // TODO: Implement ACL operations when rdkafka supports them
                // For production use, this would require using the Kafka Admin API directly
                tracing::warn!(
                    "ACL operations not yet implemented for cluster '{}', ACL '{}'",
                    cluster,
                    acl_id
                );
                Ok(OpExecResponse {
                    outputs: None,
                    friendly_message: Some("ACL operation not yet implemented".to_string()),
                })
            }
            KafkaResourceAddress::Quota { cluster, quota_id } => {
                // TODO: Implement quota operations when rdkafka supports them
                // For production use, this would require using the Kafka Admin API directly
                tracing::warn!(
                    "Quota operations not yet implemented for cluster '{}', quota '{}'",
                    cluster,
                    quota_id
                );
                Ok(OpExecResponse {
                    outputs: None,
                    friendly_message: Some("Quota operation not yet implemented".to_string()),
                })
            }
        }
    }
}
