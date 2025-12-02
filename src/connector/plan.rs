use crate::{KafkaConnector, addr::KafkaResourceAddress, op::KafkaConnectorOp, resource};
use anyhow::Context;
use autoschematic_core::{
    connector::{ConnectorOp, PlanResponseElement, Resource, ResourceAddress},
    connector_op,
};
use std::path::Path;

impl KafkaConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> anyhow::Result<Vec<PlanResponseElement>> {
        let addr = KafkaResourceAddress::from_path(addr)?;

        match addr {
            KafkaResourceAddress::Topic { .. } => self.plan_topic(addr, current, desired).await,
            KafkaResourceAddress::Acl { .. } => self.plan_acl(addr, current, desired).await,
            KafkaResourceAddress::Quota { .. } => self.plan_quota(addr, current, desired).await,
            KafkaResourceAddress::Config => Ok(vec![]),
            KafkaResourceAddress::Task { .. } => Ok(vec![]),
        }
    }

    async fn plan_topic(
        &self,
        addr: KafkaResourceAddress,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> anyhow::Result<Vec<PlanResponseElement>> {
        let mut ops = Vec::new();

        match (current, desired) {
            (None, None) => {} // Nothing to do
            (None, Some(desired_bytes)) => {
                // Create new topic
                let desired_topic: resource::KafkaTopic = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired topic")?
                    .into();

                ops.push(connector_op!(
                    KafkaConnectorOp::CreateTopic(desired_topic.clone()),
                    format!(
                        "Create topic with {} partitions and replication factor {}",
                        desired_topic.partitions, desired_topic.replication_factor
                    )
                ));
            }
            (Some(_), None) => {
                // Delete topic
                ops.push(connector_op!(KafkaConnectorOp::DeleteTopic, "Delete topic".to_string()));
            }
            (Some(current_bytes), Some(desired_bytes)) => {
                // Update topic
                let current_topic: resource::KafkaTopic = resource::KafkaResource::from_bytes(&addr, &current_bytes)
                    .context("Failed to parse current topic")?
                    .into();

                let desired_topic: resource::KafkaTopic = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired topic")?
                    .into();

                // Check if partitions changed (can only increase)
                if desired_topic.partitions > current_topic.partitions {
                    ops.push(connector_op!(
                        KafkaConnectorOp::UpdateTopicPartitions {
                            partitions: desired_topic.partitions
                        },
                        format!(
                            "Increase partitions from {} to {}",
                            current_topic.partitions, desired_topic.partitions
                        )
                    ));
                } else if desired_topic.partitions < current_topic.partitions {
                    return Err(anyhow::anyhow!(
                        "Cannot decrease partition count from {} to {} (partitions for existing topics can only be increased)",
                        current_topic.partitions,
                        desired_topic.partitions
                    ));
                }

                // Check if replication factor changed (immutable in Kafka)
                if desired_topic.replication_factor != current_topic.replication_factor {
                    return Err(anyhow::anyhow!(
                        "Cannot change replication factor from {} to {} (immutable)",
                        current_topic.replication_factor,
                        desired_topic.replication_factor
                    ));
                }

                // Check config changes
                if desired_topic.config != current_topic.config {
                    ops.push(connector_op!(
                        KafkaConnectorOp::UpdateTopicConfig {
                            config: desired_topic.config.clone()
                        },
                        "Update topic configuration".to_string()
                    ));
                }
            }
        }

        Ok(ops)
    }

    async fn plan_acl(
        &self,
        addr: KafkaResourceAddress,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> anyhow::Result<Vec<PlanResponseElement>> {
        let mut ops = Vec::new();

        match (current, desired) {
            (None, None) => {}
            (None, Some(desired_bytes)) => {
                let desired_acl: resource::KafkaAcl = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired ACL")?
                    .into();

                ops.push(connector_op!(
                    KafkaConnectorOp::CreateAcl(desired_acl.clone()),
                    format!("Create ACL for {:?} on {}", desired_acl.principal, desired_acl.resource_name)
                ));
            }
            (Some(current_bytes), None) => {
                let current_acl: resource::KafkaAcl = resource::KafkaResource::from_bytes(&addr, &current_bytes)
                    .context("Failed to parse current ACL")?
                    .into();

                ops.push(connector_op!(
                    KafkaConnectorOp::DeleteAcl(current_acl),
                    "Delete ACL".to_string()
                ));
            }
            (Some(current_bytes), Some(desired_bytes)) => {
                // ACLs are typically recreated rather than updated
                let current_acl: resource::KafkaAcl = resource::KafkaResource::from_bytes(&addr, &current_bytes)
                    .context("Failed to parse current ACL")?
                    .into();

                let desired_acl: resource::KafkaAcl = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired ACL")?
                    .into();

                if current_acl != desired_acl {
                    ops.push(connector_op!(
                        KafkaConnectorOp::DeleteAcl(current_acl),
                        "Delete old ACL".to_string()
                    ));
                    ops.push(connector_op!(
                        KafkaConnectorOp::CreateAcl(desired_acl.clone()),
                        format!(
                            "Create new ACL for {:?} on {}",
                            desired_acl.principal, desired_acl.resource_name
                        )
                    ));
                }
            }
        }

        Ok(ops)
    }

    async fn plan_quota(
        &self,
        addr: KafkaResourceAddress,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> anyhow::Result<Vec<PlanResponseElement>> {
        let mut ops = Vec::new();

        match (current, desired) {
            (None, None) => {}
            (None, Some(desired_bytes)) => {
                let desired_quota: resource::KafkaQuota = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired quota")?
                    .into();

                ops.push(connector_op!(
                    KafkaConnectorOp::CreateQuota(desired_quota.clone()),
                    "Create quota".to_string()
                ));
            }
            (Some(_), None) => {
                ops.push(connector_op!(KafkaConnectorOp::DeleteQuota, "Delete quota".to_string()));
            }
            (Some(current_bytes), Some(desired_bytes)) => {
                let current_quota: resource::KafkaQuota = resource::KafkaResource::from_bytes(&addr, &current_bytes)
                    .context("Failed to parse current quota")?
                    .into();

                let desired_quota: resource::KafkaQuota = resource::KafkaResource::from_bytes(&addr, &desired_bytes)
                    .context("Failed to parse desired quota")?
                    .into();

                if current_quota != desired_quota {
                    ops.push(connector_op!(
                        KafkaConnectorOp::UpdateQuota(desired_quota.clone()),
                        "Update quota".to_string()
                    ));
                }
            }
        }

        Ok(ops)
    }
}

// Helper to convert KafkaResource into specific types
impl From<resource::KafkaResource> for resource::KafkaTopic {
    fn from(res: resource::KafkaResource) -> Self {
        match res {
            resource::KafkaResource::Topic(t) => t,
            _ => panic!("Expected Topic resource"),
        }
    }
}

impl From<resource::KafkaResource> for resource::KafkaAcl {
    fn from(res: resource::KafkaResource) -> Self {
        match res {
            resource::KafkaResource::Acl(a) => a,
            _ => panic!("Expected Acl resource"),
        }
    }
}

impl From<resource::KafkaResource> for resource::KafkaQuota {
    fn from(res: resource::KafkaResource) -> Self {
        match res {
            resource::KafkaResource::Quota(q) => q,
            _ => panic!("Expected Quota resource"),
        }
    }
}
