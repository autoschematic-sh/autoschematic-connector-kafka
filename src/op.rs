use autoschematic_core::connector::ConnectorOp;
use autoschematic_core::util::RON;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::resource::{KafkaAcl, KafkaQuota, KafkaTopic};

#[derive(Debug, Serialize, Deserialize)]
pub enum KafkaConnectorOp {
    // Topic operations
    CreateTopic(KafkaTopic),
    UpdateTopicPartitions { partitions: i32 },
    UpdateTopicConfig { config: IndexMap<String, String> },
    DeleteTopic,

    // ACL operations
    CreateAcl(KafkaAcl),
    DeleteAcl(KafkaAcl),

    // Quota operations
    CreateQuota(KafkaQuota),
    UpdateQuota(KafkaQuota),
    DeleteQuota,
}

impl ConnectorOp for KafkaConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}
