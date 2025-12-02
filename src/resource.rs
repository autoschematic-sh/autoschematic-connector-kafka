use std::collections::HashMap;

use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    error_util::invalid_addr,
    macros::FieldTypes,
    util::RON,
};
use autoschematic_macros::FieldTypes;
use documented::{Documented, DocumentedFields};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::addr::KafkaResourceAddress;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(default, deny_unknown_fields)]
/// A Kafka topic with its configuration settings
pub struct KafkaTopic {
    /// Number of partitions for the topic
    pub partitions: i32,
    /// Replication factor for the topic
    pub replication_factor: i16,
    /// Topic-level configuration properties
    pub config: IndexMap<String, String>,
}

impl Default for KafkaTopic {
    fn default() -> Self {
        Self {
            partitions: 1,
            replication_factor: 1,
            config: IndexMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// The type of Kafka resource being controlled by the ACL
pub enum KafkaResourceType {
    /// Topic resource type
    Topic,
    /// Consumer group resource type
    Group,
    /// Cluster resource type
    Cluster,
    /// Transactional ID resource type
    TransactionalId,
    /// Delegation token resource type
    DelegationToken,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// The pattern type for matching Kafka resources in ACLs
pub enum KafkaResourcePatternType {
    /// Matches resources with the exact name
    Literal,
    /// Matches resources with names that have the specified prefix
    Prefixed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// The type of principal in a Kafka ACL
pub enum KafkaPrincipalType {
    /// A user principal
    User,
    /// A group principal
    Group,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// A Kafka principal (user or group)
pub struct KafkaPrincipal {
    /// The type of principal
    pub principal_type: KafkaPrincipalType,
    /// The name of the principal
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// The operation type for Kafka ACLs
pub enum KafkaAclOperation {
    /// Read operation
    Read,
    /// Write operation
    Write,
    /// Create operation
    Create,
    /// Delete operation
    Delete,
    /// Alter operation
    Alter,
    /// Describe operation
    Describe,
    /// ClusterAction operation
    ClusterAction,
    /// DescribeConfigs operation
    DescribeConfigs,
    /// AlterConfigs operation
    AlterConfigs,
    /// IdempotentWrite operation
    IdempotentWrite,
    /// All operations
    All,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// Permission type for Kafka ACLs
pub enum KafkaAclPermission {
    /// Allow the operation
    Allow,
    /// Deny the operation
    Deny,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// A Kafka Access Control List (ACL) entry
pub struct KafkaAcl {
    /// The type of resource this ACL applies to
    pub resource_type: KafkaResourceType,
    /// The name of the resource (e.g., topic name, group name)
    pub resource_name: String,
    /// The pattern type for matching resources
    pub pattern_type: KafkaResourcePatternType,
    /// The principal this ACL applies to
    pub principal: KafkaPrincipal,
    /// The host from which the principal is allowed/denied access (* for all hosts)
    pub host: String,
    /// The operation this ACL controls
    pub operation: KafkaAclOperation,
    /// Whether this ACL allows or denies the operation
    pub permission: KafkaAclPermission,
}

impl Default for KafkaAcl {
    fn default() -> Self {
        Self {
            resource_type: KafkaResourceType::Topic,
            resource_name: String::new(),
            pattern_type: KafkaResourcePatternType::Literal,
            principal: KafkaPrincipal {
                principal_type: KafkaPrincipalType::User,
                name: String::new(),
            },
            host: String::from("*"),
            operation: KafkaAclOperation::All,
            permission: KafkaAclPermission::Allow,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields)]
/// The entity type for Kafka quotas
pub enum KafkaQuotaEntityType {
    /// User quota
    User,
    /// Client ID quota
    ClientId,
    /// IP address quota
    Ip,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Documented, DocumentedFields, FieldTypes)]
/// A Kafka quota entity identifier
pub struct KafkaQuotaEntity {
    /// The type of entity
    pub entity_type: KafkaQuotaEntityType,
    /// The name of the entity (user name, client ID, or IP address)
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Documented, DocumentedFields, FieldTypes)]
#[serde(deny_unknown_fields)]
/// Kafka quotas for rate limiting clients
pub struct KafkaQuota {
    /// The entities this quota applies to (e.g., user, client ID)
    pub entities: Vec<KafkaQuotaEntity>,
    /// Producer byte rate quota (bytes/second, optional)
    pub producer_byte_rate: Option<f64>,
    /// Consumer byte rate quota (bytes/second, optional)
    pub consumer_byte_rate: Option<f64>,
    /// Request percentage quota (percentage, optional)
    pub request_percentage: Option<f64>,
}

impl Default for KafkaQuota {
    fn default() -> Self {
        Self {
            entities: Vec::new(),
            producer_byte_rate: None,
            consumer_byte_rate: None,
            request_percentage: None,
        }
    }
}

pub enum KafkaResource {
    Topic(KafkaTopic),
    Acl(KafkaAcl),
    Quota(KafkaQuota),
}

impl Resource for KafkaResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);
        match self {
            KafkaResource::Topic(topic) => Ok(RON.to_string_pretty(&topic, pretty_config)?.into()),
            KafkaResource::Acl(acl) => Ok(RON.to_string_pretty(&acl, pretty_config)?.into()),
            KafkaResource::Quota(quota) => Ok(RON.to_string_pretty(&quota, pretty_config)?.into()),
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = KafkaResourceAddress::from_path(&addr.to_path_buf())?;
        let s = std::str::from_utf8(s)?;

        match addr {
            KafkaResourceAddress::Topic { .. } => Ok(KafkaResource::Topic(RON.from_str(s)?)),
            KafkaResourceAddress::Acl { .. } => Ok(KafkaResource::Acl(RON.from_str(s)?)),
            KafkaResourceAddress::Quota { .. } => Ok(KafkaResource::Quota(RON.from_str(s)?)),
            _ => Err(invalid_addr(&addr)),
        }
    }
}
