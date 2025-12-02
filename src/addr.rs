use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

use crate::task::KafkaTask;

#[derive(Debug, Clone)]
pub enum KafkaResourceAddress {
    /// Config file at kafka/config.ron
    Config,
    /// Topic at kafka/{cluster}/topics/{topic_name}.ron
    Topic {
        cluster: String,
        topic: String,
    },
    /// ACL at kafka/{cluster}/acls/{acl_id}.ron
    Acl {
        cluster: String,
        acl_id: String,
    },
    /// Quota at kafka/{cluster}/quotas/{quota_id}.ron
    Quota {
        cluster: String,
        quota_id: String,
    },
    Task {
        kind: KafkaTask,
    },
}

impl ResourceAddress for KafkaResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            KafkaResourceAddress::Config => PathBuf::from("kafka/config.ron"),
            KafkaResourceAddress::Topic { cluster, topic } => PathBuf::from(format!("kafka/{cluster}/topics/{topic}.ron")),
            KafkaResourceAddress::Acl { cluster, acl_id } => PathBuf::from(format!("kafka/{cluster}/acls/{acl_id}.ron")),
            KafkaResourceAddress::Quota { cluster, quota_id } => {
                PathBuf::from(format!("kafka/{cluster}/quotas/{quota_id}.ron"))
            }
            KafkaResourceAddress::Task { .. } => PathBuf::from(format!("kafka/task.ron")),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match path_components[..] {
            ["kafka", "config.ron"] => Ok(KafkaResourceAddress::Config),
            ["kafka", cluster, "topics", topic_file] if topic_file.ends_with(".ron") => {
                let topic = topic_file.strip_suffix(".ron").unwrap_or(topic_file);

                Ok(KafkaResourceAddress::Topic {
                    cluster: cluster.to_string(),
                    topic: topic.to_string(),
                })
            }
            ["kafka", cluster, "acls", acl_file] if acl_file.ends_with(".ron") => {
                let acl_id = acl_file.strip_suffix(".ron").unwrap_or(acl_file);

                Ok(KafkaResourceAddress::Acl {
                    cluster: cluster.to_string(),
                    acl_id: acl_id.to_string(),
                })
            }
            ["kafka", cluster, "quotas", quota_file] if quota_file.ends_with(".ron") => {
                let quota_id = quota_file.strip_suffix(".ron").unwrap_or(quota_file);
                Ok(KafkaResourceAddress::Quota {
                    cluster: cluster.to_string(),
                    quota_id: quota_id.to_string(),
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
