use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::client::{KafkaAdminClient, create_admin_client};
use crate::resource::{self, KafkaAcl, KafkaQuota, KafkaTopic};
use crate::{addr::KafkaResourceAddress, config::KafkaConnectorConfig};
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, DocIdent, FilterResponse, GetDocResponse, GetResourceResponse, OpExecResponse,
        PlanResponseElement, Resource, ResourceAddress, SkeletonResponse, TaskExecResponse,
    },
    diag::DiagnosticResponse,
    doc_dispatch, skeleton,
    util::{ron_check_eq, ron_check_syntax},
};
use indexmap::IndexMap;
use tokio::sync::{RwLock, Semaphore};

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

pub struct KafkaConnector {
    prefix: PathBuf,
    /// Map of cluster name to Kafka admin client
    clients: RwLock<HashMap<String, KafkaAdminClient>>,
    config: RwLock<KafkaConnectorConfig>,
    semaphore: RwLock<Semaphore>,
}

impl Default for KafkaConnector {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            clients: RwLock::new(HashMap::new()),
            config: Default::default(),
            semaphore: RwLock::new(Semaphore::const_new(1)),
        }
    }
}

#[async_trait]
impl Connector for KafkaConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(KafkaConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config = KafkaConnectorConfig::try_load(&self.prefix)?.unwrap_or_default();

        // Create admin clients for each cluster
        let mut clients = HashMap::new();
        for (cluster_name, cluster_config) in &config.clusters {
            let client = create_admin_client(cluster_config)?;
            clients.insert(cluster_name.clone(), client);
        }

        *self.config.write().await = config.clone();
        *self.semaphore.write().await = Semaphore::new(config.concurrent_requests);
        *self.clients.write().await = clients;

        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(addr) = KafkaResourceAddress::from_path(addr) {
            match addr {
                KafkaResourceAddress::Config => Ok(FilterResponse::Config),
                _ => Ok(FilterResponse::Resource),
            }
        } else {
            Ok(FilterResponse::none())
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn subpaths(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        let config = self.config.read().await;
        let mut subpaths = Vec::new();

        for cluster_name in config.clusters.keys() {
            subpaths.push(PathBuf::from(format!("kafka/{}", cluster_name)));
        }

        Ok(subpaths)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        res.push(skeleton!(KafkaResourceAddress::Config, KafkaConnectorConfig::default()));

        let mut topic_config = IndexMap::new();
        topic_config.insert("retention.ms".to_string(), "604800000".to_string()); // 7 days
        topic_config.insert("compression.type".to_string(), "snappy".to_string());

        res.push(skeleton!(
            KafkaResourceAddress::Topic {
                cluster: String::from("[cluster_name]"),
                topic: String::from("[topic_name]"),
            },
            resource::KafkaResource::Topic(KafkaTopic {
                partitions: 3,
                replication_factor: 2,
                config: topic_config,
            })
        ));

        res.push(skeleton!(
            KafkaResourceAddress::Acl {
                cluster: String::from("[cluster_name]"),
                acl_id: String::from("[acl_identifier]"),
            },
            resource::KafkaResource::Acl(KafkaAcl::default())
        ));

        res.push(skeleton!(
            KafkaResourceAddress::Quota {
                cluster: String::from("[cluster_name]"),
                quota_id: String::from("[quota_identifier]"),
            },
            resource::KafkaResource::Quota(KafkaQuota::default())
        ));

        Ok(res)
    }

    async fn get_docstring(&self, _addr: &Path, ident: DocIdent) -> Result<Option<GetDocResponse>, anyhow::Error> {
        use crate::config::*;
        use crate::resource::*;

        doc_dispatch!(
            ident,
            [
                KafkaConnectorConfig,
                KafkaTopic,
                KafkaAcl,
                KafkaQuota,
                KafkaClusterConfig,
                KafkaTlsConfig,
            ],
            [
                KafkaAuth::None,
                KafkaAuth::SaslPlain,
                KafkaAuth::SaslScramSha256,
                KafkaAuth::SaslScramSha512,
                KafkaAuth::SaslGssapi,
                KafkaResourcePatternType::Literal,
                KafkaResourcePatternType::Prefixed,
                KafkaQuotaEntityType::User,
            ]
        )
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = KafkaResourceAddress::from_path(addr)?;

        match addr {
            KafkaResourceAddress::Config => ron_check_eq::<KafkaConnectorConfig>(a, b),
            KafkaResourceAddress::Topic { .. } => ron_check_eq::<KafkaTopic>(a, b),
            KafkaResourceAddress::Acl { .. } => ron_check_eq::<KafkaAcl>(a, b),
            KafkaResourceAddress::Quota { .. } => ron_check_eq::<KafkaQuota>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = KafkaResourceAddress::from_path(addr)?;

        match addr {
            KafkaResourceAddress::Config => ron_check_syntax::<KafkaConnectorConfig>(a),
            KafkaResourceAddress::Topic { .. } => ron_check_syntax::<KafkaTopic>(a),
            KafkaResourceAddress::Acl { .. } => ron_check_syntax::<KafkaAcl>(a),
            KafkaResourceAddress::Quota { .. } => ron_check_syntax::<KafkaQuota>(a),
        }
    }

    async fn task_exec(
        &self,
        addr: &Path,
        _body: Vec<u8>,

        // `arg` sets the initial argument for the task. `arg` is set to None after the first execution.
        _arg: Option<Vec<u8>>,
        // The current state of the task as returned by a previous task_exec(...) call.
        // state always starts as None when a task is first executed.
        _state: Option<Vec<u8>>,
    ) -> anyhow::Result<TaskExecResponse> {
        let addr = KafkaResourceAddress::from_path(addr)?;

        match addr {
            KafkaResourceAddress::Task { kind } => Ok(TaskExecResponse::default()),
            _ => Ok(TaskExecResponse::default()),
        }
    }
}
