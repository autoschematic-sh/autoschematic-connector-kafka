use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::KafkaConnector;

pub mod addr;
pub mod client;
pub mod config;
pub mod connector;
pub mod op;
pub mod resource;
pub mod task;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<KafkaConnector>().await?;
    Ok(())
}
