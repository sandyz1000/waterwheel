///! Scheduler Cluster.
///! Multiple schedulers can form a cluster using the chitchat membership finding library
///! and then choose which triggers to manage using rendezvous hashing.
///! Membership changes are expected to be rare and don't need to be handled quickly.
use anyhow::{Context, Result};
use chitchat::{transport::UdpTransport, ChitchatConfig, ChitchatHandle, ChitchatId};
use chrono::Utc;
use futures::StreamExt as _;
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use tracing::{debug, info};
use crate::{
    config::Config,
    server::Server,
};

pub fn get_node_id() -> Result<String> {
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let ts = Utc::now().timestamp();
    let name = format!("{hostname}/{ts}");
    Ok(name)
}

fn get_gossip_addr(config: &Config) -> Result<SocketAddr> {
    let gossip_addr = config
        .cluster_gossip_addr
        .parse()
        .context("parsing cluster_gossip_addr")?;

    Ok(gossip_addr)
}

pub async fn start_cluster(config: &Config, node_id: &str) -> Result<ChitchatHandle> {
    let gossip_addr = get_gossip_addr(config)?;
    let gen_id = Utc::now().timestamp() as u64;  // A monotonically increasing `gen_id`
    let chitchat_id = ChitchatId::new(node_id.to_owned(), gen_id, gossip_addr);
    let config = ChitchatConfig {
        chitchat_id,
        cluster_id: config
            .cluster_id
            .as_deref()
            .unwrap_or("waterwheel")
            .to_owned(),
        listen_addr: config
            .cluster_gossip_bind
            .parse()
            .context("parsing cluster_gossip_bind")?,
        seed_nodes: config.cluster_seed_nodes.clone(),
        gossip_interval: Duration::from_millis(50),
        failure_detector_config: Default::default(),
        // Each heartbeat increments the version, with one heartbeat each second
        // 86400 ~ 24h.
        marked_for_deletion_grace_period: 86400,
    };

    let chitchat_handle = chitchat::spawn_chitchat(config, vec![], &UdpTransport).await?;

    Ok(chitchat_handle)
}

pub async fn watch_live_nodes(server: Arc<Server>) -> Result<()> {
    let chitchat = server.get_chitchat().await;

    let mut watcher = chitchat.lock().await.live_nodes_watcher();

    while let Some(live_nodes) = watcher.next().await {
        info!("cluster membership changed");

        server.on_cluster_membership_change.send_modify(|rendezvous| {
            rendezvous.clear();
            rendezvous.add_node(server.node_id.clone());

            for (id, state) in live_nodes {
                rendezvous.add_node(id.node_id);
            }
        });

        debug!("updated rendezvous with new members");
    }

    anyhow::bail!("chitchat watcher was closed!");
}
