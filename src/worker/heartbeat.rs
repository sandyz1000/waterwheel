use crate::{config, GIT_VERSION};
use crate::messages::WorkerHeartbeat;
use anyhow::Result;

use chrono::Utc;
use tracing::{debug, trace, warn, error};

use super::{RUNNING_TASKS, TOTAL_TASKS, WORKER_ID};
use reqwest::{StatusCode, Url};

pub async fn post_heartbeat(client: &reqwest::Client) -> Result<bool> {
    let server_addr = config::get().server_addr.as_ref();
    let url = Url::parse(server_addr)?.join("int-api/heartbeat")?;

    let resp = client
        .post(url.clone())
        .json(&WorkerHeartbeat {
            uuid: *WORKER_ID,
            addr: "TODO".to_owned(),
            last_seen_datetime: Utc::now(),
            running_tasks: RUNNING_TASKS.get(),
            total_tasks: TOTAL_TASKS.get(),
            version: GIT_VERSION.to_owned(),
        })
        .send()
        .await;

    match resp {
        Ok(resp) if resp.status() == StatusCode::OK => {
            trace!("heartbeat: OK");
            Ok(true)
        }
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await?;
            warn!("heartbeat: {}", status);
            debug!("heartbeat: {}", body);
            Ok(false)
        }
        Err(err) => {
            if err.is_connect() {
                warn!("heartbeat failed: could not connect to the server");
            } else if err.is_timeout() {
                warn!("heartbeat failed: server timed out");
            } else {
                warn!("heartbeat failed: unexpected reason {}", err);
            }
            debug!("heartbeat failed: {}", err);
            Ok(false)
        }
    }
}

pub async fn heartbeat() -> Result<!> {
    let client = reqwest::Client::new();

    loop {
        trace!("sending heartbeat");
        post_heartbeat(&client).await?;

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

pub async fn wait_for_server() {
    // before accepting tasks perform a synchronous heartbeat to ensure
    // the server has our worker ID recorded
    let client = reqwest::Client::new();

    trace!("waiting for initial heartbeat");
    let mut retries = 5;
    loop {
        trace!("sending heartbeat");
        if post_heartbeat(&client).await.expect("error posting heartbeat") {
            break;
        }

        retries -= 1;
        if retries == 0 {
            error!("failed to send initial heartbeat to the server, aborting!");
            std::process::exit(1);
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    trace!("server received initial heartbeat, starting work");
}
