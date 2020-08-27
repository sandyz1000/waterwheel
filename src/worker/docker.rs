use crate::messages::TaskDef;
use anyhow::Result;
use bollard::container::{
    Config, CreateContainerOptions, LogsOptions, RemoveContainerOptions, StartContainerOptions,
    WaitContainerOptions,
};
use futures::TryStreamExt;
use kv_log_macro::{info, trace};

pub async fn run_docker(task_def: TaskDef) -> Result<bool> {
    // TODO - return actual error messages from Docker
    let exit = async_std::task::spawn_blocking(move || -> Result<bool> {
        let mut rt = tokio::runtime::Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()?;

        rt.block_on(async move {
            let image = task_def.image.unwrap();
            let args = task_def.args;
            let env = task_def.env.unwrap_or_default();

            let docker = bollard::Docker::connect_with_local_defaults()?;

            info!("launching container", {
                image: image,
                args: format!("{:?}", args),
                env: format!("{:?}", env),
            });

            let container = docker
                .create_container(
                    None::<CreateContainerOptions<String>>,
                    Config {
                        env: Some(env),
                        cmd: Some(args),
                        image: Some(image),
                        ..Config::default()
                    },
                )
                .await?;

            trace!("created container", { id: container.id });

            docker
                .start_container(&container.id, None::<StartContainerOptions<String>>)
                .await?;

            trace!("started container", { id: container.id});

            let mut logs = docker.logs(
                &container.id,
                Some(LogsOptions {
                    follow: true,
                    stdout: true,
                    stderr: true,
                    ..LogsOptions::default()
                }),
            );

            while let Some(line) = logs.try_next().await? {
                info!(target: "task", "{}", line, {
                    task_id: task_def.task_id,
                    trigger_datetime: task_def.trigger_datetime,
                });
            }

            let mut waiter =
                docker.wait_container(&container.id, None::<WaitContainerOptions<String>>);

            let mut exit = 0;
            while let Some(x) = waiter.try_next().await? {
                info!("container exit code: {}", x.status_code);
                exit = x.status_code;
            }

            docker
                .remove_container(&container.id, None::<RemoveContainerOptions>)
                .await?;

            trace!("container removed", { id: container.id });

            Ok(exit == 0)
        })
    })
    .await?;

    Ok(exit)
}
