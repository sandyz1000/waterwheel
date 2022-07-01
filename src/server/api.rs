use crate::server::Server;
use anyhow::Result;
use lapin::Channel;
use std::sync::Arc;

pub mod auth;
mod config_cache;
mod heartbeat;
mod job;
pub mod jwt;
mod project;
mod request_ext;
mod stash;
mod status;
mod task;
mod task_logs;
pub mod types;
mod updates;
mod workers;

pub struct State {
    amqp_channel: Channel,
    server: Arc<Server>,
}

impl highnoon::State for State {
    type Context = ();
    fn new_context(&self) -> Self::Context {
        Self::Context::default()
    }
}

#[allow(unused)]
macro_rules! get_file {
    ($data:expr ; $mime:expr) => {
        |_req| async {
            use highnoon::{headers::ContentType, Mime, Response};

            Response::ok()
                .body($data)
                .header(ContentType::from($mime.parse::<Mime>().unwrap()))
        }
    };
}

pub async fn make_app(server: Arc<Server>) -> Result<highnoon::App<State>> {
    let state = State {
        amqp_channel: server.amqp_conn.create_channel().await?,
        server,
    };

    updates::setup(&state.amqp_channel).await?;
    config_cache::setup(&state.amqp_channel).await?;

    let mut app = highnoon::App::new(state);
    app.with(highnoon::filter::Log);

    // basic healthcheck to see if waterwheel is up
    app.at("/healthcheck").get(|_req| async { Ok("OK") });

    app.at("/api/status").get(status::status);

    // worker heartbeats
    app.at("/int-api/heartbeat").post(heartbeat::post);

    // project
    app.at("/api/projects")
        .get(project::get_by_name)
        .post(project::create)
        .put(project::create);
    app.at("/api/projects/:id")
        .get(project::get_by_id)
        .delete(project::delete);
    app.at("/api/projects/:id/jobs").get(project::list_jobs);

    app.at("/int-api/projects/:id/config")
        .get(project::get_config);

    // project stash
    app.at("/api/projects/:id/stash").get(stash::project::list);
    app.at("/api/projects/:id/stash/:key")
        .put(stash::project::create)
        .delete(stash::project::delete);

    app.at("/int-api/projects/:id/stash/:key")
        .get(stash::project::get);

    // job
    app.at("/api/jobs")
        .get(job::get_by_name)
        .post(job::create)
        .put(job::create);
    app.at("/api/jobs/:id")
        .get(job::get_by_id)
        .delete(job::delete);
    app.at("/api/jobs/:id/tasks").get(job::list_tasks);
    app.at("/api/jobs/:id/paused")
        .get(job::get_paused)
        .put(job::set_paused);
    app.at("/api/jobs/:id/graph").get(job::get_graph);
    app.at("/api/jobs/:id/duration").get(job::get_duration);

    // job tokens
    app.at("/api/jobs/:id/tokens").get(job::get_tokens);
    app.at("/api/jobs/:id/tokens-overview")
        .get(job::get_tokens_overview);
    app.at("/api/jobs/:id/tokens/:trigger_datetime")
        .get(job::get_tokens_trigger_datetime)
        .delete(job::clear_tokens_trigger_datetime);

    // job runs
    app.at("/api/jobs/:id/runs/:trigger_datetime")
        .get(job::list_job_all_task_runs);

    // job triggers
    app.at("/api/jobs/:id/triggers")
        .get(job::get_triggers_by_job);

    // job stash
    app.at("/int-api/jobs/:id/stash/:trigger_datetime/")
        .get(stash::job::list);
    app.at("/int-api/jobs/:id/stash/:trigger_datetime/:key")
        .put(stash::job::create)
        .get(stash::job::get)
        .delete(stash::job::delete);

    // tasks
    app.at("/api/tasks/:id").get(task::get_task_def);
    app.at("/api/tasks/:id/tokens")
        .post(task::activate_multiple_tokens);
    app.at("/api/tasks/:id/tokens/:trigger_datetime")
        .put(task::activate_token);
    app.at("/int-api/tasks/:id")
        .get(task::internal_get_task_def);

    // task runs
    app.at("/api/tasks/:id/runs/:trigger_datetime")
        .get(job::list_task_runs);

    // task logs - TODO unimplemented
    //app.at("/api/tasks/:id/logs").ws(task_logs::logs);

    // trigger times
    app.at("/api/triggers/:id").get(job::get_trigger);

    // workers
    app.at("/api/workers").get(workers::list);
    app.at("/api/workers/:id").get(workers::tasks);

    // stash
    app.at("/api/stash").get(stash::global::list);
    app.at("/api/stash/:key")
        .put(stash::global::create)
        .delete(stash::global::delete);

    app.at("/int-api/stash/:key").get(stash::global::get);

    // web UI

    #[cfg(debug_assertions)]
    {
        let index = |_req| async {
            let body = highnoon::Response::ok().path("ui/dist/index.html").await?;
            Ok(body)
        };
        app.at("/static/*").static_files("ui/dist/");
        app.at("/**").get(index);
        app.at("/").get(index);
    }

    #[cfg(not(debug_assertions))]
    {
        static JS: &str = include_str!("../../ui/dist/main.js");
        static HTML: &str = include_str!("../../ui/dist/index.html");

        app.at("/static/main.js")
            .get(get_file!(JS; "text/javascript"));
        app.at("/").get(get_file!(HTML; "text/html;charset=utf-8"));
        app.at("/**")
            .get(get_file!(HTML; "text/html;charset=utf-8"));
    }

    Ok(app)
}

pub async fn serve(server: Arc<Server>) -> Result<()> {
    let app = make_app(server.clone()).await?;
    app.listen(&server.config.server_bind).await?;
    Ok(())
}
