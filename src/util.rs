use crate::circuit_breaker::CircuitBreaker;
use std::future::Future;
use anyhow::Result;
use chrono::Duration;
use sqlx::postgres::PgDatabaseError;
use log::error;


/// Postgres returns errors in a weird way, sigh
const PG_INTEGRITY_ERROR: &str = "23";

/// Converts a SQLx error into a nested error - the outer layer is for all unexpected
/// errors, the inner layer is for Database errors from Postgres. The inner error is
/// downcast into the correct PgDatabaseError type so that it can be checked.
pub fn pg_error<T>(res: sqlx::Result<T>) -> Result<std::result::Result<T, Box<PgDatabaseError>>> {
    match res {
        Ok(t) => Ok(Ok(t)),
        Err(err) => match err {
            sqlx::Error::Database(db_err) => Ok(Err(db_err.downcast::<PgDatabaseError>())),
            err => Err(err.into()),
        },
    }
}

/// Check if an error is an integrity error (ie. unique constraint or FK relation failed)
pub fn is_pg_integrity_error(err: &PgDatabaseError) -> bool {
    &err.code()[..2] == PG_INTEGRITY_ERROR
}

/// Format a date in human readable format, but only approx
/// (ie. rounds off everything subsecond)
/// Negative durations will be printed using their absolute length
pub fn format_duration_approx(duration: Duration) -> String {
    let rounded = std::time::Duration::from_secs(duration.num_seconds().unsigned_abs());
    format!("{}", humantime::format_duration(rounded))
}

/// execute a future and retry it when it fails, using a circuit breaker
/// to abort if the future fails too often too quickly (5 times in 1 minute)
pub fn spawn_retry<F, Fut>(name: impl Into<String>, func: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<!>> + Send + 'static,
{
    let name = name.into();

    let _ = tokio::spawn(async move {
        let mut cb = CircuitBreaker::new(5, Duration::minutes(1));
        while cb.retry() {
            match func().await {
                Ok(_) => unreachable!("func never returns"),
                Err(err) => error!("task {} failed: {:?}", name, err),
            }
        }
        error!("task {} failed too many times, aborting!", name);
        std::process::exit(1);
    });
}
