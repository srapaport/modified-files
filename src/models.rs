use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;

use crate::env as app_env;

pub async fn get_pool() -> Result<sqlx::PgPool> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .context("Failed to create pool.")?;

    println!("Connected to the database!");
    Ok(pool)
}

fn status_to_str(s: &app_env::Status) -> &'static str {
    match s {
        app_env::Status::NotFound => "NotFound",
        app_env::Status::Modified => "Modified",
        app_env::Status::Found => "Found",
    }
}

pub async fn insert_modified_files(pool: &sqlx::PgPool, rows: &[app_env::Row]) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS modified_files (
            id BIGSERIAL PRIMARY KEY,
            origin TEXT NOT NULL,
            revision TEXT NOT NULL,
            branch TEXT NOT NULL,
            snapshot_without TEXT NOT NULL,
            path TEXT NOT NULL,
            status TEXT NOT NULL,
            source_category TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("Failed to create modified_files table")?;

    sqlx::query("CREATE INDEX IF NOT EXISTS idx_mf_origin ON modified_files (origin)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_mf_path ON modified_files (path)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_mf_source_category ON modified_files (source_category)")
        .execute(pool)
        .await?;

    println!("Table modified_files created or verified.");

    if rows.is_empty() {
        println!("No rows to insert.");
        return Ok(());
    }

    let bar = ProgressBar::new(rows.len() as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} {wide_bar} {pos}/{len} {percent}% {elapsed_precise} ETA: {eta}",
        )
        .unwrap(),
    );
    bar.set_message("Inserting into modified_files");

    for chunk in rows.chunks(1000) {
        let mut origins: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut revisions: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut branches: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut snapshot_withouts: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut paths: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut statuses: Vec<&str> = Vec::with_capacity(chunk.len());
        let mut source_categories: Vec<&str> = Vec::with_capacity(chunk.len());

        for row in chunk {
            origins.push(&row.origin);
            revisions.push(&row.revision);
            branches.push(&row.branch);
            snapshot_withouts.push(&row.snapshot_without);
            paths.push(&row.path);
            statuses.push(status_to_str(&row.status));
            source_categories.push(&row.source_category);
        }

        sqlx::query(
            "INSERT INTO modified_files (origin, revision, branch, snapshot_without, path, status, source_category)
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])",
        )
        .bind(&origins)
        .bind(&revisions)
        .bind(&branches)
        .bind(&snapshot_withouts)
        .bind(&paths)
        .bind(&statuses)
        .bind(&source_categories)
        .execute(pool)
        .await
        .context("Failed to batch insert modified files")?;

        bar.inc(chunk.len() as u64);
    }

    bar.finish_with_message("Done inserting into modified_files");
    println!("Inserted {} rows into modified_files", rows.len());
    Ok(())
}
