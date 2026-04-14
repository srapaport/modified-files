use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenv::dotenv;

use crate::env as app_env;

/// Holds the suffixed table names derived from the graph timestamp.
#[derive(Debug, Clone)]
pub struct TableNames {
    pub altered_histories: String,
    pub modified_files: String,
}

/// Extracts a `YYYY_MM_DD` suffix from a graph path containing a date segment
/// like `2025-05-18`. Falls back to an empty suffix if no date is found.
pub fn extract_graph_suffix(graph_path: &str) -> String {
    let re = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();
    if let Some(caps) = re.captures(graph_path) {
        format!("{}_{}_{}",
            caps.get(1).unwrap().as_str(),
            caps.get(2).unwrap().as_str(),
            caps.get(3).unwrap().as_str(),
        )
    } else {
        String::new()
    }
}

impl TableNames {
    pub fn from_graph_path(graph_path: &str) -> Self {
        let suffix = extract_graph_suffix(graph_path);
        if suffix.is_empty() {
            TableNames {
                altered_histories: "altered_histories".to_string(),
                modified_files: "modified_files".to_string(),
            }
        } else {
            TableNames {
                altered_histories: format!("altered_histories_{}", suffix),
                modified_files: format!("modified_files_{}", suffix),
            }
        }
    }
}

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

pub async fn insert_modified_files(
    pool: &sqlx::PgPool,
    tables: &TableNames,
    rows: &[app_env::Row],
) -> Result<()> {
    let q = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGSERIAL PRIMARY KEY,
            origin TEXT NOT NULL,
            revision TEXT NOT NULL,
            branch TEXT NOT NULL,
            snapshot_without TEXT NOT NULL,
            path TEXT NOT NULL,
            status TEXT NOT NULL,
            source_category TEXT NOT NULL
        )",
        tables.modified_files
    );
    sqlx::query(&q)
        .execute(pool)
        .await
        .context("Failed to create modified_files table")?;

    let idx = format!(
        "CREATE INDEX IF NOT EXISTS idx_{t}_origin ON {t} (origin)",
        t = tables.modified_files
    );
    sqlx::query(&idx).execute(pool).await?;
    let idx = format!(
        "CREATE INDEX IF NOT EXISTS idx_{t}_path ON {t} (path)",
        t = tables.modified_files
    );
    sqlx::query(&idx).execute(pool).await?;
    let idx = format!(
        "CREATE INDEX IF NOT EXISTS idx_{t}_source_category ON {t} (source_category)",
        t = tables.modified_files
    );
    sqlx::query(&idx).execute(pool).await?;

    println!("Table {} created or verified.", tables.modified_files);

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
    bar.set_message(format!("Inserting into {}", tables.modified_files));

    let insert_q = format!(
        "INSERT INTO {} (origin, revision, branch, snapshot_without, path, status, source_category)
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[])",
        tables.modified_files
    );

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

        sqlx::query(&insert_q)
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

    bar.finish_with_message(format!("Done inserting into {}", tables.modified_files));
    println!("Inserted {} rows into {}", rows.len(), tables.modified_files);
    Ok(())
}
