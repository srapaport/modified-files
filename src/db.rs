use anyhow::{Context, Result};
use dotenv::dotenv;
use regex::Regex;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

/// Holds the suffixed table names derived from the graph timestamp.
#[derive(Debug, Clone)]
pub struct TableNames {
    pub altered_histories: String,
    pub modified_files: String,
}

pub fn extract_graph_suffix(graph_path: &str) -> String {
    let re = Regex::new(r"(\d{4})-(\d{2})-(\d{2})").unwrap();
    if let Some(caps) = re.captures(graph_path) {
        format!(
            "{}_{}_{}",
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

pub async fn init_pool() -> Result<PgPool> {
    dotenv().ok();
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .context("Failed to connect to PostgreSQL")?;
    Ok(pool)
}

const FILE_CHANGE_FILTER: &str =
    "(sub_categories LIKE '%FileModified%' OR sub_categories LIKE '%FileRemoved%')";

pub async fn count_file_change_commits(pool: &PgPool, tables: &TableNames) -> Result<u64> {
    let q = format!(
        "SELECT COUNT(*) FROM {} WHERE status = 'classified' AND {}",
        tables.altered_histories, FILE_CHANGE_FILTER
    );
    let (count,): (i64,) = sqlx::query_as(&q)
        .fetch_one(pool)
        .await
        .context("Failed to count file-change commits")?;
    Ok(count as u64)
}

/// Returns the next page of classified commits with file-level sub_categories,
/// ordered by primary key for stable paging.
pub async fn load_file_change_commits_after(
    pool: &PgPool,
    tables: &TableNames,
    after_id: i64,
    limit: i64,
) -> Result<Vec<(i64, String, String, String, String, String)>> {
    let q = format!(
        "SELECT id, origin, snapshot_src, branch_name, missing_commit, snapshot_dst
         FROM {} WHERE status = 'classified' AND {} AND id > $1
         ORDER BY id LIMIT $2",
        tables.altered_histories, FILE_CHANGE_FILTER
    );
    let rows: Vec<(i64, String, String, String, String, String)> = sqlx::query_as(&q)
        .bind(after_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("Failed to load file-change commits page")?;
    Ok(rows)
}

pub async fn create_modified_files_table(pool: &PgPool, table_name: &str) -> Result<()> {
    let q = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGSERIAL PRIMARY KEY,
            origin TEXT NOT NULL,
            revision TEXT NOT NULL,
            branch TEXT NOT NULL,
            snapshot_without TEXT NOT NULL,
            path TEXT NOT NULL,
            status TEXT NOT NULL
        )",
        table_name
    );
    sqlx::query(&q)
        .execute(pool)
        .await
        .context("Failed to create modified_files table")?;

    for (idx, col) in [("origin", "origin"), ("path", "path"), ("branch", "branch")] {
        let idx_q = format!(
            "CREATE INDEX IF NOT EXISTS idx_{table}_{idx} ON {table} ({col})",
            table = table_name,
            idx = idx,
            col = col
        );
        sqlx::query(&idx_q).execute(pool).await?;
    }

    println!("Modified files table created or verified: {}", table_name);
    Ok(())
}

pub async fn truncate_modified_files(pool: &PgPool, table_name: &str) -> Result<()> {
    sqlx::query(&format!("TRUNCATE TABLE {}", table_name))
        .execute(pool)
        .await
        .context("Failed to truncate modified_files table")?;
    Ok(())
}

pub async fn batch_insert_modified_files(
    pool: &PgPool,
    table_name: &str,
    rows: &[crate::env::Row],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let q = format!(
        "INSERT INTO {} (origin, revision, branch, snapshot_without, path, status)
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])",
        table_name
    );

    for chunk in rows.chunks(1000) {
        let mut origins = Vec::with_capacity(chunk.len());
        let mut revisions = Vec::with_capacity(chunk.len());
        let mut branches = Vec::with_capacity(chunk.len());
        let mut snapshot_withouts = Vec::with_capacity(chunk.len());
        let mut paths = Vec::with_capacity(chunk.len());
        let mut statuses = Vec::with_capacity(chunk.len());

        for row in chunk {
            origins.push(row.origin.as_str());
            revisions.push(row.revision.as_str());
            branches.push(row.branch.as_str());
            snapshot_withouts.push(row.snapshot_without.as_str());
            paths.push(row.path.as_str());
            statuses.push(row.status.as_str());
        }

        sqlx::query(&q)
            .bind(&origins)
            .bind(&revisions)
            .bind(&branches)
            .bind(&snapshot_withouts)
            .bind(&paths)
            .bind(&statuses)
            .execute(pool)
            .await
            .context("Failed to batch insert modified file rows")?;
    }

    Ok(())
}
