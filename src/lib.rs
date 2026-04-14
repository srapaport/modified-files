pub mod env;
pub mod models;
mod file_modified;
mod origin_grading;
use chashmap::CHashMap;
use csv::WriterBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn};
use rayon::prelude::*;
use serde::Serialize;
use sqlx::PgPool;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use swh_graph::graph::*;

/// Retrieves file change data (FileModified and FileRemoved) from the altered_histories DB table.
///
/// Returns a concurrent hash map where:
///   - Key: Origin URL (String)
///   - Value: Vector of tuples (snapshot_src, branch_name, missing_commit, snapshot_dst, sub_categories)
pub async fn retrieve_file_changes(
    pool: &PgPool,
) -> Option<CHashMap<String, Vec<(String, String, String, String, String)>>> {
    let rows: Vec<(String, String, String, String, String, String)> = match sqlx::query_as(
        "SELECT origin, snapshot_src, branch_name, missing_commit, snapshot_dst, sub_categories
         FROM altered_histories
         WHERE (sub_categories LIKE '%FileModified%' OR sub_categories LIKE '%FileRemoved%')
           AND status = 'classified'",
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error querying altered_histories: {}", e);
            return None;
        }
    };

    let res = CHashMap::with_capacity(1024);
    let mut count = 0usize;
    for (origin, snapshot_src, branch_name, missing_commit, snapshot_dst, sub_categories) in rows {
        count += 1;
        res.upsert(
            origin,
            || vec![(snapshot_src.clone(), branch_name.clone(), missing_commit.clone(), snapshot_dst.clone(), sub_categories.clone())],
            |vec| {
                vec.push((snapshot_src.clone(), branch_name.clone(), missing_commit.clone(), snapshot_dst.clone(), sub_categories.clone()));
            },
        );
    }
    println!("Retrieved {} records from altered_histories (FileModified + FileRemoved)", count);
    Some(res)
}

fn extract_source_category(sub_categories: &str) -> String {
    if sub_categories.contains("FileModified") {
        "FileModified".to_string()
    } else if sub_categories.contains("FileRemoved") {
        "FileRemoved".to_string()
    } else {
        "Unknown".to_string()
    }
}

/// Processes all file changes and returns rows for DB insertion.
///
/// Takes the aggregated file change data and processes each entry through
/// the Software Heritage graph to determine the actual file modification status.
pub fn all_modified<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    data: CHashMap<String, Vec<(String, String, String, String, String)>>,
    graph_t: &G,
) -> Vec<env::Row>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let bar = ProgressBar::new(data.len() as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} {wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );
    let amount_err_compare = AtomicUsize::new(0);
    let collected_rows: Arc<Mutex<Vec<env::Row>>> = Arc::new(Mutex::new(Vec::new()));

    data.into_iter().par_bridge().for_each(|(url, lines)| {
        let collected_rows = collected_rows.clone();
        lines.into_iter().for_each(|line| {
            let four_tuple = (line.0.clone(), line.1.clone(), line.2.clone(), line.3.clone());
            let Some(paths) = file_modified::map_commit(four_tuple, graph_t) else {
                warn!("No dir found for rev: {}", line.2);
                return;
            };
            let Some(res) = file_modified::compare_paths(&line.3, &line.1, &paths, graph_t) else {
                warn!(
                    "Couldn't compare paths for url: {} and snap_dst: {} and branch: {}",
                    url, line.3, line.1
                );
                amount_err_compare.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let source_cat = extract_source_category(&line.4);
            let rows_to_add: Vec<_> = res
                .into_iter()
                .filter(|(_, status)| *status != env::Status::Found)
                .map(|(path, status)| env::Row {
                    origin: url.clone(),
                    revision: line.2.clone(),
                    branch: line.1.clone(),
                    snapshot_without: line.3.clone(),
                    path,
                    status,
                    source_category: source_cat.clone(),
                })
                .collect();

            if !rows_to_add.is_empty() {
                if let Ok(mut rows) = collected_rows.lock() {
                    rows.extend(rows_to_add);
                }
            }
        });
        bar.inc(1);
    });
    bar.finish_with_message("Done");
    println!(
        "Amount of altered commits that weren't checked: {}",
        amount_err_compare.load(Ordering::Relaxed)
    );
    info!(
        "Amount of altered commits that weren't checked: {}",
        amount_err_compare.load(Ordering::Relaxed)
    );
    println!(
        "Amount of branch without name: {}",
        env::ERR_BRANCH.load(Ordering::Relaxed)
    );
    info!(
        "Amount of branch without name: {}",
        env::ERR_BRANCH.load(Ordering::Relaxed)
    );

    Arc::try_unwrap(collected_rows)
        .expect("Failed to unwrap collected rows")
        .into_inner()
        .expect("Failed to unwrap mutex")
}

pub fn single_modified<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph,
>(
    line: (String, String, String, String),
    graph_t: &G,
) where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let paths = file_modified::map_commit(line.clone(), graph_t).unwrap();

    let Some(res) = file_modified::compare_paths(&line.3, &line.1, &paths, graph_t) else {
        println!("Couldn't compare paths");
        return;
    };
    #[derive(Serialize)]
    struct RowTmp {
        path: String,
        status: env::Status,
    }
    let mut csv_wrt = WriterBuilder::new()
        .has_headers(true)
        .from_path(format!("{}.csv", line.2))
        .unwrap();
    res.into_iter().for_each(|(path, status)| {
        if status != env::Status::Found {
            csv_wrt.serialize(RowTmp { path, status }).unwrap();
        }
    });
    csv_wrt.flush().unwrap();
}

pub fn all_grade<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    graph: &G,
) where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut csv_wrt = match WriterBuilder::new().from_path("results/grades.csv") {
        Ok(writer) => writer,
        Err(e) => {
            error!("couldn't create csv file: {:?}", e);
            return;
        }
    };
    #[derive(Serialize)]
    struct Row{
        origin: String,
        amount_contrib: usize,
        amount_author: usize,
        amount_committer: usize,
        amount_snap: usize,
        amount_rel: usize,
        amount_rev: usize,
        freq_snap: f64,
        freq_rev: f64,
    }
    origin_grading::grades(graph).into_iter().for_each(|(url, stats)|{
        csv_wrt.serialize(Row{
            origin: url.clone(),
            amount_contrib: stats.amount_contrib,
            amount_author: stats.amount_author,
            amount_committer: stats.amount_committer,
            amount_snap: stats.amount_snap,
            amount_rel: stats.amount_rel,
            amount_rev: stats.amount_rev,
            freq_snap: stats.freq_snap,
            freq_rev: stats.freq_rev,
        }).expect(&format!("Couldn't serialize stats for {}", url));
    });
    csv_wrt.flush().unwrap();
}
