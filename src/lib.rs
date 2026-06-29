pub mod db;
pub mod env;
pub mod models;
mod file_modified;
mod origin_grading;
use csv::{ReaderBuilder, WriterBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn};
use rayon::{ThreadPool, ThreadPoolBuilder};
use serde::Serialize;
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use swh_graph::graph::*;

/// Default path for the legacy CSV export path (`all_modified`).
pub const MODIFIED_FILES_CSV: &str = "results/modified_files.csv";

/// Commit tuple: (snapshot_src, branch_name, missing_commit, snapshot_dst)
pub type CommitLine = (String, String, String, String);

pub fn is_file_change_subcategories(sub_categories: &str) -> bool {
    sub_categories.contains("FileModified") || sub_categories.contains("FileRemoved")
}

fn collect_commit_rows<G>(
    origin: &str,
    line: CommitLine,
    graph_t: &G,
    amount_err_compare: &AtomicUsize,
) -> Vec<env::Row>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let Some(paths) = file_modified::map_commit(line.clone(), graph_t) else {
        amount_err_compare.fetch_add(1, Ordering::Relaxed);
        return Vec::new();
    };
    let Some(res) = file_modified::compare_paths(&line.3, &line.1, &paths, graph_t) else {
        warn!(
            "Couldn't compare paths for url: {} and snap_dst: {} and branch: {}",
            origin, line.3, line.1
        );
        amount_err_compare.fetch_add(1, Ordering::Relaxed);
        return Vec::new();
    };
    res.into_iter()
        .filter(|(_, status)| *status != env::Status::Found)
        .map(|(path, status)| env::Row {
            origin: origin.to_string(),
            revision: line.2.clone(),
            branch: line.1.clone(),
            snapshot_without: line.3.clone(),
            path,
            status,
        })
        .collect()
}

fn write_rows_to_csv(
    rows: &[env::Row],
    csv_wrt: &Arc<Mutex<csv::Writer<std::fs::File>>>,
) {
    if rows.is_empty() {
        return;
    }
    if let Ok(mut writer) = csv_wrt.lock() {
        for row in rows {
            if let Err(e) = writer.serialize(row) {
                error!("Failed to write row: {}", e);
            }
        }
        if let Err(e) = writer.flush() {
            error!("Failed to flush writer: {}", e);
        }
    }
}

fn open_modified_files_writer(output_path: &str) -> Arc<Mutex<csv::Writer<std::fs::File>>> {
    if let Some(parent) = Path::new(output_path).parent() {
        fs::create_dir_all(parent).expect("Failed to create output directory");
    }
    Arc::new(Mutex::new(
        WriterBuilder::new()
            .has_headers(true)
            .from_path(output_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {}", output_path, e)),
    ))
}

fn worker_thread_pool() -> ThreadPool {
    let workers = (num_cpus::get() / 3).max(1);
    ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .expect("Failed to build rayon thread pool")
}

fn print_compare_stats(amount_err_compare: &AtomicUsize) {
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
}

/// Process a bounded batch of commits grouped by origin (legacy CSV aggregation path).
pub fn process_modified_batch<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    batch: HashMap<String, Vec<CommitLine>>,
    graph_t: &G,
    csv_wrt: &Arc<Mutex<csv::Writer<std::fs::File>>>,
    thread_pool: &ThreadPool,
    amount_err_compare: &AtomicUsize,
    bar: &ProgressBar,
) where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    thread_pool.install(|| {
        batch.into_iter().for_each(|(origin, lines)| {
            for line in lines {
                let rows = collect_commit_rows(&origin, line, graph_t, amount_err_compare);
                write_rows_to_csv(&rows, csv_wrt);
            }
            bar.inc(1);
        });
    });
}

/// Retrieves file modification data from CSV files in a specified directory.
///
/// Legacy path: loads all matching rows into memory. Prefer `all_modified_from_db` for
/// full-graph runs to avoid OOM.
pub fn retrieve_file_modified(path: &str) -> Option<HashMap<String, Vec<CommitLine>>> {
    let res: Mutex<HashMap<String, Vec<CommitLine>>> = Mutex::new(HashMap::new());
    let dir_entries = match fs::read_dir(path) {
        Ok(entries) => match entries.collect::<Result<Vec<_>, _>>() {
            Ok(collected_entries) => collected_entries,
            Err(e) => {
                eprintln!("Error collecting directory entries: {}", e);
                return None;
            }
        },
        Err(e) => {
            eprintln!("Error reading directory {}: {}", path, e);
            return None;
        }
    };

    let amount_err_record = AtomicUsize::new(0);
    let amount_file_modified = AtomicUsize::new(0);

    use rayon::prelude::*;

    dir_entries.into_par_iter().for_each(|entry| {
        let file_path = entry.path();
        if !file_path.is_file() || file_path.extension().map_or(false, |ext| ext != "csv") {
            return;
        }

        let file_path_str = match file_path.to_str() {
            Some(s) => s,
            None => return,
        };

        let reader_result = ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b';')
            .from_path(file_path_str);

        let mut reader = match reader_result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Couldn't read csv {}: {}", file_path_str, e);
                return;
            }
        };

        reader
            .records()
            .into_iter()
            .for_each(|record| match record {
                Ok(s) => {
                    if s.len() > 7 && is_file_change_subcategories(&s[7]) {
                        amount_file_modified.fetch_add(1, Ordering::Relaxed);
                        let origin = String::from(&s[0]);
                        let line = (
                            String::from(&s[1]),
                            String::from(&s[2]),
                            String::from(&s[3]),
                            String::from(&s[4]),
                        );
                        if let Ok(mut map) = res.lock() {
                            map.entry(origin).or_default().push(line);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error reading record {}", e);
                    amount_err_record.fetch_add(1, Ordering::Relaxed);
                }
            });
    });
    println!(
        "Amount of errors reading records: {}",
        amount_err_record.load(Ordering::Relaxed)
    );
    println!(
        "Amount of commits with at least 1 altered file: {}",
        amount_file_modified.load(Ordering::Relaxed)
    );
    Some(res.into_inner().unwrap_or_else(|e| e.into_inner()))
}

/// Processes aggregated commit data and writes graph-verified file rows to CSV.
pub fn all_modified<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    data: HashMap<String, Vec<CommitLine>>,
    graph_t: &G,
    output_path: &str,
) where
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
    let csv_wrt = open_modified_files_writer(output_path);
    let thread_pool = worker_thread_pool();
    process_modified_batch(
        data,
        graph_t,
        &csv_wrt,
        &thread_pool,
        &amount_err_compare,
        &bar,
    );
    bar.finish_with_message("Done");
    print_compare_stats(&amount_err_compare);
}

/// Stream classified commits from altered-history PostgreSQL in bounded pages,
/// verify file changes through the graph, and write results directly to PostgreSQL.
pub async fn all_modified_from_db<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    pool: &PgPool,
    tables: &db::TableNames,
    graph_t: &G,
    batch_size: i64,
) where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    use rayon::prelude::*;

    db::create_modified_files_table(pool, &tables.modified_files)
        .await
        .expect("Failed to create modified_files table");
    db::truncate_modified_files(pool, &tables.modified_files)
        .await
        .expect("Failed to truncate modified_files table");

    let total = db::count_file_change_commits(pool, tables)
        .await
        .expect("Failed to count file-change commits");
    println!(
        "File-change commits to process: {} -> table {}",
        total, tables.modified_files
    );

    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::with_template(
            "{msg} {wide_bar} {pos} {percent_precise}% {elapsed_precise} {duration_precise} {eta}",
        )
        .unwrap(),
    );

    let amount_err_compare = AtomicUsize::new(0);
    let thread_pool = worker_thread_pool();

    let mut after_id: i64 = 0;
    let mut total_rows_written: u64 = 0;
    loop {
        let page = db::load_file_change_commits_after(pool, tables, after_id, batch_size)
            .await
            .expect("Failed to load file-change commits page");
        if page.is_empty() {
            break;
        }
        after_id = page.last().unwrap().0;

        let page_rows = Mutex::new(Vec::new());
        thread_pool.install(|| {
            page.par_iter().for_each(
                |(_id, origin, snapshot_src, branch_name, missing_commit, snapshot_dst)| {
                    let line = (
                        snapshot_src.clone(),
                        branch_name.clone(),
                        missing_commit.clone(),
                        snapshot_dst.clone(),
                    );
                    let rows = collect_commit_rows(origin, line, graph_t, &amount_err_compare);
                    if let Ok(mut buffer) = page_rows.lock() {
                        buffer.extend(rows);
                    }
                    bar.inc(1);
                },
            );
        });

        let rows = page_rows.into_inner().unwrap_or_default();
        total_rows_written += rows.len() as u64;
        db::batch_insert_modified_files(pool, &tables.modified_files, &rows)
            .await
            .expect("Failed to insert modified file rows");
    }

    bar.finish_with_message("Done");
    println!(
        "Inserted {} file-level rows into {}",
        total_rows_written, tables.modified_files
    );
    print_compare_stats(&amount_err_compare);
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
