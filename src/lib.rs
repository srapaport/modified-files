pub mod env;
pub mod models;
mod file_modified;
mod origin_grading;
use chashmap::CHashMap;
use csv::{ReaderBuilder, WriterBuilder};
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, warn};
use rayon::prelude::*;
use serde::Serialize;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use swh_graph::graph::*;

/// Retrieves file modification data from CSV files in a specified directory.
///
/// This function scans a directory for CSV files and extracts records containing "FileModified" entries.
/// It processes multiple CSV files in parallel and aggregates the data into a concurrent hash map.
///
/// # Arguments
///
/// * `path` - The directory path containing CSV files to process
///
/// # Returns
///
/// * `Some(CHashMap)` - A concurrent hash map where:
///   - Key: Origin URL (String)
///   - Value: Vector of tuples containing (branch, revision, snapshot_without, file_path)
/// * `None` - If the directory cannot be read or processed
///
/// # CSV Format Expected
///
/// The CSV files should have at least 8 columns with semicolon (`;`) delimiter:
/// - Column 0: Origin URL
/// - Column 1: Branch name
/// - Column 2: Revision hash
/// - Column 3: Snapshot without hash
/// - Column 4: File path
/// - Column 7: Event type (must contain "FileModified")
///
/// # Performance
///
/// Uses parallel processing with Rayon for improved performance when processing multiple CSV files.
/// Progress information is printed to stdout including error counts and successful record counts.
pub fn retrieve_file_modified(
    path: &str,
) -> Option<CHashMap<String, Vec<(String, String, String, String)>>> {
    let res = CHashMap::with_capacity(1024);
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
                    if s[7].contains("FileModified") {
                        // debug!(
                        //     "line upserted:\nori: {} | line: {},{},{},{}",
                        //     String::from(&s[0]),
                        //     String::from(&s[1]),
                        //     String::from(&s[2]),
                        //     String::from(&s[3]),
                        //     String::from(&s[4])
                        // );
                        amount_file_modified.fetch_add(1, Ordering::Relaxed);
                        res.upsert(
                            String::from(&s[0]),
                            || {
                                vec![(
                                    String::from(&s[1]),
                                    String::from(&s[2]),
                                    String::from(&s[3]),
                                    String::from(&s[4]),
                                )]
                            },
                            |vec| {
                                vec.push((
                                    String::from(&s[1]),
                                    String::from(&s[2]),
                                    String::from(&s[3]),
                                    String::from(&s[4]),
                                ));
                            },
                        );
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
    Some(res)
}

/// Processes all modified files data and generates a CSV report of altered file histories.
///
/// This function takes the aggregated file modification data and processes each entry through
/// the Software Heritage graph to determine the actual file modification status. It compares
/// file states between different snapshots to identify truly modified files versus unchanged ones.
///
/// # Arguments
///
/// * `data` - A concurrent hash map containing file modification data from `retrieve_file_modified`
/// * `graph_t` - A reference to the Software Heritage bidirectional graph with loaded properties
///
/// # Type Parameters
///
/// * `G` - Must implement `SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync`
///   with the following property traits:
///   - `Maps`: For SWHID to node ID mapping
///   - `LabelNames`: For edge label name resolution
///   - `Strings`: For string property access
///   - `Persons`: For person/author information
///   - `Timestamps`: For temporal data
///
/// # Output
///
/// Creates a CSV file at `results/modified_files.csv` with the following columns:
/// - `origin`: Origin URL
/// - `revision`: Revision hash
/// - `branch`: Branch name
/// - `snapshot_without`: Snapshot hash without the changes
/// - `path`: File path
/// - `status`: Modification status (Modified, Found, etc.)
///
/// # Performance
///
/// - Uses parallel processing with Rayon for concurrent processing of multiple origins
/// - Displays a progress bar showing processing status
/// - Logs warnings for entries that cannot be processed
/// - Reports final statistics including error counts
pub fn all_modified<
    G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync,
>(
    data: CHashMap<String, Vec<(String, String, String, String)>>,
    graph_t: &G,
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
    let csv_wrt = Arc::new(Mutex::new(
        WriterBuilder::new()
            .has_headers(true)
            .from_path("results/modified_files.csv")
            .unwrap(),
    ));
    data.into_iter().par_bridge().for_each(|(url, lines)| {
        let csv_wrt = csv_wrt.clone();
        lines.into_iter().for_each(|line| {
            let paths = file_modified::map_commit(line.clone(), graph_t).unwrap();
            let Some(res) = file_modified::compare_paths(&line.3, &line.1, &paths, graph_t) else {
                warn!(
                    "Couldn't compare paths for url: {} and snap_dst: {} and branch: {}",
                    url, line.3, line.1
                );
                amount_err_compare.fetch_add(1, Ordering::Relaxed);
                return;
            };
            let rows_to_write: Vec<_> = res
                .into_iter()
                .filter(|(_, status)| *status != env::Status::Found)
                .map(|(path, status)| env::Row {
                    origin: url.clone(),
                    revision: line.2.clone(),
                    branch: line.1.clone(),
                    snapshot_without: line.3.clone(),
                    path,
                    status,
                })
                .collect();

            if !rows_to_write.is_empty() {
                if let Ok(mut writer) = csv_wrt.lock() {
                    for row in rows_to_write {
                        if let Err(e) = writer.serialize(row) {
                            error!("Failed to write row: {}", e);
                        }
                    }
                    if let Err(e) = writer.flush() {
                        error!("Failed to flush writer: {}", e);
                    }
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
