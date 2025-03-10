pub mod env;
pub mod file_modified;
pub mod origin_grading;
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
        .from_path("test.csv")
        .unwrap();
    res.into_iter().for_each(|(path, status)| {
        if status != env::Status::Found {
            csv_wrt.serialize(RowTmp { path, status }).unwrap();
        }
    });
    csv_wrt.flush().unwrap();
}
