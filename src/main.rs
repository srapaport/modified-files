//! # Altered History Analysis
//!
//! This application analyzes altered file histories in software repositories using the Software Heritage graph.
//! It reads classified commits from the `altered_histories` PostgreSQL table and determines which files
//! were actually modified by comparing different snapshots through the Software Heritage infrastructure.
//!
//! ## Main Workflow
//!
//! 1. **Setup Logging**: Configures file-based logging with rotation
//! 2. **Load SWH Graph**: Initializes the Software Heritage bidirectional graph with all required properties
//! 3. **Retrieve from DB**: Queries `altered_histories` for FileModified and FileRemoved commits
//! 4. **Process Modified Files**: Analyzes file modifications using the graph data
//! 5. **Write to DB**: Inserts results directly into `modified_files` table

use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::{info, LevelFilter};
use std::{path::PathBuf, time::Instant};
use swh_graph::{graph::SwhBidirectionalGraph, mph::DynMphf, SwhGraphProperties};
use anyhow::Result;
use altered_history_analysis::models;

const GRAPH_PATH: &str = ""; // /path/to/compressed/graph

#[tokio::main]
async fn main() -> Result<()> {
    Logger::with(LevelFilter::Info)
        .log_to_file(
            flexi_logger::FileSpec::default()
                .directory("logs")
                .basename("prod")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000),
            Naming::Numbers,
            Cleanup::KeepLogFiles(5),
        )
        .duplicate_to_stderr(Duplicate::Error)
        .start()
        .unwrap();

    let pool = models::get_pool().await?;

    let graph_t = SwhBidirectionalGraph::new(PathBuf::from(GRAPH_PATH))
        .expect("Could not load graph")
        .init_properties()
        .load_properties(|properties| properties.load_maps::<DynMphf>())
        .expect("Could not load maps")
        .load_properties(|properties| properties.load_label_names())
        .expect("Could no load label names")
        .load_labels()
        .expect("Could not load labels")
        .load_properties(SwhGraphProperties::load_strings)
        .expect("Could not load strings")
        .load_properties(SwhGraphProperties::load_persons)
        .expect("Could not load persons")
        .load_properties(SwhGraphProperties::load_timestamps)
        .expect("Could not load timestamps");

    let start = Instant::now();

    let data = altered_history_analysis::retrieve_file_changes(&pool)
        .await
        .expect("Failed to retrieve file changes from DB");

    let rows = altered_history_analysis::all_modified(data, &graph_t);

    info!("Graph analysis complete | time elapsed: {:.2?}", start.elapsed());
    println!("Graph analysis complete | time elapsed: {:.2?}", start.elapsed());
    println!("Collected {} rows to insert into modified_files", rows.len());

    models::insert_modified_files(&pool, &rows).await?;

    info!("All done.");
    Ok(())
}
