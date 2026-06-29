//! Reads classified file-change commits from altered-history's PostgreSQL tables,
//! verifies file-level changes through the Software Heritage graph, and writes
//! results directly to PostgreSQL.

use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::{info, LevelFilter};
use std::{path::PathBuf, time::Instant};
use swh_graph::{graph::SwhBidirectionalGraph, mph::DynMphf, SwhGraphProperties};
use anyhow::Result;
use altered_history_analysis::{db, models};

const GRAPH_PATH: &str = "/dev/shm/swh-graph/current/graph";
const TABLE_GRAPH: &str = "/swh/scratch/graph/2026-03-02/compressed/graph";
const BATCH_SIZE: i64 = 50_000;

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

    let pool = db::init_pool().await?;
    let tables = db::TableNames::from_graph_path(TABLE_GRAPH);
    println!("Reading classified commits from table: {}", tables.altered_histories);
    println!("Writing file-level results to table: {}", tables.modified_files);

    let start = Instant::now();

    altered_history_analysis::all_modified_from_db(&pool, &tables, &graph_t, BATCH_SIZE).await;

    info!("Graph analysis time elapsed: {:.2?}", start.elapsed());

    models::copy_classified_altered_histories(&tables.altered_histories).await?;

    Ok(())
}
