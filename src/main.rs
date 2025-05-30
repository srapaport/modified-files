//! # Altered History Analysis
//!
//! This application analyzes altered file histories in software repositories using the Software Heritage graph.
//! It processes CSV data containing file modification events and determines which files were actually modified
//! by comparing different snapshots through the Software Heritage infrastructure.
//!
//! ## Main Workflow
//!
//! 1. **Setup Logging**: Configures file-based logging with rotation
//! 2. **Load SWH Graph**: Initializes the Software Heritage bidirectional graph with all required properties
//! 3. **Process Modified Files**: Analyzes file modifications using the graph data
//! 4. **Convert to Database**: Stores results in PostgreSQL for further analysis
//!
//! ## Configuration
//!
//! Two constants must be configured before running:
//! - `GRAPH_PATH`: Path to the compressed Software Heritage graph (without extension)
//! - `RESULTS_PATH`: Directory path where analysis results are stored
//!
//! ## Output
//!
//! - Log files in `logs/` directory with rotation
//! - Modified files analysis in CSV format
//! - Database tables for persistent storage and querying

use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::{info, LevelFilter};
use std::{path::PathBuf, time::Instant};
use swh_graph::{graph::SwhBidirectionalGraph, mph::DynMphf, SwhGraphProperties};
use anyhow::Result;
use altered_history_analysis::models;

const GRAPH_PATH: &str = "";// /path/to/compressed/graph --> keep the name of the graph (often graph) without any extension
const RESULTS_PATH: &str = "";// /path/to/results/directory

/// Main application entry point for altered history analysis.
///
/// This async function orchestrates the complete workflow for analyzing altered file histories:
///
/// 1. **Logging Setup**: Configures rotating file logs with error duplication to stderr
/// 2. **Graph Loading**: Loads the Software Heritage bidirectional graph with all properties:
///    - Node-to-ID mappings
///    - Label names for edge metadata
///    - String properties for textual data
///    - Person information for authorship
///    - Timestamp data for temporal analysis
/// 3. **File Analysis**: Processes modified files data through the graph
/// 4. **Database Storage**: Converts results to PostgreSQL tables
///
/// # Returns
///
/// * `Result<()>` - Success or error result of the complete analysis workflow
///
/// # Panics
///
/// Will panic if:
/// - Graph files cannot be loaded from `GRAPH_PATH`
/// - Required graph properties cannot be initialized
/// - Critical logging setup fails
///
/// # Performance
///
/// Execution time is measured and logged. Large graphs may require significant memory
/// and processing time for the initial loading phase.
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
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        .duplicate_to_stderr(Duplicate::Error) // Duplicate logs to stderr
        .start()
        .unwrap();

    let graph_t = SwhBidirectionalGraph::new(PathBuf::from(
        GRAPH_PATH,
    ))
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

    altered_history_analysis::all_modified(
        altered_history_analysis::retrieve_file_modified(
            &format!("{}/focus/classes", RESULTS_PATH),
        )
        .unwrap(),
        &graph_t,
    );

    info!("Time elapsed: {:.2?}", start.elapsed());

    models::convert_modified_files("../data/modified_files.csv").await?;
    
    let directory_path = format!("{}/focus/classes", RESULTS_PATH);
    models::convert_altered_histories(&directory_path).await?;

    Ok(())
}
