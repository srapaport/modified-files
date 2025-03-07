use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::{info, LevelFilter};
use std::{path::PathBuf, time::Instant};
use swh_graph::{
    graph::SwhBidirectionalGraph,
    mph::DynMphf,
    SwhGraphProperties,
};

fn main() {
    Logger::with(LevelFilter::Info)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory("logs")
                .basename("test")
                .suffix("log"),
        )
        .rotate(
            Criterion::Size(10_000_000), // Rotate when the file exceeds 10 MB
            Naming::Numbers,             // Use numbers for rotated files
            Cleanup::KeepLogFiles(5),    // Keep at most 5 log files
        )
        .duplicate_to_stderr(Duplicate::Warn) // Duplicate logs to stderr
        .start()
        .unwrap();

    let graph_t = SwhBidirectionalGraph::new(PathBuf::from(
            "/infres/ir800/rapaport/datasets/2024-08-23-popular-500-python/compressed/graph",
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
            "/home/infres/rapaport/altered-history/results_2024/focus/classes",
        ).unwrap(),
        &graph_t,);
    
    info!("Time elapsed: {:.2?}", start.elapsed());
}
