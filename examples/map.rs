use flexi_logger::{Cleanup, Criterion, Duplicate, Logger, Naming};
use log::LevelFilter;
use serde::Serialize;
use std::{path::PathBuf, time::Instant};
use swh_graph::{graph::SwhBidirectionalGraph, mph::DynMphf, SwhGraphProperties};

fn main() {
    Logger::with(LevelFilter::Debug)
        .log_to_file(
            flexi_logger::FileSpec::default()
                // .directory("/infres/ir800/rapaport/results/FULL_new/logs")
                // .basename("FULL-all")
                .directory("logs")
                .basename("test-single-modified")
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
        //"/infres/ir800/rapaport/datasets/2024-08-23-popular-500-python/compressed/graph",
        "/poolswh/softwareheritage/graph/2024-08-23/compressed/graph",
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

    // let content_modified = altered_history_analysis::retrieve_file_modified("/home/infres/rapaport/altered-history/results_2024/focus/classes").unwrap();

    // let first_entry = content_modified.into_iter().next().expect("Map is empty");
    // let (k, v) = (first_entry.0, first_entry.1);
    // let line = v.first().unwrap();
    // info!("url: {} | line: {:?}", k, line);

    //let mut csv_wrt = csv::WriterBuilder::new().from_path("test2.csv").unwrap();
    /* #[derive(Serialize)]
    struct Row{
        path: String,
        node: usize
    }
    altered_history_analysis::file_modified::map_commit(
        ("swh:1:snp:08d348ea5cdc0c37e8371f2df48fab78d3460163".to_string(), "refs/pull/1599/head".to_string(), "swh:1:rev:05284f7caf6a62f2b86ed3eb275e291372df5222".to_string(), "swh:1:snp:2ee8649b5215c25e0c2c4a575ab21772d41c1ec2".to_string()),
        &graph_t
    ).unwrap().into_iter().for_each(|(k, v)|{
        csv_wrt.serialize(Row{
            path: k,
            node: v
        }).unwrap()
    });
    csv_wrt.flush().unwrap(); */
    altered_history_analysis::single_modified(
        ("swh:1:snp:08d348ea5cdc0c37e8371f2df48fab78d3460163".to_string(), "refs/pull/1599/head".to_string(), "swh:1:rev:05284f7caf6a62f2b86ed3eb275e291372df5222".to_string(), "swh:1:snp:2ee8649b5215c25e0c2c4a575ab21772d41c1ec2".to_string()),
        &graph_t
    );

    //altered_history_analysis::all_grade(&graph_t);

    println!("time elapsed: {:.2?}", start.elapsed());
}
