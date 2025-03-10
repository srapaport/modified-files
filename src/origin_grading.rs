use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicUsize, Ordering},
};

use log::info;
use rayon::prelude::*;
use swh_graph::{
    graph::*,
    labels::{EdgeLabel, VisitStatus},
    NodeType,
};

use crate::env;

pub fn grades<G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph + Sync>(
    graph: &G,
) -> HashMap<String, env::Stats>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut res = HashMap::new();
    let no_timestamps_ori = AtomicUsize::new(0);
    let no_timestamps_rev = AtomicUsize::new(0);

    (0..graph.num_nodes())
        .into_par_iter()
        .filter(|&node| graph.properties().node_type(node) == NodeType::Origin)
        .for_each(|node| {
            let mut stats = env::Stats::default();
            let mut first_snap = 0;
            let mut last_snap = 0;
            graph
                .labeled_predecessors(node)
                .into_iter()
                .for_each(|(_, labels)| {
                    stats.amount_snap += 1;
                    labels.into_iter().for_each(|label| {
                        if let EdgeLabel::Visit(visit) = label {
                            if visit.status() == VisitStatus::Full {
                                let ts = visit.timestamp();
                                first_snap = std::cmp::min(ts, first_snap);
                                last_snap = std::cmp::max(ts, last_snap);
                            }
                        }
                    });
                });
            if first_snap != 0 && last_snap != 0 {
                let mut to_visit = vec![node];
                let mut visited = HashSet::new();
                while let Some(node) = to_visit.pop() {
                    if visited.contains(&node) {
                        continue;
                    }
                    visited.insert(node);
                    graph.successors(node).into_iter().for_each(|succ|{
                        match graph.properties().node_type(succ) {
                            NodeType::Snapshot => to_visit.push(succ),
                            NodeType::Release => {
                                stats.amount_rel += 1;
                                to_visit.push(succ);
                            },
                            NodeType::Revision => {
                                if let Some(ts) = graph.properties().committer_timestamp(succ){
                                    if ts as u64 >= first_snap{
                                        stats.amount_rev += 1;
                                    }
                                } else {
                                    no_timestamps_rev.fetch_add(1, Ordering::Relaxed);
                                }
                                to_visit.push(succ);
                            },
                            _ => (),
                        }
                    });
                }
            } else {
                no_timestamps_ori.fetch_add(1, Ordering::Relaxed);
            }
            todo!("calculate freq for snap and rev");
        });
    println!(
        "amount of origin skipped because no timestamps: {}",
        no_timestamps_ori.load(Ordering::Relaxed)
    );
    info!(
        "amount of origin skipped because no timestamps: {}",
        no_timestamps_ori.load(Ordering::Relaxed)
    );
    println!(
        "amount of rev or rel skipped because no timestamps: {}",
        no_timestamps_rev.load(Ordering::Relaxed)
    );
    info!(
        "amount of rev or rel skipped because no timestamps: {}",
        no_timestamps_rev.load(Ordering::Relaxed)
    );
    res
}
