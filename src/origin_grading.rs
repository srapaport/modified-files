use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use chashmap::CHashMap;
use indicatif::{ProgressBar, ProgressStyle};
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
) -> CHashMap<String, env::Stats>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let res = CHashMap::new();
    let no_timestamps_ori = AtomicUsize::new(0);
    let no_timestamps_rev = AtomicUsize::new(0);
    let no_readable_msg_ori = AtomicUsize::new(0);

    let bar = ProgressBar::new(310_334_314);
    //let bar = ProgressBar::new(443);
    bar.set_style(
        ProgressStyle::with_template(
            "{wide_bar} {pos} {percent_precise}% {elapsed_precise} {eta}",
        )
        .unwrap(),
    );

    (0..graph.num_nodes())
        .into_par_iter()
        .filter(|&node| graph.properties().node_type(node) == NodeType::Origin)
        .for_each(|ori| {
            let mut stats = env::Stats::default();
            let mut first_snap = None;
            let mut last_snap = None;
            graph
                .labeled_successors(ori)
                .into_iter()
                .for_each(|(_, labels)| {
                    stats.amount_snap += 1;
                    labels.into_iter().for_each(|label| {
                        if let EdgeLabel::Visit(visit) = label {
                            if visit.status() == VisitStatus::Full {
                                let ts = visit.timestamp();
                                first_snap = Some(std::cmp::min(ts, match first_snap{
                                    Some(v) => v,
                                    None => ts,
                                }));
                                last_snap = Some(std::cmp::max(ts, match last_snap{
                                    Some(v) => v,
                                    None => ts,
                                }));
                            }
                        }
                    });
                });
            if first_snap != None && last_snap != None {
                let first_snap= first_snap.unwrap();
                let last_snap = last_snap.unwrap();
                let mut contrib = HashSet::new();
                let mut contrib_aut = HashSet::new();
                let mut contrib_com = HashSet::new();
                let mut to_visit = vec![ori];
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
                                if let Some(aut_id) = graph.properties().author_id(succ){
                                    contrib.insert(aut_id);
                                    contrib_aut.insert(aut_id);
                                }
                                if let Some(com_id) = graph.properties().committer_id(succ){
                                    contrib.insert(com_id);
                                    contrib_com.insert(com_id);
                                }
                                stats.amount_rel += 1;
                                to_visit.push(succ);
                            },
                            NodeType::Revision => {
                                if let Some(aut_id) = graph.properties().author_id(succ){
                                    contrib.insert(aut_id);
                                    contrib_aut.insert(aut_id);
                                }
                                if let Some(com_id) = graph.properties().committer_id(succ){
                                    contrib.insert(com_id);
                                    contrib_com.insert(com_id);
                                }
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
                stats.amount_contrib = contrib.into_iter().count();
                stats.amount_author = contrib_aut.into_iter().count();
                stats.amount_committer = contrib_com.into_iter().count();
                let duration = (last_snap - first_snap) / (60*60*24);
                stats.freq_rev = (stats.amount_rev as f64) / (duration as f64);
                stats.freq_snap = (stats.amount_snap as f64) / (duration as f64);
                if let Some(url) = graph.properties().message(ori){
                    if let Ok(url_str) = String::from_utf8(url){
                        res.insert(url_str, stats);
                    } else{
                        no_readable_msg_ori.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                no_timestamps_ori.fetch_add(1, Ordering::Relaxed);
            }
            bar.inc(1);
        });
    bar.finish_with_message("Done");
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
    println!(
        "amount of origin skipped because no readable url: {}",
        no_readable_msg_ori.load(Ordering::Relaxed)
    );
    info!(
        "amount of origin skipped because no readable url: {}",
        no_readable_msg_ori.load(Ordering::Relaxed)
    );
    res
}
