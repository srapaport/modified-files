use std::collections::{HashMap, HashSet, VecDeque};

use swh_graph::graph::*;
use swh_graph::labels::EdgeLabel;
use swh_graph::NodeType;

use log::{debug, error};

use crate::env;

pub fn get_dir<G: SwhLabeledForwardGraph + SwhGraphWithProperties>(
    rev_swhid: &str,
    graph: &G,
) -> Option<usize>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
{
    let node_id = graph.properties().node_id(rev_swhid).unwrap();
    for succ in graph.successors(node_id) {
        if graph.properties().node_type(succ) == NodeType::Directory {
            return Some(succ);
        }
    }
    None
}

/// return the list of all the content node id associated with their path in the given dir
pub fn get_list_of_content<G: SwhLabeledForwardGraph + SwhGraphWithProperties>(
    dir: usize,
    graph: &G,
) -> HashMap<String, usize>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    // map returned by the function <path of content, node_id>
    let mut res = HashMap::new();

    // map with <node id, path> to build the final path of each content node
    let mut path_node: HashMap<usize, String> = HashMap::new();
    path_node.insert(dir, String::from("."));

    let mut to_visit = VecDeque::new();
    to_visit.push_back(dir);
    let mut visited = HashSet::new();
    while let Some(node) = to_visit.pop_front() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node);
        let successors = graph.labeled_successors(node);
        for (succ, labels) in successors {
            for label in labels {
                let name: String;
                if let EdgeLabel::DirEntry(dir) = label {
                    name =
                        String::from_utf8_lossy(&graph.properties().label_name(dir.filename_id()))
                            .to_string();
                } else {
                    // We just care about labels that are giving the path
                    continue;
                }
                let path = format!(
                    "{}/{}",
                    path_node
                        .get(&node)
                        .expect("couldn't find path in path_node"),
                    name
                );
                match graph.properties().node_type(succ) {
                    NodeType::Content => {
                        res.insert(path, succ);
                    }
                    NodeType::Directory => {
                        path_node.insert(succ, path);
                        to_visit.push_front(succ);
                    }
                    _ => continue,
                }
            }
        }
    }
    res
}

pub fn compare_paths<G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph>(
    snap_dst: &str,
    branch_name: &str,
    paths: &HashMap<String, usize>,
    graph_t: &G,
) -> Option<HashMap<String, env::Status>>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let mut res = HashMap::new();
    paths.iter().for_each(|(path, _)| {
        res.insert(path.clone(), env::Status::default());
    });

    let Some(rev_id) = get_rev(snap_dst, branch_name, graph_t) else {
        return None;
    };
    let Some(dir) = get_dir(&graph_t.properties().swhid(rev_id).to_string(), graph_t) else {
        return None;
    };
    let mut path_node: HashMap<usize, String> = HashMap::new();
    path_node.insert(dir, String::from("."));

    let mut to_visit = VecDeque::new();
    to_visit.push_back(dir);
    let mut visited = HashSet::new();
    while let Some(node) = to_visit.pop_front() {
        if visited.contains(&node) {
            continue;
        }
        visited.insert(node);
        let successors = graph_t.labeled_successors(node);
        for (succ, labels) in successors {
            for label in labels {
                let name: String;
                if let EdgeLabel::DirEntry(dir) = label {
                    name = String::from_utf8_lossy(
                        &graph_t.properties().label_name(dir.filename_id()),
                    )
                    .to_string();
                } else {
                    // We just care about labels that are giving the path
                    continue;
                }
                let path = format!(
                    "{}/{}",
                    path_node
                        .get(&node)
                        .expect("couldn't find path in path_node"),
                    name
                );
                match graph_t.properties().node_type(succ) {
                    NodeType::Content => {
                        debug!("found a file: {}", path);
                        if let Some(node_path) = paths.get(&path) {
                            if *node_path == succ {
                                res.insert(path, env::Status::Found);
                            } else {
                                res.insert(path, env::Status::Modified);
                            }
                        }
                    }
                    NodeType::Directory => {
                        path_node.insert(succ, path);
                        to_visit.push_front(succ);
                    }
                    _ => continue,
                }
            }
        }
    }
    Some(res)
}

fn get_rev<G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph>(
    snap_dst: &str,
    branch_name: &str,
    graph_t: &G,
) -> Option<usize>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let props = graph_t.properties();
    let snap_id = props
        .node_id(snap_dst)
        .expect(&format!("couldn't find {} in the graph", snap_dst));

    for (succ, labels) in graph_t.labeled_successors(snap_id) {
        for label in labels {
            let curr_branch: String;
            if let EdgeLabel::Branch(b) = label {
                curr_branch = String::from_utf8(props.label_name(b.filename_id())).expect(
                    &format!("couldn't convert message in string for node {}", succ),
                );
            } else {
                continue;
            }
            if &curr_branch == branch_name {
                return Some(succ);
            }
        }
    }
    None
}

pub fn map_commit<G: SwhLabeledForwardGraph + SwhGraphWithProperties + SwhLabeledBackwardGraph>(
    line: (String, String, String, String),
    graph_t: &G,
) -> Option<HashMap<String, usize>>
where
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    if let Some(dir) = get_dir(&line.2, graph_t) {
        return Some(get_list_of_content(dir, graph_t));
    } else {
        error!("No dir found for rev: {}", line.2);
    }
    None
}
