use std::sync::atomic::AtomicUsize;

use serde::Serialize;

#[derive(Default, Serialize, PartialEq)]
pub enum Status {
    #[default]
    NotFound,
    Modified,
    Found,
}

#[derive(Serialize)]
pub struct Row {
    pub origin: String,
    pub revision: String,
    pub branch: String,
    pub snapshot_without: String,
    pub path: String,
    pub status: Status,
}

#[derive(Serialize, Default)]
pub struct Stats {
    pub amount_contrib: usize,
    pub amount_snap: usize,
    pub amount_rel: usize,
    pub amount_rev: usize,
    pub freq_snap: f64,
    pub freq_rev: f64,
}

pub static ERR_BRANCH: AtomicUsize = AtomicUsize::new(0);
