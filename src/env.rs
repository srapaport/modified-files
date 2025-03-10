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

pub static ERR_BRANCH: AtomicUsize = AtomicUsize::new(0);