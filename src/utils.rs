use std::{fmt::Display, sync::{atomic::{AtomicBool, Ordering}, Arc}};

use clap::ValueEnum;
use kafka::{consumer::FetchOffset, producer::RequiredAcks};

#[derive(Clone, ValueEnum, Debug)]
pub enum ClapFetchOffsetWrapper {
    Latest,
    Earliest,
}

impl From<ClapFetchOffsetWrapper> for FetchOffset {
    fn from(val: ClapFetchOffsetWrapper) -> Self {
        match val {
            ClapFetchOffsetWrapper::Latest => FetchOffset::Latest,
            ClapFetchOffsetWrapper::Earliest => FetchOffset::Earliest,
        }
    }
}

impl Display for ClapFetchOffsetWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = format!("{:?}", self);
        let out = out.trim().to_lowercase();
        write!(f, "{}", out)
    }
}

#[derive(Clone, ValueEnum, Debug)]
pub enum ClapRequiredAcksWrapper {
    None,
    One,
    All
}

impl From<ClapRequiredAcksWrapper> for RequiredAcks {
    fn from(val: ClapRequiredAcksWrapper) -> Self {
        match val {
            ClapRequiredAcksWrapper::None => RequiredAcks::None,
            ClapRequiredAcksWrapper::One => RequiredAcks::One,
            ClapRequiredAcksWrapper::All => RequiredAcks::All,
        }
    }
}

impl Display for ClapRequiredAcksWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = format!("{:?}", self);
        let out = out.trim().to_lowercase();
        write!(f, "{}", out)
    }
}

pub fn get_running_bool() -> Arc<AtomicBool> {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    running
}
