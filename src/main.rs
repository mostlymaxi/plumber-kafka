
use std::io::{Write, BufRead};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use clap::Parser;
use kafka::producer::{Producer, RequiredAcks, Record};

mod utils;
mod consumer;
mod producer;


/// unix pipelines made easy!
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[command(subcommand)]
    command: Subargs,
    /// maximum consecutive errors
    ///
    /// This represents the number of allowed consecutive message errors before halting.
    ///
    /// Errors may be caused by failure to commit message offset or being unable to write to stdout.
    ///
    /// Setting this to 0 will cause the program to ignore all errored messages.
    #[arg(short, long, default_value_t = 2)]
    max_errors: usize,
}

#[derive(clap::Subcommand)]
pub enum Subargs {
    /// consume from kafka topic
    Consume {
        /// list of kafka brokers
        #[arg(short, long, required = true)]
        brokers: Vec<String>,
        /// topics to consume from
        #[arg(short, long, required = true)]
        topics: Vec<String>,
        /// consumer group to use
        #[arg(short, long, default_value = "")]
        group: String,
        /// starting and fallback offset
        #[arg(long, default_value_t = utils::ClapFetchOffsetWrapper::Earliest)]
        fallback_offset: utils::ClapFetchOffsetWrapper,
        /// fetch.min.bytes
        #[arg(long, default_value_t = 1)]
        fetch_min_bytes: i32,
        /// max.partition.fetch.bytes
        #[arg(long, default_value_t = 1048576)]
        max_partition_fetch_bytes: i32,
        /// fetch.max.bytes
        #[arg(long, default_value_t = 52428800)]
        fetch_max_bytes: i32,
    },
    /// produce to kafka topic
    Produce {
        /// list of kafka brokers
        #[arg(short, long, required = true)]
        brokers: Vec<String>,
        /// topic to produce to
        #[arg(short, long)]
        topic: String,
        /// required acks
        #[arg(long, default_value_t = utils::ClapRequiredAcksWrapper::None)]
        required_acks: utils::ClapRequiredAcksWrapper,

    },
}

fn main() {
    let args = Args::parse();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    match &args.command {
        Subargs::Consume { .. } => {
                consumer::run(
                    args.max_errors,
                    args.command
                );
            },
        Subargs::Produce {
            topic,
            brokers,
            required_acks, } => {
            producer::run(topic, brokers, required_acks.into());
        }
    };
}
