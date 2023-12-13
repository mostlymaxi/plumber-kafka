use std::fmt::Display;
use std::io::{Write, BufRead};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use clap::{Parser, ValueEnum};
use kafka::producer::{Producer, RequiredAcks, Record};

mod utils;

/// unix pipelines made easy!
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
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
enum Subargs {
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

fn consume(
    max_errors: usize,
    topics: &Vec<String>,
    brokers: &Vec<String>,
    group: &String,
    fetch_offset: FetchOffset,
    fetch_min_bytes: i32,
    max_partition_fetch_bytes: i32,
    fetch_max_bytes: i32,
    ) {

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let mut consumer = Consumer::from_hosts(brokers.to_vec());

    for topic in topics {
        consumer = consumer.with_topic(topic.to_string())
    }

    let mut consumer = consumer
        .with_fallback_offset(fetch_offset)
        .with_fetch_max_bytes_per_partition(max_partition_fetch_bytes)
        .with_retry_max_bytes_limit(fetch_max_bytes)
        .with_fetch_min_bytes(fetch_min_bytes)
        .with_fetch_max_wait_time(Duration::from_secs(1))
        .with_group(group.to_string())
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .expect("Failed to create kafka consumer");

    log::info!("created kafka consumer @ {:?} from topics {:?}", brokers, topics);


    let mut sequential_errors: usize = 0;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    while running.load(Ordering::Relaxed) {
        match sequential_errors.cmp(&max_errors) {
            std::cmp::Ordering::Less => {},
            std::cmp::Ordering::Equal => { log::error!("hit max errors: {:?}", max_errors); break; },
            std::cmp::Ordering::Greater => { log::error!("hit max errors: {:?}", max_errors); break; },
        };

        let Ok(ms) = consumer.poll() else {
            thread::sleep(Duration::from_millis(500));
            log::trace!("{:#?}", consumer.poll());
            log::trace!("polling...");
            continue;
        };

        if ms.is_empty() {
            thread::sleep(Duration::from_millis(500));
            log::trace!("empty... polling...");
            continue;
        }

        for ms in ms.iter() {
            for m in ms.messages() {
                if let Err(e) = stdout.write_all(m.value) {
                    log::error!("{:#?}", e);
                    sequential_errors += 1;
                } else {
                    let _ = stdout.write_all(&[b'\n']);
                }
            }
            if let Err(e) = consumer.consume_messageset(ms) {
                log::error!("{:#?}", e);
                sequential_errors += 1;
            };
        }

        match consumer.commit_consumed() {
            Err(e) => {
                log::error!("{:#?}", e);
                sequential_errors += 1;
            },
            Ok(_) => sequential_errors = 0,
        };
    }
}

fn produce(topic: &String, brokers: &Vec<String>, required_acks: RequiredAcks) {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let mut producer = Producer::from_hosts(brokers.to_vec())
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(required_acks)
        .create()
        .unwrap();

    log::info!("created kafka producer @ {:?} to topic {:?}", brokers, topic);

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    // user should be allowed to change these values:
    const BUF_LENGTH: usize = 8192 * 10;
    const REASONABLE_AMOUNT_OF_BYTES: usize = BUF_LENGTH - 8192;
    let mut buf = Vec::with_capacity(BUF_LENGTH);

    while running.load(Ordering::Relaxed) {
        let mut num_bytes = 0;
        while num_bytes < REASONABLE_AMOUNT_OF_BYTES {
            let read_bytes = stdin.read_until(0xA, &mut buf).unwrap();
            num_bytes += read_bytes;

            if read_bytes == 0 {
                log::info!("reached EOF... exiting");
                running.store(false, Ordering::Relaxed);
                break
            }
        }

        if buf.is_empty() { continue }

        let recs: Vec<Record<'_, (), String>> = buf.lines()
            .map(|t| t.unwrap())
            .filter(|s| !s.is_empty())
            .map(|s| Record::from_value(topic, s))
            .collect();

        log::trace!("{:#?}", recs);

        if recs.is_empty() {
            log::error!("no lines in buffer. this is not supposed to happen... exiting");
            break;
        }

        let _ = producer.send_all(&recs).unwrap();

        buf.clear()
    }


}

fn main() {
    let args = Args::parse();
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    match &args.command {
        Subargs::Consume {
            topics,
            group,
            fallback_offset,
            brokers,
            fetch_min_bytes,
            max_partition_fetch_bytes,
            fetch_max_bytes } => {
                consume(
                    args.max_errors,
                    topics,
                    brokers,
                    group,
                    fallback_offset.into(),
                    *fetch_min_bytes,
                    *max_partition_fetch_bytes,
                    *fetch_max_bytes,
                );
            },
        Subargs::Produce {
            topic,
            brokers,
            required_acks, } => {
            produce(topic, brokers, required_acks.into());
        }
    };
}
