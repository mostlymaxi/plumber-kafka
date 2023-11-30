use std::fmt::Display;
use std::io::{BufWriter, Write, BufReader, BufRead, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, fs};
use std::time::Duration;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use clap::{Parser, ValueEnum};
use kafka::producer::{Producer, RequiredAcks, Record};

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

#[derive(Clone, ValueEnum, Debug)]
enum PointlessFetchOffsetWrapper {
    Latest,
    Earliest,
}

impl Into<FetchOffset> for &PointlessFetchOffsetWrapper {
    fn into(self) -> FetchOffset {
        match self {
            PointlessFetchOffsetWrapper::Latest => FetchOffset::Latest,
            PointlessFetchOffsetWrapper::Earliest => FetchOffset::Earliest,
        }
    }
}

impl Display for PointlessFetchOffsetWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let out = format!("{:?}", self);
        let out = out.trim().to_lowercase();
        write!(f, "{}", out)
    }
}

#[derive(clap::Subcommand)]
enum Subargs {
    /// consume from kafka topic
    Consume {
        /// list of kafka brokers
        #[arg(short, long, required = true)]
        brokers: Vec<String>,
        /// topics to consume from
        #[arg(short, long, num_args = 1..)]
        topics: Vec<String>,
        /// consumer group to use
        #[arg(short, long, default_value = "")]
        group: String,
        /// starting offset
        #[arg(short, long, default_value_t = PointlessFetchOffsetWrapper::Earliest)]
        fallback_offset: PointlessFetchOffsetWrapper
    },
    /// produce to kafka topic
    Produce {
        /// list of kafka brokers
        #[arg(short, long, required = true)]
        brokers: Vec<String>,
        /// topic to produce to
        #[arg(short, long)]
        topic: String,
    },
}

fn consume(
    max_errors: usize,
    topics: &Vec<String>,
    brokers: &Vec<String>,
    group: &String,
    fetch_offset: FetchOffset
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
        .with_group(group.to_string())
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .create()
        .expect("Failed to create kafka consumer");

    let mut sequential_errors: usize = 0;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    while running.load(Ordering::Relaxed) {
        match sequential_errors.cmp(&max_errors) {
            std::cmp::Ordering::Less => {},
            std::cmp::Ordering::Equal => break,
            std::cmp::Ordering::Greater => break,
        };

        let Ok(ms) = consumer.poll() else {
            thread::sleep(Duration::from_secs(1));
            continue;
        };

        for ms in ms.iter() {
            for m in ms.messages() {
                if let Err(e) = stdout.write_all(m.value) {
                    eprintln!("{:#?}", e);
                    sequential_errors += 1;
                }
            }
            if let Err(e) = consumer.consume_messageset(ms) {
                eprintln!("{:#?}", e);
                sequential_errors += 1;
            };
        }

        match consumer.commit_consumed() {
            Err(e) => {
                eprintln!("{:#?}", e);
                sequential_errors += 1;
            },
            Ok(_) => sequential_errors = 0,
        };
    }
}

fn produce(topic: &String, brokers: &Vec<String>) {
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    let mut producer = Producer::from_hosts(brokers.to_vec())
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::None)
        // ~ build the producer with the above settings
        .create()
        .unwrap();

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    const BUF_LENGTH: usize = 8192 * 10;
    const REASONABLE_AMOUNT_OF_BYTES: usize = BUF_LENGTH - 8192;
    let mut buf = Vec::with_capacity(BUF_LENGTH);

    while running.load(Ordering::Relaxed) {
        let mut num_bytes = 0;
        while num_bytes < REASONABLE_AMOUNT_OF_BYTES {
            let read_bytes = stdin.read_until(0xA, &mut buf).unwrap();
            num_bytes += read_bytes;

            if read_bytes == 0 {
                eprintln!("reached EOF");
                running.store(false, Ordering::Relaxed);
                break
            }
        }

        let recs: Vec<Record<'_, (), String>> = buf.lines()
            .map(|t| t.unwrap())
            .filter(|s| !s.is_empty())
            .map(|s| Record::from_value(topic, s))
            .collect();

        if recs.is_empty() {
            eprintln!("ping");
            thread::sleep(Duration::from_secs(3));
            continue;
        }

        let _ = producer.send_all(&recs).unwrap();

        buf.clear()
    }


}

fn main() {
    let args = Args::parse();

    match &args.command {
        Subargs::Consume{
            topics,
            group,
            fallback_offset, brokers } => {
                consume(
                    args.max_errors,
                    topics,
                    brokers,
                    group,
                    fallback_offset.into()
                );
            },
        Subargs::Produce { topic, brokers } => {
            produce(topic, brokers);
        }
    };
}
