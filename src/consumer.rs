use std::{time::Duration, sync::atomic::Ordering, thread, io::Write };
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use crate::{Subargs, utils};


impl From<Subargs> for Consumer {
    fn from(value: Subargs) -> Self {
        match value {
            Subargs::Consume {
                brokers, topics, group, fallback_offset, fetch_min_bytes, max_partition_fetch_bytes, fetch_max_bytes
            } => {
                let mut consumer = Consumer::from_hosts(brokers.to_vec());

                for topic in &topics {
                    consumer = consumer.with_topic(topic.to_string())
                }

                let consumer = consumer
                    .with_fallback_offset(FetchOffset::from(fallback_offset))
                    .with_fetch_max_bytes_per_partition(max_partition_fetch_bytes)
                    .with_retry_max_bytes_limit(fetch_max_bytes)
                    .with_fetch_min_bytes(fetch_min_bytes)
                    .with_fetch_max_wait_time(Duration::from_secs(1))
                    .with_group(group.to_string())
                    .with_offset_storage(Some(GroupOffsetStorage::Kafka))
                    .create()
                    .expect("Failed to create kafka consumer");

                log::info!("created kafka consumer @ {:?} from topics {:?}", brokers, topics);
                consumer
            },
            Subargs::Produce { .. } => panic!("Tried to create consumer from producer arguments"),
        }
    }
}


pub fn run(max_errors: usize, args: Subargs) {
    let running = utils::get_running_bool();
    let mut consumer = Consumer::from(args);
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
                stdout.write_all(m.value).unwrap_or_else(|e| {
                    log::error!("encountered an error when writing to stdout: {e}. exiting gracefully...");
                    running.store(false, Ordering::Relaxed);
                });

                stdout.write_all(&[b'\n']).unwrap_or_else(|e| {
                    log::error!("encountered an error when writing to stdout: {e}. exiting gracefully...");
                    running.store(false, Ordering::Relaxed);
                });
            }
            if let Err(e) = consumer.consume_messageset(ms) {
                log::error!("error consuming message set: {:#?}", e);
                sequential_errors += 1;
            };
        }

        match consumer.commit_consumed() {
            Err(e) => {
                log::error!("error commiting offsets: {:#?}", e);
                sequential_errors += 1;
            },
            Ok(_) => sequential_errors = 0,
        };
    }
}
