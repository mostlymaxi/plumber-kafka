use std::{time::Duration, sync::atomic::Ordering, thread, io::Write };
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage, MessageSet};

use crate::{Subargs, utils};

trait DumpMessageSet {
    fn dump_messageset<T: Write>(&self, msgs: &MessageSet, f: T) -> Result<(), std::io::Error>;
}

impl DumpMessageSet for Consumer {
    fn dump_messageset<T: Write>(&self, msgs: &MessageSet, mut f: T) -> Result<(), std::io::Error> {
        for m in msgs.messages() {
            f.write_all(m.value)?;
            // unwrap_or_else(|e| {
            //     log::error!("encountered an error when dumping message set: {e}. exiting gracefully...");
            //     running.store(false, Ordering::Relaxed);
            // });

            f.write_all(&[b'\n'])?
            // .unwrap_or_else(|e| {
            //     log::error!("encountered an error when dumping message set: {e}. exiting gracefully...");
            //     running.store(false, Ordering::Relaxed);
            // });
        }

        Ok(())
    }
}

// this actually can't fail
// idk i need to think about how to better implement this
impl TryFrom<Subargs> for Consumer {
    type Error = kafka::Error;

    fn try_from(value: Subargs) -> Result<Self, Self::Error> {
        match value {
            Subargs::Consume {
                    brokers,
                    topics,
                    group,
                    fallback_offset,
                    fetch_min_bytes,
                    max_partition_fetch_bytes,
                    fetch_max_bytes } => {

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
                Ok(consumer)
            },
            Subargs::Produce { .. } => panic!("Tried to create consumer from producer arguments"),
        }
    }
}


pub fn run(max_errors: usize, args: Subargs) {
    let running = utils::get_running_bool();
    let mut consumer = Consumer::try_from(args).unwrap();
    let mut sequential_errors: usize = 0;

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    while running.load(Ordering::Relaxed) {
        match sequential_errors.cmp(&max_errors) {
            std::cmp::Ordering::Less => {},
            std::cmp::Ordering::Equal => { log::error!("hit max errors: {:?}", max_errors); break; },
            std::cmp::Ordering::Greater => { log::error!("hit max errors: {:?}", max_errors); break; },
        };

        let ms = match consumer.poll() {
            Ok(ms) => ms,
            Err(e) => {
                log::error!("error polling: {:?}, backing off for 5 seconds and trying again", e);
                thread::sleep(Duration::from_secs(5));
                sequential_errors += 1;
                continue
            }
        };

        if ms.is_empty() {
            thread::sleep(Duration::from_millis(500));
            log::trace!("empty... polling...");
            sequential_errors = 0;
            continue;
        }

        for ms in ms.iter() {
            if let Err(e) = consumer.dump_messageset(&ms, &mut stdout) {
                log::error!("encountered an error when writing to stdout: {e}. exiting gracefully...");
                running.store(false, Ordering::Relaxed);
            };

            if let Err(e) = consumer.consume_messageset(ms) {
                log::error!("error consuming message set: {:#?}", e);
                sequential_errors += 1;
            };
        }

        if let Err(e) = consumer.commit_consumed() {
            log::error!("error commiting offsets: {:#?}", e);
            sequential_errors += 1;
        } else {
            sequential_errors = 0;
        }
    }
}
