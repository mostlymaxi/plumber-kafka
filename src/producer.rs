use std::{time::Duration, sync::atomic::Ordering, io::BufRead};
use kafka::producer::{Producer, Record};

use crate::{utils, Subargs};

// this actually can't fail
// idk i need to think about how to better implement this
impl TryFrom<Subargs> for Producer {
    type Error = kafka::Error;

    fn try_from(value: Subargs) -> Result<Self, Self::Error> {
        match value {
            Subargs::Consume { .. } => panic!("Tried to create consumer from producer arguments"),
            Subargs::Produce { brokers, topic, required_acks } => {
                let producer = Producer::from_hosts(brokers.clone())
                    .with_ack_timeout(Duration::from_secs(1))
                    .with_required_acks(required_acks.into())
                    .create()?;

                log::info!("created kafka producer @ {:?} to topic {:?}", brokers, topic);
                Ok(producer)
            },
        }
    }
}

pub fn run(_max_errors: usize, args: Subargs) {
    let running = utils::get_running_bool();

    let topic = match &args {
        Subargs::Consume { .. } => panic!("Tried to create consumer from producer arguments"),
        Subargs::Produce { topic, ..} => topic.clone(),
    };

    let mut producer = match Producer::try_from(args) {
        Ok(p) => p,
        Err(e) => {
            log::error!("failed to create producer: {:?}", e);
            return;
        },
    };

    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();

    // user should be allowed to change these values:
    const BUF_LENGTH: usize = 8192 * 10;
    const REASONABLE_AMOUNT_OF_BYTES: usize = BUF_LENGTH - 8192;
    let mut buf = Vec::with_capacity(BUF_LENGTH);

    let mut count: u64 = 0;

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
            .map(|s| Record::from_value(&topic, s))
            .collect();

        log::trace!("{:#?}", recs);

        if recs.is_empty() {
            log::error!("no lines in buffer. this is not supposed to happen... exiting");
            running.store(false, Ordering::Relaxed);
            break;
        }

        let _ = producer.send_all(&recs).unwrap();

        count += recs.len() as u64;
        buf.clear()
    }

    log::info!("produced {count} lines");
}
