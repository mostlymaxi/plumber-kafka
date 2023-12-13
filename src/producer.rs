use std::{time::Duration, sync::atomic::Ordering, io::BufRead};
use kafka::producer::{Producer, RequiredAcks, Record};

use crate::utils;


pub fn run(topic: &String, brokers: &Vec<String>, required_acks: RequiredAcks) {
    let running = utils::get_running_bool();

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
