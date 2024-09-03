use std::error::Error;
use rand::{thread_rng, Rng};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, process};
use vroom::memory::*;
use vroom::QUEUE_LENGTH;
use vroom::nonseq::ZNSTarget;
use vroom::nonseq::VictimSelectionMethod;

pub fn main() -> Result<(), Box<dyn Error>> {

    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example nonseq_test <pci bus id>");
            process::exit(1);
        }
    };

    let duration = match args.next() {
        Some(secs) => Some(Duration::from_secs(secs.parse().expect(
            "Usage: cargo run --example init <pci bus id> <duration in seconds>",
        ))),
        None => None,
    };

    let nvme = vroom::init(&pci_addr)?;
    let ns_id = 2;
    let mut znstarget = vroom::nonseq::ZNSTarget::init(nvme, ns_id, 0.3, VictimSelectionMethod::InvalidBlocks)?;

    znstarget.backing.zone_action(ns_id, 0, true, vroom::ZnsZsa::ResetZone)?;

    //let znstarget = qd1(znstarget, ns_id, 1, true, false, duration)?;
    //qd1(znstarget, ns_id, 1, false, false, duration)?;
    test_concurrent(znstarget, 1)?;

    Ok(())

}

pub fn test(mut znstarget: ZNSTarget) -> Result<(), Box<dyn Error>> {

    const N_BLOCKS : usize = 15872;
    let src1 = vec!('a' as u8; N_BLOCKS);
    let src2 = ['b' as u8; N_BLOCKS];
    znstarget.write_copied(&src1, 0)?;
    znstarget.write_copied(&src2, 1)?;
    znstarget.write_copied(&src1, 50000)?;
    znstarget.write_copied(&src2, 0)?;
    znstarget.write_copied(&src1, 0)?;
    znstarget.write_copied(&src2, 0)?;

    let mut dest = [0u8; 10];
    znstarget.read_copied(&mut dest, 0)?;
    znstarget.backing.get_zone_reports(1)?;
    println!("{}", std::str::from_utf8(&dest)?);
    Ok(())
}

pub fn test_concurrent(mut znstarget: ZNSTarget, n_threads: u8) -> Result<(), Box<dyn Error>> {

    let mut queue_pairs = Vec::new();

    for _ in 0..n_threads {
        let qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH)?;
        queue_pairs.push(qpair);
    }

    let mut threads = Vec::new();

    let queue_pairs = Arc::new(Mutex::new(queue_pairs));

    let mut reclaim_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH)?;

    let znstarget = Arc::new(znstarget);
    let znstarget_reclaim = znstarget.clone();
    let reclaim_thread = std::thread::spawn(move || {
        loop {
            let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            println!("Condition is {}", condition);
            if condition {
                break;
            }
            let _ = znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer);
        }
    });

    for i in 0..(n_threads as u64) {
        let znstarget = znstarget.clone();
        let queue_pairs = queue_pairs.clone();

        let handle = std::thread::spawn(move || {

            let range = (i * 1000)..((i + 1) * 1000);
            let mut rng = thread_rng();
            let bytes = 4096 * 16;
            let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();

            let mut qpair = queue_pairs.lock().unwrap().pop().unwrap();

            let rand_block = &(0..(bytes))
                .map(|_| rand::random::<u8>())
                .collect::<Vec<_>>()[..];
            buffer[0..bytes].copy_from_slice(rand_block);
            let lba = rng.gen_range(range);

            let _ = znstarget.write_concurrent(&mut qpair ,&buffer.slice(0..bytes), lba);
            println!("Thread {} is finished :)", i)
        });
        threads.push(handle);
    }

    for handle in threads {
        handle.join().unwrap();
    }

    println!("Joined threads");

    znstarget.stop_reclaim();
    reclaim_thread.join().unwrap();

    println!("Joined reclaim thread");

    let mut znstarget = Arc::try_unwrap(znstarget).unwrap_or_else(|_| panic!("This legit can't happen"));

    znstarget.backing.get_zone_reports(2)?;

    Ok(())
}

fn qd1(
    mut znstarget: ZNSTarget,
    ns_id: u32,
    n: u64,
    write: bool,
    random: bool,
    time: Option<Duration>,
) -> Result<ZNSTarget, Box<dyn Error>> {
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE)?;

    const N_BLOCKS : u64 = 15782 * 40;

    let ns = znstarget.backing.namespaces.get(&ns_id).unwrap();
    let blocks = 8; // Blocks that will be read/written at a time
    let bytes = blocks * ns.block_size;
    //let ns_blocks = ns.blocks / blocks - 1; // - blocks - 1;
    let ns_blocks = N_BLOCKS / blocks - 1;

    let mut rng = thread_rng();
    let seq = if random {
        (0..n)
            .map(|_| rng.gen_range(0..ns_blocks as u64))
            .collect::<Vec<u64>>()
    } else {
        (0..n).map(|i| (i * 8) % ns_blocks).collect::<Vec<u64>>()
    };

    let rand_block = &(0..bytes).map(|_| rand::random::<u8>()).collect::<Vec<_>>()[..];
    buffer[..rand_block.len()].copy_from_slice(rand_block);

    let mut total = Duration::ZERO;

    if let Some(time) = time {
        let mut ios = 0;
        let lba = 0;
        while total < time {
            let lba = if random { rng.gen_range(0..ns_blocks) } else { (lba + 1) % ns_blocks };

            let before = Instant::now();
            if write {
                znstarget.write(&buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                znstarget.read(&buffer.slice(0..bytes as usize), lba * blocks)?;
            }
            let elapsed = before.elapsed();
            total += elapsed;
            ios += 1;
        }
        println!(
            "IOP: {ios}, total {} iops: {:?}",
            if write { "write" } else { "read" },
            ios as f64 / total.as_secs_f64()
        );
    } else {
        for lba in seq {
            let before = Instant::now();
            if write {
                znstarget.write(&buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                znstarget.read(&buffer.slice(0..bytes as usize), lba * blocks)?;
            }
            total += before.elapsed();
        }
        println!(
            "n: {n}, total {} iops: {:?}",
            if write { "write" } else { "read" },
            n as f64 / total.as_secs_f64()
        );
    }
    Ok(znstarget)
}