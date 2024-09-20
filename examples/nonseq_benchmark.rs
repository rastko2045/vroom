use std::error::Error;
use rand::{thread_rng, Rng};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, process, thread};
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

    let mut znstarget = vroom::nonseq::ZNSTarget::init(nvme, ns_id, 0.9, VictimSelectionMethod::InvalidBlocks)?;

    //znstarget.backing.lock().unwrap().zone_action(ns_id, 0, true, vroom::ZnsZsa::ResetZone)?;

    //fill_target(&mut znstarget);

    znstarget.backing.lock().unwrap().get_zone_reports(ns_id)?;

    //let _ = qd_n(znstarget, ns_id, 4, 0, true, 32, duration)?;
    //let _ = qd1(znstarget, ns_id, 0, true, true, duration)?;

    Ok(())
}

#[allow(unused)]
fn qd_n(
    mut nvme: ZNSTarget,
    ns_id: u32,
    n_threads: u64,
    n: u64,
    write: bool,
    batch_size: usize,
    time: Option<Duration>,
) -> Result<ZNSTarget, Box<dyn Error>> {
    let blocks = 1;
    let mut backing = nvme.backing.get_mut().unwrap();
    let ns_blocks = backing.namespaces.get(&ns_id).unwrap().blocks / blocks;
    let block_size = backing.namespaces.get(&ns_id).unwrap().block_size;

    let max_lba = nvme.max_lba;

    let mut threads = Vec::new();

    let mut queue_pairs = Vec::new();

    for _ in 0..n_threads + 1 {
        let qpair = backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        queue_pairs.push(qpair);
    }


    let queue_pairs = Arc::new(Mutex::new(queue_pairs));    
    let nvme = Arc::new(nvme);

    let znstarget_reclaim = nvme.clone();
    let znstarget_queue_pair = queue_pairs.clone();
    let reclaim_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        let mut reclaim_qpair = znstarget_queue_pair.lock().unwrap().pop().unwrap();
        loop {
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            if condition {
                break;
            }
            znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer).unwrap();
        }
        znstarget_reclaim.backing.lock().unwrap().delete_io_queue_pair(reclaim_qpair).unwrap();
    });

    for i in 0..n_threads {
        let nvme = Arc::clone(&nvme);
        let queue_pairs = queue_pairs.clone();
        let range = (0, max_lba);

        let handle = thread::spawn(move || -> (u64, f64) {
            let mut rng = rand::thread_rng();
            let bytes = (block_size * blocks) as usize;
            let mut total = std::time::Duration::ZERO;
            let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();

            let mut qpair = queue_pairs.lock().unwrap().pop().unwrap();

            let rand_block = &(0..(32 * bytes))
                .map(|_| rand::random::<u8>())
                .collect::<Vec<_>>()[..];
            buffer[0..32 * bytes].copy_from_slice(rand_block);

            let mut ctr = 0;
            if let Some(time) = time {
                let mut ios = 0;
                while total < time {
                    let lba = rng.gen_range(range.0..range.1);
                    let before = Instant::now();
                    while let Some(_) = qpair.quick_poll() {
                        ctr -= 1;
                        ios += 1;
                    }
                    if ctr == batch_size {
                        qpair.complete_io(1);
                        ctr -= 1;
                        ios += 1;
                    }
                    let mut reqs = 0;
                    if(write) {
                        reqs = nvme.write_concurrent(&mut qpair, &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), lba * blocks).unwrap();
                    } else {
                        reqs = nvme.read_concurrent(&mut qpair, &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), lba * blocks).unwrap();
                    }

                    assert!(reqs == 1);
                    total += before.elapsed();
                    ctr += reqs;
                }

                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                ios += ctr as u64;
                assert!(qpair.sub_queue.is_empty());
                nvme.backing.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

                (ios, ios as f64 / total.as_secs_f64())
            } else {
                let seq = &(0..n)
                    .map(|_| rng.gen_range(range.0..range.1))
                    .collect::<Vec<u64>>()[..];
                for &lba in seq {
                    let before = Instant::now();
                    while let Some(_) = qpair.quick_poll() {
                        ctr -= 1;
                    }
                    if ctr == batch_size {
                        qpair.complete_io(1);
                        ctr -= 1;
                    }
                    qpair.submit_io(
                        ns_id,
                        block_size,
                        &buffer.slice((ctr * bytes)..(ctr + 1) * bytes),
                        lba * blocks,
                        write,
                    );
                    total += before.elapsed();
                    ctr += 1;
                }
                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                assert!(qpair.sub_queue.is_empty());
                nvme.backing.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

                (n, n as f64 / total.as_secs_f64())
            }

        });
        threads.push(handle);
    }

    let total = threads
        .into_iter()
        .fold((0, 0.), |acc, thread| {
            let res = thread
                .join()
                .expect("The thread creation or execution failed!");
            (
                acc.0 + res.0,
                acc.1 + res.1,
            )
        });
    println!(
        "n: {}, total {} iops: {:?}",
        total.0,
        if write { "write" } else { "read" },
        total.1
    );

    nvme.stop_reclaim();
    reclaim_thread.join().unwrap();

    let mut nvme = Arc::into_inner(nvme).unwrap();

    Ok(nvme)

}

// For benchmarking qd1 with completion inside nonseq.rs
#[allow(unused)]
fn qd_n_alt(
    mut nvme: ZNSTarget,
    ns_id: u32,
    n_threads: u64,
    n: u64,
    write: bool,
    batch_size: usize,
    time: Option<Duration>,
) -> Result<ZNSTarget, Box<dyn Error>> {
    let blocks = 1;
    let mut backing = nvme.backing.get_mut().unwrap();
    let ns_blocks = backing.namespaces.get(&ns_id).unwrap().blocks / blocks;
    let block_size = backing.namespaces.get(&ns_id).unwrap().block_size;

    let max_lba = nvme.max_lba;

    let mut threads = Vec::new();

    let mut queue_pairs = Vec::new();

    for _ in 0..n_threads + 1 {
        let qpair = backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        queue_pairs.push(qpair);
    }


    let queue_pairs = Arc::new(Mutex::new(queue_pairs));    
    let nvme = Arc::new(nvme);

    let znstarget_reclaim = nvme.clone();
    let znstarget_queue_pair = queue_pairs.clone();
    let reclaim_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        let mut reclaim_qpair = znstarget_queue_pair.lock().unwrap().pop().unwrap();
        loop {
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            if condition {
                break;
            }
            znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer).unwrap();
        }
        znstarget_reclaim.backing.lock().unwrap().delete_io_queue_pair(reclaim_qpair).unwrap();
    });

    for i in 0..n_threads {
        let nvme = Arc::clone(&nvme);
        let queue_pairs = queue_pairs.clone();
        let range = (0, max_lba);

        let handle = thread::spawn(move || -> (u64, f64) {
            let mut rng = rand::thread_rng();
            let bytes = (block_size * blocks) as usize;
            let mut total = std::time::Duration::ZERO;
            let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();

            let mut qpair = queue_pairs.lock().unwrap().pop().unwrap();

            let rand_block = &(0..(32 * bytes))
                .map(|_| rand::random::<u8>())
                .collect::<Vec<_>>()[..];
            buffer[0..32 * bytes].copy_from_slice(rand_block);

            let mut ctr = 0;
            if let Some(time) = time {
                let mut ios = 0;
                while total < time {
                    let lba = rng.gen_range(range.0..range.1);
                    let before = Instant::now();

                    let mut reqs = 0;
                    if(write) {
                        reqs = nvme.write_concurrent(&mut qpair, &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), lba * blocks).unwrap();
                    } else {
                        reqs = nvme.read_concurrent(&mut qpair, &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), lba * blocks).unwrap();
                    }

                    total += before.elapsed();
                    ios += 1;
                }

                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                ios += ctr as u64;
                assert!(qpair.sub_queue.is_empty());
                nvme.backing.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

                (ios, ios as f64 / total.as_secs_f64())
            } else {
                let seq = &(0..n)
                    .map(|_| rng.gen_range(range.0..range.1))
                    .collect::<Vec<u64>>()[..];
                for &lba in seq {
                    let before = Instant::now();
                    while let Some(_) = qpair.quick_poll() {
                        ctr -= 1;
                    }
                    if ctr == batch_size {
                        qpair.complete_io(1);
                        ctr -= 1;
                    }
                    qpair.submit_io(
                        ns_id,
                        block_size,
                        &buffer.slice((ctr * bytes)..(ctr + 1) * bytes),
                        lba * blocks,
                        write,
                    );
                    total += before.elapsed();
                    ctr += 1;
                }
                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                assert!(qpair.sub_queue.is_empty());
                nvme.backing.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

                (n, n as f64 / total.as_secs_f64())
            }

        });
        threads.push(handle);
    }

    let total = threads
        .into_iter()
        .fold((0, 0.), |acc, thread| {
            let res = thread
                .join()
                .expect("The thread creation or execution failed!");
            (
                acc.0 + res.0,
                acc.1 + res.1,
            )
        });
    println!(
        "n: {}, total {} iops: {:?}",
        total.0,
        if write { "write" } else { "read" },
        total.1
    );

    nvme.stop_reclaim();
    reclaim_thread.join().unwrap();

    let mut nvme = Arc::into_inner(nvme).unwrap();

    Ok(nvme)

}

#[allow(unused)]
fn qd1(
    mut nvme: ZNSTarget,
    ns_id: u32,
    n: u64,
    write: bool,
    random: bool,
    time: Option<Duration>,
) -> Result<ZNSTarget, Box<dyn Error>> {
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE)?;

    let backing = nvme.backing.get_mut().unwrap();
    let ns = backing.namespaces.get(&ns_id).unwrap();
    let blocks = 1; // Blocks that will be read/written at a time
    let bytes = blocks * ns.block_size;
    let ns_blocks = ns.blocks / blocks - 1; // - blocks - 1;
    let available_blocks = nvme.max_lba;

    let mut rng = thread_rng();
    let seq = if random {
        (0..n)
            .map(|_| rng.gen_range(0..available_blocks as u64))
            .collect::<Vec<u64>>()
    } else {
        (0..n).map(|i| (i * 8) % available_blocks).collect::<Vec<u64>>()
    };

    let rand_block = &(0..bytes).map(|_| rand::random::<u8>()).collect::<Vec<_>>()[..];
    buffer[..rand_block.len()].copy_from_slice(rand_block);

    let mut total = Duration::ZERO;

    if let Some(time) = time {
        let mut ios = 0;
        let lba = 0;
        while total < time {
            let lba = if random { rng.gen_range(0..available_blocks) } else { (lba + 1) % available_blocks };

            let before = Instant::now();
            if write {
                nvme.write(&buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                nvme.read(&buffer.slice(0..bytes as usize), lba * blocks)?;
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
                nvme.write(&buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                nvme.read(&buffer.slice(0..bytes as usize), lba * blocks)?;
            }
            total += before.elapsed();
        }
        println!(
            "n: {n}, total {} iops: {:?}",
            if write { "write" } else { "read" },
            n as f64 / total.as_secs_f64()
        );
    }
    Ok(nvme)
}

fn fill_target(nvme: &mut ZNSTarget) {
    let buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let mut lba = 0;
    while lba <= nvme.max_lba {
        nvme.write(&buffer.slice(0..nvme.block_size as usize), lba).unwrap();
        lba += 1;
    }
}
