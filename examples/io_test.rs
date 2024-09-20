use rand::{thread_rng, Rng};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, process, thread};
use vroom::memory::*;
use vroom::{NvmeDevice, QUEUE_LENGTH};

#[allow(unused_variables, unused_mut)]
pub fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example init <pci bus id>");
            process::exit(1);
        }
    };

    let duration = match args.next() {
        Some(secs) => Some(Duration::from_secs(secs.parse().expect(
            "Usage: cargo run --example init <pci bus id> <duration in seconds>",
        ))),
        None => None,
    };

    let mut nvme = vroom::init(&pci_addr)?;

    const NS_ID : u32 = 2;

    //nvme.zone_action(NS_ID, 0, true, vroom::ZnsZsa::ResetZone)?;
    nvme.get_zone_reports(NS_ID)?;
    //let _ = qd1_zns_write(nvme, NS_ID, 0, false, duration)?;
    //let _ = qd_n_zns_write(nvme, NS_ID, 1, 0, true, 32, duration);

    Ok(())
}


#[allow(unused)]
fn qd1(
    mut nvme: NvmeDevice,
    ns_id: u32,
    n: u64,
    write: bool,
    random: bool,
    time: Option<Duration>,
) -> Result<NvmeDevice, Box<dyn Error>> {
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE)?;

    let ns = nvme.namespaces.get(&ns_id).unwrap();
    let blocks = 1; // Blocks that will be read/written at a time
    let bytes = blocks * ns.block_size;
    let ns_blocks = ns.blocks / blocks - 1; // - blocks - 1;

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
                nvme.write(ns_id, &buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                nvme.read(ns_id, &buffer.slice(0..bytes as usize), lba * blocks)?;
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
                nvme.write(ns_id, &buffer.slice(0..bytes as usize), lba * blocks)?;
            } else {
                nvme.read(ns_id, &buffer.slice(0..bytes as usize), lba * blocks)?;
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

#[allow(unused)]
fn qd_n(
    nvme: NvmeDevice,
    ns_id: u32,
    n_threads: u64,
    n: u64,
    write: bool,
    batch_size: usize,
    time: Option<Duration>,
) -> Result<NvmeDevice, Box<dyn Error>> {
    let blocks = 1;
    let ns_blocks = nvme.namespaces.get(&ns_id).unwrap().blocks / blocks;
    let block_size = nvme.namespaces.get(&ns_id).unwrap().block_size;

    let nvme = Arc::new(Mutex::new(nvme));
    let mut threads = Vec::new();

    for i in 0..n_threads {
        let nvme = Arc::clone(&nvme);
        let range = (0, ns_blocks);

        let handle = thread::spawn(move || -> (u64, f64) {
            let mut rng = rand::thread_rng();
            let bytes = (block_size * blocks) as usize;
            let mut total = std::time::Duration::ZERO;
            let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();

            let mut qpair = nvme
                .lock()
                .unwrap()
                .create_io_queue_pair(QUEUE_LENGTH)
                .unwrap();

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
                ios += ctr as u64;
                assert!(qpair.sub_queue.is_empty());
                nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

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
                nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();
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

    match Arc::try_unwrap(nvme) {
        Ok(mutex) => match mutex.into_inner() {
            Ok(t) => Ok(t),
            Err(e) => Err(e.into()),
        },
        Err(_) => Err("Arc::try_unwrap failed, not the last reference.".into()),
    }
}

#[allow(unused)]
fn qd1_zns_write(
    mut nvme: NvmeDevice,
    ns_id: u32,
    n: u64,
    append: bool,
    time: Option<Duration>,
) -> Result<NvmeDevice, Box<dyn Error>> {
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE)?;

    let ns = nvme.namespaces.get(&ns_id).unwrap();
    let blocks = 1; // Blocks that will be read/written at a time
    let bytes = blocks * ns.block_size;
    let ns_blocks = ns.blocks / blocks - 1; // - blocks - 1;

    let mut rng = thread_rng();
    let seq = (0..n).map(|i| i % ns_blocks).collect::<Vec<u64>>(); //TODO fix this


    let rand_block = &(0..bytes).map(|_| rand::random::<u8>()).collect::<Vec<_>>()[..];
    buffer[..rand_block.len()].copy_from_slice(rand_block);

    let mut total = Duration::ZERO;

    if let Some(time) = time {
        let mut ios = 0;
        let zones = nvme.namespaces.get(&ns_id).unwrap().zns_info.unwrap().n_zones;
        let zone_size = nvme.namespaces.get(&ns_id).unwrap().zns_info.unwrap().zone_size;
        let zcap = nvme.get_zone_descriptors(ns_id).unwrap()[0].zcap;
        let mut lba = 0;
        let mut zone = 0;
        while total < time {
            if(lba >= zcap) {
                lba = 0;
                zone += 1;
            }

            let before = Instant::now();
            if append {
                nvme.append_io(ns_id, zone * zone_size, &buffer.slice(0..bytes as usize))?;
            } else {
                nvme.write(ns_id, &buffer.slice(0..bytes as usize), zone * zone_size + lba)?;
            }
            let elapsed = before.elapsed();
            total += elapsed;
            ios += 1;
            lba += blocks;
        }
        println!(
            "IOP: {ios}, total {} iops: {:?}",
            if append { "append" } else { "write" },
            ios as f64 / total.as_secs_f64()
        );
    } else {
        for lba in seq {
            let before = Instant::now();
            if append {
                nvme.append_io(ns_id, lba, &buffer.slice(0..bytes as usize))?;
            } else {
                nvme.write(ns_id, &buffer.slice(0..bytes as usize), lba)?;
            }
            total += before.elapsed();
        }
        println!(
            "n: {n}, total {} iops: {:?}",
            if append { "append" } else { "write" },
            n as f64 / total.as_secs_f64()
        );
    }
    Ok(nvme)
}

#[allow(unused)]
fn qd_n_zns_write(
    mut nvme: NvmeDevice,
    ns_id: u32,
    n_threads: u64,
    n: u64,
    append: bool,
    batch_size: usize,
    time: Option<Duration>,
) -> Result<NvmeDevice, Box<dyn Error>> {
    let blocks = 1;
    let ns_blocks = nvme.namespaces.get(&ns_id).unwrap().blocks / blocks;
    let block_size = nvme.namespaces.get(&ns_id).unwrap().block_size;

    let zones = nvme.namespaces.get(&ns_id).unwrap().zns_info.unwrap().n_zones;
    let zone_size = nvme.namespaces.get(&ns_id).unwrap().zns_info.unwrap().zone_size;
    let zcap = nvme.get_zone_descriptors(ns_id).unwrap()[0].zcap;

    let nvme = Arc::new(Mutex::new(nvme));
    let mut threads = Vec::new();

    for i in 0..n_threads {
        let nvme = Arc::clone(&nvme);

        let handle = thread::spawn(move || -> (u64, f64) {
            let mut rng = rand::thread_rng();
            let bytes = (block_size * blocks) as usize;
            let mut total = std::time::Duration::ZERO;
            let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();

            let mut qpair = nvme
                .lock()
                .unwrap()
                .create_io_queue_pair(QUEUE_LENGTH)
                .unwrap();

            let rand_block = &(0..(32 * bytes))
                .map(|_| rand::random::<u8>())
                .collect::<Vec<_>>()[..];
            buffer[0..32 * bytes].copy_from_slice(rand_block);

            let mut ctr = 0;
            if let Some(time) = time {
                let mut ios = 0;
                let mut lba = 0;
                let mut zone = i * (zones / n_threads);
                while total < time {
                    if(lba >= zcap) {
                        lba = 0;
                        zone += 1;
                    }
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
                    if append {
                        qpair.append_io(
                            ns_id,
                            block_size, 
                            &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), 
                            zone * zone_size
                        );
                    } else {
                        qpair.submit_io(
                            ns_id,
                            block_size,
                            &buffer.slice((ctr * bytes)..(ctr + 1) * bytes),
                            lba,
                            true,
                        );
                    }
                    total += before.elapsed();
                    ctr += 1;
                    lba += blocks;
                }

                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                ios += ctr as u64;
                assert!(qpair.sub_queue.is_empty());
                nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();

                (ios, ios as f64 / total.as_secs_f64())
            } else {
                let seq = &(0..n)
                    .map(|_| rng.gen_range(0..ns_blocks))
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
                    if !append {
                        qpair.submit_io(
                            ns_id,
                            block_size,
                            &buffer.slice((ctr * bytes)..(ctr + 1) * bytes),
                            lba,
                            true,
                        );
                    } else {
                        qpair.append_io(
                            ns_id,
                            block_size, 
                            &buffer.slice((ctr * bytes)..(ctr + 1) * bytes), 
                            lba);
                    }
                    total += before.elapsed();
                    ctr += 1;
                }
                if ctr != 0 {
                    let before = Instant::now();
                    qpair.complete_io(ctr);
                    total += before.elapsed();
                }
                assert!(qpair.sub_queue.is_empty());
                nvme.lock().unwrap().delete_io_queue_pair(qpair).unwrap();
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
        if append { "append" } else { "write" },
        total.1
    );

    match Arc::try_unwrap(nvme) {
        Ok(mutex) => match mutex.into_inner() {
            Ok(t) => Ok(t),
            Err(e) => Err(e.into()),
        },
        Err(_) => Err("Arc::try_unwrap failed, not the last reference.".into()),
    }
}

#[allow(unused)]
fn fill_ns(nvme: &mut NvmeDevice, ns_id: u32) {
    let buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let block_size = nvme.namespaces.get(&ns_id).unwrap().block_size;
    let max_lba = nvme.namespaces.get(&ns_id).unwrap().blocks - buffer.size as u64 / block_size - 1;
    let blocks = buffer.size as u64 / block_size;
    let mut lba = 0;
    while lba < max_lba - block_size {
        nvme.write(ns_id, &buffer, lba).unwrap();
        lba += blocks;
    }
}


#[allow(unused)]
fn fill_ns_zns(nvme: &mut NvmeDevice, ns_id: u32) {
    let buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let ns = nvme.namespaces.get(&ns_id).unwrap();
    let block_size = ns.block_size;
    let zones = ns.zns_info.unwrap().n_zones;
    let zone_descriptors = nvme.get_zone_descriptors(ns_id).unwrap();
    for i in 0..zones {
        let zcap = zone_descriptors[i as usize].zcap;
        let zslba = zone_descriptors[i as usize].zslba;
        let max_lba = zslba + zcap;
        let blocks = buffer.size as u64 / block_size;
        for lba in zslba..zslba+zcap {
            nvme.write(ns_id, &buffer.slice(0..block_size as usize), lba).unwrap();
        }
        println!("Filled zone {}", i);
    }
}
