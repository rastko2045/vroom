use std::error::Error;
use rand::{thread_rng, Rng};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, process, thread};
use vroom::memory::*;
use vroom::{NvmeDevice, QUEUE_LENGTH};
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
    let mut znstarget = vroom::nonseq::ZNSTarget::init(0.3, nvme, VictimSelectionMethod::InvalidBlocks)?;

    znstarget.backing.zone_action(1, 0, true, vroom::ZnsZsa::ResetZone)?;

    qd1(znstarget, 1, true, true, duration)?;

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

// pub fn test_concurrent(nvme : NvmeDevice) -> Result<(), Box<dyn Error>> {
//     let znstarget = Arc::new(vroom::nonseq::ZNSTarget::init(0.3, nvme, VictimSelectionMethod::InvalidBlocks)?);
//     let mut threads: Vec<thread::JoinHandle<(u64, f64)>> = Vec::new();
    
//     Ok(())
// }

fn qd1(
    mut nvme: ZNSTarget,
    n: u64,
    write: bool,
    random: bool,
    time: Option<Duration>,
) -> Result<ZNSTarget, Box<dyn Error>> {
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE)?;

    const N_BLOCKS : u64 = 15782 * 40;

    let ns = nvme.backing.namespaces.get(&1).unwrap();
    let blocks = 8; // Blocks that will be read/written at a time
    let bytes = blocks * ns.block_size;
    //let ns_blocks = ns.blocks / blocks - 1; // - blocks - 1;
    let ns_blocks = N_BLOCKS;

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