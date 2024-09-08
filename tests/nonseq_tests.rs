mod common;

use std::sync::{Arc, Mutex};
use common::*;
use vroom::{memory::Dma, memory::DmaSlice, HUGE_PAGE_SIZE, QUEUE_LENGTH};
use rand::{thread_rng, Rng};

const NS : u32 = 1;

// Note: Huge page size can hold 512 blocks assuming block size 4096

// sudo NVME_ADDR="0000:00:04.0" RUST_BACKTRACE=1 cargo test --test nonseq_tests -- --nocapture --test-threads=1


#[test]
fn simple_write_then_read() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;
    let available_zones = (zones as f32 * 0.7) as usize;

    let a = &(0..block_size * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    write_buffer[..block_size * 3].copy_from_slice(a);

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..(available_zones * zcap) - 10);

    znstarget.write(&write_buffer.slice(0..block_size * 3), lba as u64).unwrap();
    znstarget.read(&mut read_buffer.slice(0..block_size * 3), lba as u64).unwrap();
    

    for a in read_buffer[0..block_size * 3].iter() {
        assert_eq!(*a, 'a' as u8);
    }
}

#[test]
fn simple_write_then_read_copied() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;
    let available_zones = (zones as f32 * 0.7) as usize;
    
    let a = vec!['a' as u8; block_size * 3];
    let mut read_buffer = vec![0 as u8; block_size * 3];

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..(available_zones * zcap) - 10);

    znstarget.write_copied(&a, lba as u64).unwrap();
    znstarget.read_copied(&mut read_buffer, lba as u64).unwrap();

    for a in read_buffer.iter() {
        assert_eq!(*a, 'a' as u8);
    }
}

#[test]
fn simple_overwrite_then_read() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;
    let available_zones = (zones as f32 * 0.7) as usize;

    let a = &(0..block_size).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    let b = &(0..block_size).map(|_| 'b' as u8).collect::<Vec<_>>()[..];
    write_buffer[..block_size].copy_from_slice(a);

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..available_zones * zcap - 10);

    znstarget.write(&write_buffer.slice(0..block_size), lba as u64).unwrap();
    write_buffer[..block_size].copy_from_slice(b);
    znstarget.write(&write_buffer.slice(0..block_size), lba as u64).unwrap();
    znstarget.read(&read_buffer.slice(0..block_size), lba as u64).unwrap();

    for i in read_buffer[0..block_size].iter() {
        assert_eq!(*i, 'b' as u8);
    }
}

#[test]
fn overwrite_partial() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer_a : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();
    let mut write_buffer_b : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();

    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;

    let a = &(0..block_size * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    let b = &(0..block_size * 3).map(|_| 'b' as u8).collect::<Vec<_>>()[..];
    write_buffer_a[0..block_size * 3].copy_from_slice(a);
    write_buffer_b[0..block_size * 3].copy_from_slice(b);

    znstarget.write(&write_buffer_a.slice(0..block_size * 3), 0).unwrap();
    znstarget.write(&write_buffer_b.slice(0..block_size * 3), 2).unwrap();
    znstarget.write(&write_buffer_a.slice(0..block_size * 3), 90000).unwrap();
    
    znstarget.read(&read_buffer.slice(0..block_size * 3), 0).unwrap();
    for i in read_buffer[..2 * block_size].iter() {
        assert_eq!(*i, 'a' as u8);
    }
    for i in read_buffer[2 * block_size..3 * block_size].iter() {
        assert_eq!(*i, 'b' as u8);
    }
}

#[test]
fn cross_zone() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer_a : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE as usize).unwrap();

    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;

    let a = &(0..block_size * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    write_buffer_a[0..block_size * 3].copy_from_slice(a);

    for _ in 0..zcap - 1 {
        znstarget.write(&write_buffer_a.slice(0..block_size), 0).unwrap();
    }
    znstarget.write(&write_buffer_a.slice(0..3 * block_size), 0).unwrap();
    
    znstarget.read(&read_buffer.slice(0..block_size * 3), 0).unwrap();
    for i in read_buffer[0..block_size * 3].iter() {
        assert_eq!(*i, 'a' as u8);
    }
}

#[test]
fn sequential_reclaim_one_writer() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;

    let available_zones = (zones as f32 * 0.7) as usize;

    if available_zones / 2 - 1 < 1 {
        return;
    }

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer : Dma<u8> = Dma::allocate(zcap * block_size).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(zcap * block_size).unwrap();

    for i in 'a'..'z' {
        let a = &(0..zcap * block_size).map(|_| i as u8).collect::<Vec<_>>()[..];
        write_buffer[0..zcap * block_size].copy_from_slice(a);
        znstarget.write(&write_buffer.slice(0..zcap * block_size), 0).unwrap();
    }

    znstarget.read(&read_buffer.slice(0..zcap * block_size), 0).unwrap();

    for a in read_buffer[0..zcap * block_size].iter() {
        assert_eq!(*a, 'y' as u8);
    }

    let zone_descriptors = znstarget.backing.get_zone_descriptors(NS).unwrap();
    let mut count = 0;
    for zone_descriptor in zone_descriptors {
        if zone_descriptor.zslba != zone_descriptor.wp {
            count += 1;
        }
    }
    assert_eq!(count, available_zones/2 - 1);
}

#[test]
fn sequential_partial_reclaim() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.6, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;

    let available_zones = (zones as f32 * 0.4) as usize;

    if available_zones / 2 - 1 < 1 {
        return;
    }

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut write_buffer : Dma<u8> = Dma::allocate(zcap * block_size).unwrap();
    let read_buffer : Dma<u8> = Dma::allocate(zcap * block_size).unwrap();

    let b = &(0..8192).map(|_| 'X' as u8).collect::<Vec<_>>()[..];

    for i in 'a'..'z' {
        let a = &(0..8192).map(|_| i as u8).collect::<Vec<_>>()[..];
        write_buffer[0..8192].copy_from_slice(a);
        for _ in 0..5000 {
            znstarget.write(&write_buffer.slice(0..8192), 0).unwrap();
        }
        write_buffer[0..8192].copy_from_slice(b);
        znstarget.write(&write_buffer.slice(0..8192), 50000).unwrap();

    }

    znstarget.read(&read_buffer.slice(0..8192), 0).unwrap();
    for a in read_buffer[0..8192].iter() {
        assert_eq!(*a, 'y' as u8);
    }

    znstarget.read(&read_buffer.slice(0..8192), 50000).unwrap();
    for a in read_buffer[0..8192].iter() {
        assert_eq!(*a, 'X' as u8);
    }
}

#[test]
fn concurrent_writer() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut writer_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    
    let znstarget = Arc::new(znstarget);

    let znstarget_write = Arc::clone(&znstarget);
    let write_thread = std::thread::spawn(move || {
        let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
        for i in 'a'..'z' {
            let a = &(0..8192).map(|_| i as u8).collect::<Vec<_>>()[..];
            write_buffer[0..8192].copy_from_slice(a);
            for _ in 0..3000 {
                let reqs = znstarget_write.write_concurrent(&mut writer_qpair,&write_buffer.slice(0..8192), 0).unwrap();
                writer_qpair.complete_io(reqs);
            }
        }
        drop(znstarget_write);
    });
    
    write_thread.join().unwrap();

    let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let mut znstarget = Arc::into_inner(znstarget).unwrap();
    znstarget.read(&read_buffer.slice(0..8192), 0).unwrap();
    for a in read_buffer[0..8192].iter() {
        assert_eq!(*a, 'y' as u8);
    }
}

#[test]
fn concurrent_writers_readers() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let mut queue_pairs = Vec::new();

    const N_THREADS : u64 = 4;

    for _ in 0..N_THREADS * 2 {
        let qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        queue_pairs.push(qpair);
    }

    let mut threads = Vec::new();

    let queue_pairs = Arc::new(Mutex::new(queue_pairs));    

    let znstarget = Arc::new(znstarget);

    for t in 0..N_THREADS {
        let znstarget_write = Arc::clone(&znstarget);
        let queue_pairs = queue_pairs.clone();
        let write_thread = std::thread::spawn(move || {
            let mut writer_qpair = queue_pairs.lock().unwrap().pop().unwrap();
            let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
            for i in 'a'..'z' {
                let a = &(0..8192).map(|_| i as u8).collect::<Vec<_>>()[..];
                write_buffer[0..8192].copy_from_slice(a);
                for _ in 0..1000 {
                    let reqs = znstarget_write.write_concurrent(&mut writer_qpair,&write_buffer.slice(0..8192), t * 100).unwrap();
                    writer_qpair.complete_io(reqs).unwrap();
                }
            }
            drop(znstarget_write);
        });
        threads.push(write_thread);
        
    }
    
    for thread in threads {
        thread.join().unwrap();
    }

    let mut threads = Vec::new();

    for t in 0..N_THREADS {
        let znstarget_read = Arc::clone(&znstarget);
        let queue_pairs = Arc::clone(&queue_pairs);
        let read_thread = std::thread::spawn(move || {
            let mut reader_qpair = queue_pairs.lock().unwrap().pop().unwrap();
            let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
                let reqs = znstarget_read.read_concurrent(&mut reader_qpair, &read_buffer.slice(0..8192), t * 100).unwrap();
                reader_qpair.complete_io(reqs).unwrap();
                for a in read_buffer[0..8192].iter() {
                    assert_eq!(*a, 'y' as u8);
                }
                drop(znstarget_read);
        });
        threads.push(read_thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn concurrent_reclaim_one_writer() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;

    let available_zones = (zones as f32 * 0.7) as usize;

    if available_zones / 2 - 1 < 1 {
        return;
    }

    let mut reclaim_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    let mut writer_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    
    let znstarget = Arc::new(znstarget);

    let znstarget_reclaim = znstarget.clone();

    let reclaim_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        loop {
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            if condition {
                break;
            }
            znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer).unwrap();
        }
    });

    let znstarget_write = znstarget.clone();
    let write_thread = std::thread::spawn(move || {
        let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
        let b = &(0..8192).map(|_| 'X' as u8).collect::<Vec<_>>()[..];
        for i in 'a'..'z' {
            let a = &(0..8192).map(|_| i as u8).collect::<Vec<_>>()[..];
            write_buffer[0..8192].copy_from_slice(a);
            for _ in 0..3000 {
                let reqs = znstarget_write.write_concurrent(&mut writer_qpair,&write_buffer.slice(0..8192), 0).unwrap();
                writer_qpair.complete_io(reqs).unwrap();
            }
            write_buffer[0..8192].copy_from_slice(b);
            let reqs = znstarget_write.write_concurrent(&mut writer_qpair,&write_buffer.slice(0..8192), 50000).unwrap();
            writer_qpair.complete_io(reqs).unwrap();
        }
        drop(znstarget_write);
    });

    write_thread.join().unwrap();

    znstarget.stop_reclaim();
    reclaim_thread.join().unwrap();

    let read_buffer : Dma<u8> = Dma::allocate(zcap * block_size).unwrap();
    let mut znstarget = Arc::try_unwrap(znstarget).unwrap_or_else(|_| panic!("Arc unwrapping went wrong :("));
    znstarget.read(&read_buffer.slice(0..8192), 0).unwrap();
    for a in read_buffer[0..8192].iter() {
        assert_eq!(*a, 'y' as u8);
    }

    znstarget.read(&read_buffer.slice(0..8192), 50000).unwrap();
    for a in read_buffer[0..8192].iter() {
        assert_eq!(*a, 'X' as u8);
    }
}

#[test]
fn concurrent_reading_reclaimed_zone() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.5, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();
    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;

    let available_zones = (zones as f32 * 0.5) as usize;

    if available_zones / 2 - 1 < 1 {
        return;
    }

    // Fill up the first zone with invalid data
    let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let a = &(0..block_size).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    write_buffer[0..block_size].copy_from_slice(a);
    for _ in 0..zcap {
        znstarget.write(&write_buffer.slice(0..block_size), 0).unwrap();
    }

    const N_THREADS : usize = 2;
    let mut queue_pairs = Vec::new();


    // Start reclaim thread, concurrent read first zone and fill up other zones with valid data
    for _ in 0..N_THREADS {
        let qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        queue_pairs.push(qpair);
    }

    let mut threads = Vec::new();

    let mut reclaim_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    let mut writer_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    
    let znstarget = Arc::new(znstarget);

    let znstarget_reclaim = znstarget.clone();

    let reclaim_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        loop {
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            if condition {
                break;
            }
            znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer).unwrap();
        }
    });

    let queue_pairs = Arc::new(Mutex::new(queue_pairs));

    let znstarget_write = znstarget.clone();

    let writer_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        let a = &(0..block_size).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
        buffer[0..4096].copy_from_slice(a);
        for zone in 1..available_zones/2 {
            for lba in 0..zcap {
                let reqs = znstarget_write.write_concurrent(&mut writer_qpair, &buffer.slice(0..4096), (zone * zcap + lba) as u64).unwrap();
                writer_qpair.complete_io(reqs).unwrap();
            }
        }
    });

    for _ in 0..N_THREADS {
        let znstarget_read = Arc::clone(&znstarget);
        let queue_pairs = Arc::clone(&queue_pairs);
        let read_thread = std::thread::spawn(move || {
            let mut reader_qpair = queue_pairs.lock().unwrap().pop().unwrap();
            let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
            for _ in 0..500000 {
                let reqs = znstarget_read.read_concurrent(&mut reader_qpair, &read_buffer.slice(0..4096), 0).unwrap();
                reader_qpair.complete_io(reqs).unwrap();
            }
            for a in read_buffer[0..4096].iter() {
                assert_eq!(*a, 'a' as u8);
            }
        });
        threads.push(read_thread);
    }

    writer_thread.join().unwrap();

    for thread in threads {
        thread.join().unwrap();
    }

    znstarget.stop_reclaim();
    reclaim_thread.join().unwrap();
}

#[test]
fn concurrency_boss() {
    let mut znstarget = init_zns_target(
        &get_pci_addr(), 
        NS, 
        0.3, 
        vroom::nonseq::VictimSelectionMethod::InvalidBlocks);

    const N_THREADS : usize = 4;

    znstarget.backing.zone_action(NS, 0, true, vroom::ZnsZsa::ResetZone).unwrap();

    let zcap = znstarget.backing.get_zone_descriptors(NS).unwrap()[0].zcap as usize;
    let block_size = znstarget.backing.namespaces.get(&NS).unwrap().block_size as usize;
    let zones = znstarget.backing.namespaces.get(&NS).unwrap().zns_info.unwrap().n_zones;

    let available_zones = (zones as f32 * 0.7) as usize;

    if available_zones / 2 - 1 < 1 {
        return;
    }

    // First write some data for the reads to use, fill up n_threads zones
    let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    for i in 0..N_THREADS {
        let a = &(0..block_size).map(|_| 'a' as u8 + i as u8).collect::<Vec<_>>()[..];
        write_buffer[0..block_size].copy_from_slice(a);
        for j in 0..zcap {
            znstarget.write(&write_buffer.slice(0..block_size), (i * zcap + j) as u64).unwrap();
        } 
    }

    let mut queue_pairs = Vec::new();

    for _ in 0..N_THREADS * 2 {
        let qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        queue_pairs.push(qpair);
    }

    let mut threads = Vec::new();

    let mut reclaim_qpair = znstarget.backing.create_io_queue_pair(QUEUE_LENGTH).unwrap();
    
    let znstarget = Arc::new(znstarget);

    let znstarget_reclaim = znstarget.clone();

    let reclaim_thread = std::thread::spawn(move || {
        let mut buffer : Dma<u8> = Dma::allocate(4096).unwrap();
        loop {
            let condition = znstarget_reclaim.end_reclaim.load(std::sync::atomic::Ordering::Relaxed);
            if condition {
                break;
            }
            znstarget_reclaim.reclaim_concurrent(&mut reclaim_qpair, &mut buffer).unwrap();
        }
    });

    let queue_pairs = Arc::new(Mutex::new(queue_pairs));    

    for t in 0..N_THREADS {
        let znstarget_write = Arc::clone(&znstarget);
        let queue_pairs = queue_pairs.clone();
        let write_thread = std::thread::spawn(move || {
            let mut writer_qpair = queue_pairs.lock().unwrap().pop().unwrap();
            let mut write_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
            for i in 'a'..'z' {
                let a = &(0..8192).map(|_| i as u8).collect::<Vec<_>>()[..];
                write_buffer[0..8192].copy_from_slice(a);
                for k in 1..3000 {
                    let reqs = znstarget_write.write_concurrent(&mut writer_qpair,&write_buffer.slice(0..8192), ((i as usize - 90) * k * t) as u64).unwrap();
                    writer_qpair.complete_io(reqs).unwrap();
                }
            }
            println!("Writer {} is finished", t);
            drop(znstarget_write);
        });
        threads.push(write_thread);
    }

    for t in 0..N_THREADS {
        let znstarget_read = Arc::clone(&znstarget);
        let queue_pairs = queue_pairs.clone();
        let read_thread = std::thread::spawn(move || {
            let mut reader_qpair = queue_pairs.lock().unwrap().pop().unwrap();
            let read_buffer : Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
            for i in 1..50000 {
                let reqs = znstarget_read.read_concurrent(&mut reader_qpair, &read_buffer.slice(0..8192), (t * zcap + i) as u64).unwrap();
                reader_qpair.complete_io(reqs).unwrap();
            }
            drop(znstarget_read);
            println!("Reader {} is finished", t);
        });
        threads.push(read_thread);
    }

    for thread in threads {
        thread.join().unwrap();
    }

    znstarget.stop_reclaim();
    reclaim_thread.join().unwrap();
}