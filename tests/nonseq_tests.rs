mod common;
use common::*;
use vroom::{memory::Dma, memory::DmaSlice, HUGE_PAGE_SIZE};
use rand::{thread_rng, Rng};

const ZCAP : usize = 15872;
const BLOCK_SIZE : usize = 4096;
const ZCAP_BYTES : usize = ZCAP * BLOCK_SIZE;
const NS : u32 = 1;
const ZONES: u64 = 32;

// Note: Huge page size can hold 512 blocks assuming block size 4096

// sudo NVME_ADDR="0000:00:04.0" RUST_BACKTRACE=1 cargo test --test nonseq_test -- simple_write_then_read  --nocapture --test-threads=1


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

    let a = &(0..BLOCK_SIZE * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    write_buffer[..BLOCK_SIZE * 3].copy_from_slice(a);

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..(ZONES * ZCAP as u64) - 10);

    znstarget.write(&write_buffer.slice(0..BLOCK_SIZE * 3), lba).unwrap();
    znstarget.read(&read_buffer.slice(0..BLOCK_SIZE * 3), lba).unwrap();
    

    for a in read_buffer[0..BLOCK_SIZE * 3].iter() {
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

    
    let a = vec!['a' as u8; BLOCK_SIZE * 3];
    let mut read_buffer = vec![0 as u8; BLOCK_SIZE * 3];

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..(ZONES * ZCAP as u64) - 10);

    znstarget.write_copied(&a, lba).unwrap();
    znstarget.read_copied(&mut read_buffer, lba).unwrap();

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

    let a = &(0..BLOCK_SIZE).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    let b = &(0..BLOCK_SIZE).map(|_| 'b' as u8).collect::<Vec<_>>()[..];
    write_buffer[..BLOCK_SIZE].copy_from_slice(a);

    let mut rng = thread_rng();
    let lba = rng.gen_range(0..ZONES * ZCAP as u64 - 10);

    znstarget.write(&write_buffer.slice(0..BLOCK_SIZE), lba).unwrap();
    write_buffer[..BLOCK_SIZE].copy_from_slice(b);
    znstarget.write(&write_buffer.slice(0..BLOCK_SIZE), lba).unwrap();
    znstarget.read(&read_buffer.slice(0..BLOCK_SIZE), lba).unwrap();

    for i in read_buffer[0..BLOCK_SIZE].iter() {
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

    let a = &(0..BLOCK_SIZE * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    let b = &(0..BLOCK_SIZE * 3).map(|_| 'b' as u8).collect::<Vec<_>>()[..];
    write_buffer_a[0..BLOCK_SIZE * 3].copy_from_slice(a);
    write_buffer_b[0..BLOCK_SIZE * 3].copy_from_slice(b);

    znstarget.write(&write_buffer_a.slice(0..BLOCK_SIZE * 3), 0).unwrap();
    znstarget.write(&write_buffer_b.slice(0..BLOCK_SIZE * 3), 2).unwrap();
    znstarget.write(&write_buffer_a.slice(0..BLOCK_SIZE * 3), 90000).unwrap();
    
    znstarget.read(&read_buffer.slice(0..BLOCK_SIZE * 3), 0).unwrap();
    for i in read_buffer[..2 * BLOCK_SIZE].iter() {
        assert_eq!(*i, 'a' as u8);
    }
    for i in read_buffer[2 * BLOCK_SIZE..3 * BLOCK_SIZE].iter() {
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

    let a = &(0..BLOCK_SIZE * 3).map(|_| 'a' as u8).collect::<Vec<_>>()[..];
    write_buffer_a[0..BLOCK_SIZE * 3].copy_from_slice(a);

    for _ in 0..ZCAP - 1 {
        znstarget.write(&write_buffer_a.slice(0..BLOCK_SIZE), 0).unwrap();
    }
    znstarget.write(&write_buffer_a.slice(0..3 * BLOCK_SIZE), 0).unwrap();
    
    znstarget.read(&read_buffer.slice(0..BLOCK_SIZE * 3), 0).unwrap();
    for i in read_buffer[0..BLOCK_SIZE * 3].iter() {
        assert_eq!(*i, 'a' as u8);
    }
}
