use std::error::Error;
use std::{env, process};

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

    let nvme = vroom::init(&pci_addr)?;
    let mut zns_target = vroom::nonseq::ZNSTarget::init(0.3, nvme)?;

    zns_target.backing.zone_action(1, 0, true, vroom::ZnsZsa::ResetZone)?;

    const N_BLOCKS : usize = 2;
    let src1 = ['a' as u8; 4096 * N_BLOCKS];
    let src2 = ['b' as u8; 4096 * N_BLOCKS];

    zns_target.write_copied(&src1, 1)?; //TODO investigate

    zns_target.write_copied(&src2, 0)?;


    let mut dest = [0u8; 4096 * N_BLOCKS];
    zns_target.read_copied(&mut dest, 0)?;
    println!("{}", std::str::from_utf8(&dest)?);
    zns_target.backing.get_zone_reports(0)?;
    Ok(())
}