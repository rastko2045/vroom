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
    let mut znstarget = vroom::nonseq::ZNSTarget::init(0.3, nvme)?;

    znstarget.backing.zone_action(1, 0, true, vroom::ZnsZsa::ResetZone)?;

    const N_BLOCKS : usize = 100;
    let src1 = vec!('a' as u8; 4096 * N_BLOCKS);
    znstarget.backing.append_io(1, 0, &src1)?;

    //let src2 = ['b' as u8; 4096 * N_BLOCKS];

    //znstarget.write_copied(&src1, 0)?;

    //let mut dest = [0u8; 10];
    //znstarget.read_copied(&mut dest, 0)?;
    znstarget.backing.get_zone_reports(1)?;
    //println!("{}", std::str::from_utf8(&dest)?);
    Ok(())
}