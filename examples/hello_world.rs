use std::error::Error;
use std::{env, process};

pub fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example hello_world <pci bus id>");
            process::exit(1);
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;
    let ns_id = 1;
    //nvme.write_copied("hello world".as_bytes(), 0)?;
    nvme.zone_action(ns_id, 0, false, vroom::ZnsZsa::ResetZone)?;

    nvme.append_io_copied(ns_id, 0, "hello world".as_bytes())?;
    nvme.append_io_copied(ns_id, 0, "hello world".as_bytes())?;

    let mut dest = [0u8; 12];
    nvme.read_copied(ns_id, &mut dest, 0)?;

    println!("{}", std::str::from_utf8(&dest)?);

    nvme.read_copied(ns_id, &mut dest, 2)?;

    println!("{}", std::str::from_utf8(&dest)?);


    nvme.zone_action(ns_id, 0, false, vroom::ZnsZsa::CloseZone)?;

    Ok(())
}
