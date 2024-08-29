use std::{env,process};
use vroom::{self, NvmeDevice};



pub fn get_pci_addr() -> String {
    env::var("NVME_ADDR").unwrap_or_else(|_| {
        eprintln!("Please set the NVME_ADDR environment variable.");
        process::exit(1);
    })
}

pub fn init_nvme(pci_addr: &str) -> NvmeDevice {
    vroom::init(pci_addr).unwrap_or_else(|e| {
        eprintln!("Initialization failed: {}", e);
        process::exit(1);
    })
}

pub fn init_zns_target(pci_addr: &str, ns_id: u32, op_rate: f32, method: vroom::nonseq::VictimSelectionMethod) -> vroom::nonseq::ZNSTarget {
    let nvme = init_nvme(pci_addr);
    vroom::nonseq::ZNSTarget::init(nvme, ns_id, op_rate, method).unwrap_or_else(|e| {
        eprintln!("Initialization failed: {}", e);
        process::exit(1);
    })
}