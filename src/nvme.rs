use crate::cmd::NvmeCommand;
use crate::memory::{Dma, DmaSlice};
use crate::pci::pci_map_resource;
use crate::queues::*;
use crate::zns::*;
use crate::{NvmeNamespace, NvmeZNSInfo, NvmeStats, HUGE_PAGE_SIZE, ZnsZsa};
use std::collections::HashMap;
use std::error::Error;
use std::hint::spin_loop;

// clippy doesnt like this
#[allow(unused, clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug)]
pub enum NvmeRegs32 {
    VS = 0x8,        // Version
    INTMS = 0xC,     // Interrupt Mask Set
    INTMC = 0x10,    // Interrupt Mask Clear
    CC = 0x14,       // Controller Configuration
    CSTS = 0x1C,     // Controller Status
    NSSR = 0x20,     // NVM Subsystem Reset
    AQA = 0x24,      // Admin Queue Attributes
    CMBLOC = 0x38,   // Contoller Memory Buffer Location
    CMBSZ = 0x3C,    // Controller Memory Buffer Size
    BPINFO = 0x40,   // Boot Partition Info
    BPRSEL = 0x44,   // Boot Partition Read Select
    BPMBL = 0x48,    // Bood Partition Memory Location
    CMBSTS = 0x58,   // Controller Memory Buffer Status
    PMRCAP = 0xE00,  // PMem Capabilities
    PMRCTL = 0xE04,  // PMem Region Control
    PMRSTS = 0xE08,  // PMem Region Status
    PMREBS = 0xE0C,  // PMem Elasticity Buffer Size
    PMRSWTP = 0xE10, // PMem Sustained Write Throughput
}

#[allow(unused, clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug)]
pub enum NvmeRegs64 {
    CAP = 0x0,      // Controller Capabilities
    ASQ = 0x28,     // Admin Submission Queue Base Address
    ACQ = 0x30,     // Admin Completion Queue Base Address
    CMBMSC = 0x50,  // Controller Memory Buffer Space Control
    PMRMSC = 0xE14, // Persistent Memory Buffer Space Control
}

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug)]
pub(crate) enum NvmeArrayRegs {
    SQyTDBL,
    CQyHDBL,
}

// who tf is abbreviating this stuff
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
struct IdentifyNamespaceData {
    pub nsze: u64,
    pub ncap: u64,
    nuse: u64,
    nsfeat: u8,
    pub nlbaf: u8,
    pub flbas: u8,
    mc: u8,
    dpc: u8,
    dps: u8,
    nmic: u8,
    rescap: u8,
    fpi: u8,
    dlfeat: u8,
    nawun: u16,
    nawupf: u16,
    nacwu: u16,
    nabsn: u16,
    nabo: u16,
    nabspf: u16,
    noiob: u16,
    nvmcap: u128,
    npwg: u16,
    npwa: u16,
    npdg: u16,
    npda: u16,
    nows: u16,
    pub mssrl: u16,
    pub mcl: u32,
    _rsvd1: [u8; 12],
    anagrpid: u32,
    _rsvd2: [u8; 3],
    nsattr: u8,
    nvmsetid: u16,
    endgid: u16,
    nguid: [u8; 16],
    eui64: u64,
    pub lba_format_support: [u32; 16],
    _rsvd3: [u8; 192],
    vendor_specific: [u8; 3712],
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
// Used for the copy command, experimental, only supports 1 source range (TODO)
pub struct SourceRangeEntriesDescriptorFormat0 {
    _reserved1: u64,
    slba: u64,
    n_blocks: u16,
    _reserved2: u16,
    rest: [u8; 4076]
}

pub struct NvmeQueuePair {
    pub id: u16,
    pub sub_queue: NvmeSubQueue,
    comp_queue: NvmeCompQueue,
}

unsafe impl Send for NvmeQueuePair {}
unsafe impl Sync for NvmeQueuePair {}

impl NvmeQueuePair {
    /// returns amount of requests pushed into submission queue
    pub fn submit_io(&mut self, ns_id: u32, block_size: u64, data: &impl DmaSlice, mut lba: u64, write: bool) -> usize {
        let mut reqs = 0;
        // TODO: contruct PRP list?
        for chunk in data.chunks(2 * 4096) {
            let blocks = (chunk.slice.len() as u64 + block_size - 1) / block_size; // No??

            let addr = chunk.phys_addr as u64;
            let bytes = blocks * block_size;
            let ptr1 = if bytes <= 4096 {
                0
            } else {
                addr + 4096 // self.page_size
            };

            let entry = if write {
                NvmeCommand::io_write(
                    self.id << 11 | self.sub_queue.tail as u16,
                    ns_id,
                    lba,
                    blocks as u16 - 1,
                    addr,
                    ptr1,
                )
            } else {
                NvmeCommand::io_read(
                    self.id << 11 | self.sub_queue.tail as u16,
                    ns_id,
                    lba,
                    blocks as u16 - 1,
                    addr,
                    ptr1,
                )
            };

            if let Some(tail) = self.sub_queue.submit_checked(entry) {
                unsafe {
                    std::ptr::write_volatile(self.sub_queue.doorbell as *mut u32, tail as u32);
                }
            } else {
                eprintln!("queue full");
                return reqs;
            }

            lba += blocks;
            reqs += 1;
        }
        reqs
    }

    pub fn append_io(&mut self, ns_id: u32, block_size: u64, data: &impl DmaSlice, zslba: u64) -> usize {
        let mut reqs = 0;
        for chunk in data.chunks(2 * 4096) {
            let blocks = (chunk.slice.len() as u64 + block_size - 1) / block_size;

            let addr = chunk.phys_addr as u64;
            let bytes = blocks * block_size;
            let ptr1 = if bytes <= 4096 {
                0
            } else {
                addr + 4096 // self.page_size
            };
            let entry = NvmeCommand::zone_append(
                self.id << 11 | self.sub_queue.tail as u16,
                ns_id, 
                zslba, 
                blocks as u16 - 1, 
                addr, 
                ptr1);
            
            if let Some(tail) = self.sub_queue.submit_checked(entry) {
                unsafe {
                    std::ptr::write_volatile(self.sub_queue.doorbell as *mut u32, tail as u32);
                }
            } else {
                eprintln!("queue full");
                return reqs;
            }
            reqs += 1;
        }
        reqs
    }

    pub fn copy(&mut self, ns_id: u32, mut src: u64, mut dest: u64, mut len: u64, buffer: &mut Dma<u8>) -> usize {
        assert!(buffer.size >= 4096);
        let mut reqs = 0;
        while len > 0 {
            let current_len = std::cmp::min(len, 128);
            let data = buffer.virt as *mut SourceRangeEntriesDescriptorFormat0;
            unsafe {
                (*data).slba = src;
                (*data).n_blocks = current_len as u16 - 1;
                (*data)._reserved1 = 0;
                (*data)._reserved2 = 0;
                (*data).rest = [0; 4076];
            }
            let ptr0 = buffer.phys as u64;

            let entry = NvmeCommand::copy(
                self.id << 11 | self.sub_queue.tail as u16,
                ns_id,
                dest,
                ptr0,
            );

            if let Some(tail) = self.sub_queue.submit_checked(entry) {
                unsafe {
                    std::ptr::write_volatile(self.sub_queue.doorbell as *mut u32, tail as u32);
                }
            } else {
                eprintln!("queue full");
                return reqs;
            }

            len -= current_len;
            src += current_len;
            dest += current_len;
            reqs += 1;
        }

        reqs
    }

    pub fn zone_action(&mut self, ns_id: u32, zslba: u64, za: ZnsZsa) {
		let entry = NvmeCommand::zone_management_send(
            self.id << 11 | self.sub_queue.tail as u16,
            ns_id, 
            zslba, 
            false, 
            za as u8,
            0);        
        let tail = self.sub_queue.submit_checked(entry).unwrap();
        unsafe {
            std::ptr::write_volatile(self.sub_queue.doorbell as *mut u32, tail as u32);
        }
    }


    // TODO: maybe return result
    pub fn complete_io(&mut self, n: usize) -> Option<u16> {
        assert!(n > 0);
        let (tail, c_entry, _) = self.comp_queue.complete_n(n);
        unsafe {
            std::ptr::write_volatile(self.comp_queue.doorbell as *mut u32, tail as u32);
        }
        self.sub_queue.head = c_entry.sq_head as usize;
        let status = c_entry.status >> 1;
        if status != 0 {
            eprintln!(
                "Status: 0x{:x}, Status Code 0x{:x}, Status Code Type: 0x{:x}",
                status,
                status & 0xFF,
                (status >> 8) & 0x7
            );
            eprintln!("{:?}", c_entry);
            return None;
        }
        Some(c_entry.sq_head)
    }

    pub fn quick_poll(&mut self) -> Option<()> {
        if let Some((tail, c_entry, _)) = self.comp_queue.complete() {
            unsafe {
                std::ptr::write_volatile(self.comp_queue.doorbell as *mut u32, tail as u32);
            }
            self.sub_queue.head = c_entry.sq_head as usize;
            let status = c_entry.status >> 1;
            if status != 0 {
                eprintln!(
                    "Status: 0x{:x}, Status Code 0x{:x}, Status Code Type: 0x{:x}",
                    status,
                    status & 0xFF,
                    (status >> 8) & 0x7
                );
                eprintln!("{:?}", c_entry);
            }
            return Some(());
        }
        None
    }
}

#[allow(unused)]
pub struct NvmeDevice {
    pci_addr: String,
    addr: *mut u8,
    len: usize,
    // Doorbell stride
    dstrd: u16,
    admin_sq: NvmeSubQueue,
    admin_cq: NvmeCompQueue,
    io_sq: NvmeSubQueue,
    io_cq: NvmeCompQueue,
    buffer: Dma<u8>,           // 2MiB of buffer
    prp_list: Dma<[u64; 512]>, // Address of PRP's, devices doesn't necessarily support 2MiB page sizes; 8 Bytes * 512 = 4096
    pub namespaces: HashMap<u32, NvmeNamespace>,
    pub stats: NvmeStats,
    q_id: u16,
}

// TODO
unsafe impl Send for NvmeDevice {}
unsafe impl Sync for NvmeDevice {}

#[allow(unused)]
impl NvmeDevice {
    pub fn init(pci_addr: &str) -> Result<Self, Box<dyn Error>> {
        let (addr, len) = pci_map_resource(pci_addr)?;
        let mut dev = Self {
            pci_addr: pci_addr.to_string(),
            addr,
            dstrd: {
                unsafe {
                    ((std::ptr::read_volatile(
                        (addr as usize + NvmeRegs64::CAP as usize) as *const u64,
                    ) >> 32)
                        & 0b1111) as u16
                }
            },
            len,
            admin_sq: NvmeSubQueue::new(QUEUE_LENGTH, 0)?,
            admin_cq: NvmeCompQueue::new(QUEUE_LENGTH, 0)?,
            io_sq: NvmeSubQueue::new(QUEUE_LENGTH, 0)?,
            io_cq: NvmeCompQueue::new(QUEUE_LENGTH, 0)?,
            buffer: Dma::allocate(crate::memory::HUGE_PAGE_SIZE)?,
            prp_list: Dma::allocate(8 * 512)?,
            namespaces: HashMap::new(),
            stats: NvmeStats::default(),
            q_id: 1,
        };

        for i in 1..512 {
            dev.prp_list[i - 1] = (dev.buffer.phys + i * 4096) as u64;
        }

        println!("CAP: 0x{:x}", dev.get_reg64(NvmeRegs64::CAP as u64));
        println!("VS: 0x{:x}", dev.get_reg32(NvmeRegs32::VS as u32));
        println!("CC: 0x{:x}", dev.get_reg32(NvmeRegs32::CC as u32));

        println!("Disabling controller");
        // Set Enable bit to 0
        let ctrl_config = dev.get_reg32(NvmeRegs32::CC as u32) & 0xFFFF_FFFE;
        dev.set_reg32(NvmeRegs32::CC as u32, ctrl_config);

        // Wait for not ready
        loop {
            let csts = dev.get_reg32(NvmeRegs32::CSTS as u32);
            if csts & 1 == 1 {
                spin_loop();
            } else {
                break;
            }
        }

        // Configure Admin Queues
        dev.set_reg64(NvmeRegs64::ASQ as u32, dev.admin_sq.get_addr() as u64);
        dev.set_reg64(NvmeRegs64::ACQ as u32, dev.admin_cq.get_addr() as u64);
        dev.set_reg32(
            NvmeRegs32::AQA as u32,
            (QUEUE_LENGTH as u32 - 1) << 16 | (QUEUE_LENGTH as u32 - 1),
        );

        // Configure other stuff
        // TODO: check css values
        let mut cc = dev.get_reg32(NvmeRegs32::CC as u32);
        // mask out reserved stuff
        cc &= 0xFF00_000F;
        // Set Completion (2^4 = 16 Bytes) and Submission Entry (2^6 = 64 Bytes) sizes
        cc |= (4 << 20) | (6 << 16);

        // This is normally sane, but QEMU nvme might be bugged? Both bits 6 and 7 of CAP.CSS are set?? 
        // Step 3 of controller initialization, setting CC.CSS according to CAP.CSS
        // if((dev.get_reg64(NvmeRegs64::CAP as u64) >> 37) & 0x80 != 0) {
        //     cc |= (7 << 4); // 111b
        // }
        if((dev.get_reg64(NvmeRegs64::CAP as u64) >> 37) & 0x40 != 0) {
            cc |= (6 << 4); // 110b
        }

        // Set Memory Page Size
        // let mpsmax = ((dev.get_reg64(NvmeRegs64::CAP as u64) >> 52) & 0xF) as u32;
        // cc |= (mpsmax << 7);
        // println!("MPS {}", (cc >> 7) & 0xF);
        println!("MPSMIN: {}", (dev.get_reg64(NvmeRegs64::CAP as u64) >> 48) & 0xF);

        dev.set_reg32(NvmeRegs32::CC as u32, cc);

        // Enable the controller
        println!("Enabling controller");
        let ctrl_config = dev.get_reg32(NvmeRegs32::CC as u32) | 1;
        dev.set_reg32(NvmeRegs32::CC as u32, ctrl_config);

        // wait for ready
        loop {
            let csts = dev.get_reg32(NvmeRegs32::CSTS as u32);
            if csts & 1 == 0 {
                spin_loop();
            } else {
                break;
            }
        }

        let q_id = dev.q_id;
        let addr = dev.io_cq.get_addr();
        println!("Requesting i/o completion queue");
        let comp = dev.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::create_io_completion_queue(c_id, q_id, addr, (QUEUE_LENGTH - 1) as u16)
        })?;
        let addr = dev.io_sq.get_addr();
        println!("Requesting i/o submission queue");
        let comp = dev.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::create_io_submission_queue(
                c_id,
                q_id,
                addr,
                (QUEUE_LENGTH - 1) as u16,
                q_id,
            )
        })?;
        dev.q_id += 1;

        dev.identify_controller()?;
        let ns = dev.identify_namespace_list(0);
        
        for n in ns {
            println!("ns_id: {n}");
            dev.identify_namespace(n);
        }
        
        if((dev.get_reg64(NvmeRegs64::CAP as u64) >> 37) & 0x40 != 0) {
            let zns_ns = dev.identify_zns_namespace_list(0);
            for n in zns_ns {
                println!("ns_id: {n} supports zns");
                dev.identify_zns_namespace(n)
            }
        }
        else {
            println!("ZNS is not supported!")
        }

        Ok(dev)
    }

    pub fn identify_controller(&mut self) -> Result<(), Box<dyn Error>> {
        println!("Trying to identify controller");
        let _entry = self.submit_and_complete_admin(NvmeCommand::identify_controller);

        println!("Dumping identify controller");
        let mut serial = String::new();
        let data = &self.buffer;

        for &b in &data[4..24] {
            if b == 0 {
                break;
            }
            serial.push(b as char);
        }

        let mut model = String::new();
        for &b in &data[24..64] {
            if b == 0 {
                break;
            }
            model.push(b as char);
        }

        let mut firmware = String::new();
        for &b in &data[64..72] {
            if b == 0 {
                break;
            }
            firmware.push(b as char);
        }

        println!(
            "  - Model: {} Serial: {} Firmware: {}",
            model.trim(),
            serial.trim(),
            firmware.trim()
        );

        Ok(())
    }

    // 1 to 1 Submission/Completion Queue Mapping
    pub fn create_io_queue_pair(&mut self, len: usize) -> Result<NvmeQueuePair, Box<dyn Error>> {
        let q_id = self.q_id;
        println!("Requesting i/o queue pair with id {q_id}");

        let offset = 0x1000 + ((4 << self.dstrd) * (2 * q_id + 1) as usize);
        assert!(offset <= self.len - 4, "SQ doorbell offset out of bounds");

        let dbl = self.addr as usize + offset;

        let comp_queue: NvmeCompQueue = NvmeCompQueue::new(len, dbl)?;
        let comp = self.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::create_io_completion_queue(
                c_id,
                q_id,
                comp_queue.get_addr(),
                (len - 1) as u16,
            )
        })?;

        let dbl = self.addr as usize + 0x1000 + ((4 << self.dstrd) * (2 * q_id) as usize);
        let sub_queue = NvmeSubQueue::new(len, dbl)?;
        let comp = self.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::create_io_submission_queue(
                c_id,
                q_id,
                sub_queue.get_addr(),
                (len - 1) as u16,
                q_id,
            )
        })?;

        self.q_id += 1;
        Ok(NvmeQueuePair {
            id: q_id,
            sub_queue,
            comp_queue,
        })
    }

    pub fn delete_io_queue_pair(&mut self, qpair: NvmeQueuePair) -> Result<(), Box<dyn Error>> {
        println!("Deleting i/o queue pair with id {}", qpair.id);
        self.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::delete_io_submission_queue(c_id, qpair.id)
        })?;
        self.submit_and_complete_admin(|c_id, _| {
            NvmeCommand::delete_io_completion_queue(c_id, qpair.id)
        })?;
        Ok(())
    }

    pub fn identify_namespace_list(&mut self, base: u32) -> Vec<u32> {
        self.submit_and_complete_admin(|c_id, addr| {
            NvmeCommand::identify_namespace_list(c_id, addr, base)
        });

        // TODO: idk bout this/don't hardcode len
        let data: &[u32] =
            unsafe { std::slice::from_raw_parts(self.buffer.virt as *const u32, 1024) };

        data.iter()
            .copied()
            .take_while(|&id| id != 0)
            .collect::<Vec<u32>>()
    }

    pub fn identify_zns_namespace_list(&mut self, base: u32) -> Vec<u32> {
        self.submit_and_complete_admin(|c_id, addr| {
            NvmeCommand::identify_zns_namespace_list(c_id, addr, base)
        });

        let data: &[u32] =
            unsafe { std::slice::from_raw_parts(self.buffer.virt as *const u32, 1024) };

        data.iter()
            .copied()
            .take_while(|&id| id != 0)
            .collect::<Vec<u32>>()
    }

    pub fn identify_namespace(&mut self, id: u32) -> NvmeNamespace {
        self.submit_and_complete_admin(|c_id, addr| {
            NvmeCommand::identify_namespace(c_id, addr, id)
        });

        let namespace_data: IdentifyNamespaceData =
            unsafe { *(self.buffer.virt as *const IdentifyNamespaceData) };

        // let namespace_data = unsafe { *tmp_buff.virt };
        let size = namespace_data.nsze;
        let blocks = namespace_data.ncap;

        // figure out block size
        //TODO this is actually making a big assumption, added assert to check
        let flba_idx = (namespace_data.flbas & 0xF); 
        assert!(namespace_data.nlbaf <= 16); 
        let flba_data = (namespace_data.lba_format_support[flba_idx as usize] >> 16) & 0xFF;
        let block_size = if !(9..32).contains(&flba_data) {
            0
        } else {
            1 << flba_data
        };

        // TODO: check metadata?
        println!("Namespace {id}, Size: {size}, Blocks: {blocks}, Block size: {block_size}");
        let mssrl = namespace_data.mssrl;
        let mcl = namespace_data.mcl;
        println!("Copy command mssrl {} and mcl {}", mssrl,mcl);
        let namespace = NvmeNamespace {
            id,
            blocks,
            block_size,
            flba_idx,
            zns_info : None
        };
        self.namespaces.insert(id, namespace);
        namespace
    }

    pub fn identify_zns_namespace(&mut self, id : u32) {
        self.submit_and_complete_admin(|c_id, addr| {
            NvmeCommand::identify_namespace_zns(c_id, addr, id)
        });
        
        let zns_data : IdentifyNamespaceZNSData =
            unsafe { *(self.buffer.virt as *const IdentifyNamespaceZNSData) };

        let ns = self.namespaces.get(&id).unwrap();
        let zone_size = (zns_data.lbafe[ns.flba_idx as usize] & 0xFFFF_FFFF) as u64;
        let n_zones = ns.blocks / zone_size;
        println!("Namespace {id}, Zone Size: {zone_size}, Number of Zones: {n_zones}");

        // See Figure 48 of the ZNS spec
        if((zns_data.zoc >> 1) & 1 == 1) {
            println!("Zones may randomly be marked as full?")
        }
        if(zns_data.zoc & 1 == 1) {
            println!("Zone capacity may change after a reset.")
        } else {
            println!("Zone capacity won't change.")
        }
        if(zns_data.ozcs & 1 == 1) {
            println!("Cross zone reads are supported.")
        }

        let zns_info = NvmeZNSInfo {
            zone_size,
            n_zones
        };
        self.namespaces.get_mut(&id).unwrap().zns_info = Some(zns_info);
    }

    pub fn write(
        &mut self, 
        ns_id: u32,
        data: &impl DmaSlice, 
        mut lba: u64) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        for chunk in data.chunks(2 * 4096) {
            let blocks = (chunk.slice.len() as u64 + ns.block_size - 1) / ns.block_size;
            self.namespace_io(ns_id, blocks, lba, chunk.phys_addr as u64, true)?;
            lba += blocks;
        }

        Ok(())
    }

    pub fn read(
        &mut self, 
        ns_id: u32,
        dest: &impl DmaSlice, 
        mut lba: u64
    ) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        for chunk in dest.chunks(2 * 4096) {
            let blocks = (chunk.slice.len() as u64 + ns.block_size - 1) / ns.block_size;
            self.namespace_io(ns_id, blocks, lba, chunk.phys_addr as u64, false)?;
            lba += blocks;
        }
        Ok(())
    }

    pub fn write_copied(
        &mut self, 
        ns_id: u32, 
        data: &[u8], 
        mut lba: u64
    ) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        for chunk in data.chunks(128 * 4096) {
            self.buffer[..chunk.len()].copy_from_slice(chunk);
            let blocks = (chunk.len() as u64 + ns.block_size - 1) / ns.block_size;
            self.namespace_io(ns_id, blocks, lba, self.buffer.phys as u64, true)?;
            lba += blocks;
        }

        Ok(())
    }

    pub fn read_copied(
        &mut self,
        ns_id: u32,
        dest: &mut [u8],
        mut lba: u64,
    ) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        for chunk in dest.chunks_mut(128 * 4096) {
            let blocks = (chunk.len() as u64 + ns.block_size - 1) / ns.block_size;
            self.namespace_io(ns_id, blocks, lba, self.buffer.phys as u64, false)?;
            lba += blocks;
            chunk.copy_from_slice(&self.buffer[..chunk.len()]);
        }
        Ok(())
    }

    fn submit_io(
        &mut self,
        ns: &NvmeNamespace,
        addr: u64,
        blocks: u64,
        lba: u64,
        write: bool,
    ) -> Option<usize> {
        assert!(blocks > 0);
        assert!(blocks <= 0x1_0000);
        let q_id = 1;

        let bytes = blocks * ns.block_size;
        let ptr1 = self.get_prp2(bytes, addr);

        let entry = if write {
            NvmeCommand::io_write(
                self.io_sq.tail as u16,
                ns.id,
                lba,
                blocks as u16 - 1,
                addr,
                ptr1,
            )
        } else {
            NvmeCommand::io_read(
                self.io_sq.tail as u16,
                ns.id,
                lba,
                blocks as u16 - 1,
                addr,
                ptr1,
            )
        };
        self.io_sq.submit_checked(entry)
    }

    fn complete_io(&mut self, step: u64) -> Result<NvmeCompletion, NvmeCompletion> {
        let q_id = 1;

        let (tail, c_entry, _) = self.io_cq.complete_n(step as usize);
        self.write_reg_idx(NvmeArrayRegs::CQyHDBL, q_id as u16, tail as u32);

        let status = c_entry.status >> 1;
        if status != 0 {
            eprintln!(
                "Status: 0x{:x}, Status Code 0x{:x}, Status Code Type: 0x{:x}",
                status,
                status & 0xFF,
                (status >> 8) & 0x7
            );
            eprintln!("{:?}", c_entry);
            return Err(c_entry);
        }
        self.stats.completions += 1;
        Ok(c_entry)
    }

    pub fn batched_write(
        &mut self,
        ns_id: u32,
        data: &[u8],
        mut lba: u64,
        batch_len: u64,
    ) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        let q_id = 1;

        for chunk in data.chunks(HUGE_PAGE_SIZE) {
            self.buffer[..chunk.len()].copy_from_slice(chunk);
            let tail = self.io_sq.tail;

            let batch_len = std::cmp::min(batch_len, chunk.len() as u64 / ns.block_size);
            let batch_size = chunk.len() as u64 / batch_len;
            let blocks = batch_size / ns.block_size;

            for i in 0..batch_len {
                if let Some(tail) = self.submit_io(
                    &ns,
                    self.buffer.phys as u64 + i * batch_size,
                    blocks,
                    lba,
                    true,
                ) {
                    self.stats.submissions += 1;
                    self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);
                } else {
                    eprintln!("tail: {tail}, batch_len: {batch_len}, batch_size: {batch_size}, blocks: {blocks}");
                }
                lba += blocks;
            }
            self.io_sq.head = self.complete_io(batch_len).unwrap().sq_head as usize;
        }

        Ok(())
    }

    pub fn batched_read(
        &mut self,
        ns_id: u32,
        data: &mut [u8],
        mut lba: u64,
        batch_len: u64,
    ) -> Result<(), Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        let q_id = 1;

        for chunk in data.chunks_mut(HUGE_PAGE_SIZE) {
            let tail = self.io_sq.tail;

            let batch_len = std::cmp::min(batch_len, chunk.len() as u64 / ns.block_size);
            let batch_size = chunk.len() as u64 / batch_len;
            let blocks = batch_size / ns.block_size;

            for i in 0..batch_len {
                if let Some(tail) = self.submit_io(
                    &ns,
                    self.buffer.phys as u64 + i * batch_size,
                    blocks,
                    lba,
                    false,
                ) {
                    self.stats.submissions += 1;
                    self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);
                } else {
                    eprintln!("tail: {tail}, batch_len: {batch_len}, batch_size: {batch_size}, blocks: {blocks}");
                }
                lba += blocks;
            }
            self.io_sq.head = self.complete_io(batch_len).unwrap().sq_head as usize;
            chunk.copy_from_slice(&self.buffer[..chunk.len()]);
        }
        Ok(())
    }

    #[inline(always)]
    fn namespace_io(
        &mut self,
        ns_id: u32,
        blocks: u64,
        lba: u64,
        addr: u64,
        write: bool,
    ) -> Result<(), Box<dyn Error>> {
        assert!(blocks > 0);
        assert!(blocks <= 0x1_0000);

        let q_id = 1;
        let ns = *self.namespaces.get(&ns_id).unwrap();

        let bytes = blocks * ns.block_size;
        let ptr1 = self.get_prp2(bytes, addr);

        let entry = if write {
            NvmeCommand::io_write(
                self.io_sq.tail as u16,
                ns_id,
                lba,
                blocks as u16 - 1,
                addr,
                ptr1,
            )
        } else {
            NvmeCommand::io_read(
                self.io_sq.tail as u16,
                ns_id,
                lba,
                blocks as u16 - 1,
                addr,
                ptr1,
            )
        };

        let tail = self.io_sq.submit(entry);
        self.stats.submissions += 1;

        self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);
        self.io_sq.head = self.complete_io(1).unwrap().sq_head as usize;
        Ok(())
    }

    fn submit_and_complete_admin<F: FnOnce(u16, usize) -> NvmeCommand>(
        &mut self,
        cmd_init: F,
    ) -> Result<NvmeCompletion, Box<dyn Error>> {
        let cid = self.admin_sq.tail;
        let tail = self.admin_sq.submit(cmd_init(cid as u16, self.buffer.phys));
        self.write_reg_idx(NvmeArrayRegs::SQyTDBL, 0, tail as u32);

        let (head, entry, _) = self.admin_cq.complete_spin();
        self.write_reg_idx(NvmeArrayRegs::CQyHDBL, 0, head as u32);
        let status = entry.status >> 1;
        if status != 0 {
            eprintln!(
                "Status: 0x{:x}, Status Code 0x{:x}, Status Code Type: 0x{:x}",
                status,
                status & 0xFF,
                (status >> 8) & 0x7
            );
            return Err("Requesting i/o completion queue failed".into());
        }
        Ok(entry)
    }

    pub fn clear_namespace(&mut self, ns_id: Option<u32>) {
        let ns_id = if let Some(ns_id) = ns_id {
            assert!(self.namespaces.contains_key(&ns_id));
            ns_id
        } else {
            0xFFFF_FFFF
        };
        self.submit_and_complete_admin(|c_id, _| NvmeCommand::format_nvm(c_id, ns_id));
    }

    // Unfortunately not supported by the WD ZNS SSD :(
    // TODO maybe use MCL instead of 128
    pub fn copy(&mut self, ns_id: u32, mut src: u64, mut dest: u64, mut len: u64) -> Result<(), Box<dyn Error>> {
        while len > 0 {
            let current_len = std::cmp::min(len, 128);
            let mut data = self.buffer.virt as *mut SourceRangeEntriesDescriptorFormat0;
            unsafe {
                (*data).slba = src;
                (*data).n_blocks = current_len as u16 - 1;
                //TODO is this necessary?
                (*data)._reserved1 = 0;
                (*data)._reserved2 = 0;
                (*data).rest = [0; 4076];
            }
            let ptr0 = self.buffer.phys as u64;

            let entry = NvmeCommand::copy(self.io_sq.tail as u16, ns_id, dest, ptr0);
            let tail = self.io_sq.submit(entry);
            self.write_reg_idx(NvmeArrayRegs::SQyTDBL, 1, tail as u32);
            self.io_sq.head = self.complete_io(1).unwrap().sq_head as usize;

            len -= current_len;
            src += current_len;
            dest += current_len;
        }

        Ok(())
    }

    // ZNS specific commands

    // Zone Report Data Structure
    // See Section 3.4.2.2.1 and Figure 35 of the ZNS NVME Specification
    pub fn get_zone_reports(
        &mut self,
        ns_id: u32,
    ) -> Result<(), Box<dyn Error>> {
        let zones = self.namespaces.get(&ns_id).unwrap().zns_info.unwrap().n_zones;
        let n_dwords = (zones + 1) * 16; //64 bytes per zone descriptor structure
        self.zns_zone_mgmt_rcv(ns_id, 0, n_dwords as u32, 0, 0, true)?;        
        let nr_zones = unsafe { *(self.buffer.virt as *const u64) };
        println!("nr_zones: {}", nr_zones);

        for i in 0..nr_zones {
            let offset = (i + 1) * 64;
            let data = unsafe { *(self.buffer.virt.add(offset as usize) as *const ZoneDescriptorData)};
            let zslba = data.zslba;
            let wp = data.wp;
            let zcap = data.zcap;
            let zs = data.zs >> 4;
            println!("SLBA: 0x{:x}  WP: 0x{:x}  Cap: 0x{:x}   State: {}   Type: {}    Attrs:  0x{:x}", 
                    zslba, wp, zcap, zonestate_to_string(zs), zonetype_to_string(data.zt), data.za);
        }

        Ok(())
    }

    pub fn get_zone_descriptors(
        &mut self,
        ns_id: u32,
    ) -> Result<Vec<ZoneDescriptorData>, Box<dyn Error>> {
        let zones = self.namespaces.get(&ns_id).unwrap().zns_info.unwrap().n_zones;
        let n_dwords = (zones + 1) * 16; //64 bytes per zone descriptor structure
        self.zns_zone_mgmt_rcv(ns_id, 0, n_dwords as u32, 0, 0, true)?;        
        let nr_zones = unsafe { *(self.buffer.virt as *const u64) };

        let mut result: Vec<ZoneDescriptorData> = Vec::with_capacity(nr_zones as usize);

        for i in 0..nr_zones {
            let offset = (i + 1) * 64;
            let data = unsafe { *(self.buffer.virt.add(offset as usize) as *const ZoneDescriptorData)};
            result.push(data);
        }

        Ok(result)
    }

    pub fn zone_action(
        &mut self,
        ns_id: u32,
        slbda: u64,
        all_zones: bool,
        zsa: ZnsZsa
    ) -> Result<(), Box<dyn Error>> {
        self.zns_zone_mgmt_send(ns_id, slbda, all_zones, zsa as u8)?;
        Ok(())
    }

    pub fn append_io(
        &mut self,
        ns_id: u32,
        slba: u64,
        data: &impl DmaSlice
    ) -> Result<u64, Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        let mut is_first = true;
        let mut result = 0;
        for chunk in data.chunks(2 * 4096) {
            let blocks = (chunk.slice.len() as u64 + ns.block_size - 1) / ns.block_size;
            if is_first {
                result = self.zone_append(ns_id, slba, blocks as u16, chunk.phys_addr as u64)?;
                is_first = false;
            }
            else {
                self.zone_append(ns_id, slba, blocks as u16, chunk.phys_addr as u64)?;
            }
        }

        Ok(result)
    }

    // TODO: find out ZASL, most likely 32 * 4096
    pub fn append_io_copied(
        &mut self,
        ns_id: u32,
        slba: u64,
        data: &[u8]
    ) -> Result<u64, Box<dyn Error>> {
        let ns = *self.namespaces.get(&ns_id).unwrap();
        let mut is_first = true;
        let mut result = 0;
        for chunk in data.chunks(32 * 4096) {
            self.buffer[..chunk.len()].copy_from_slice(chunk);
            let blocks = (chunk.len() as u64 + ns.block_size - 1) / ns.block_size;
            if is_first {
                result = self.zone_append(ns_id, slba, blocks as u16, self.buffer.phys as u64)?;
                is_first = false;
            }
            else {
                self.zone_append(ns_id, slba, blocks as u16, self.buffer.phys as u64)?;
            }
        }

        Ok(result)
    }

    pub fn zone_append(
        &mut self,
        ns_id: u32,
        slba: u64,
        n_blocks: u16,
        addr: u64
    ) -> Result<u64, Box<dyn Error>> {
        
        let ns = *self.namespaces.get(&ns_id).unwrap();
        let bytes = (n_blocks as u64) * ns.block_size;
        let ptr1 = self.get_prp2(bytes, addr);

        let entry = NvmeCommand::zone_append(self.io_sq.tail as u16, ns_id, slba, n_blocks - 1, addr, ptr1);
        let q_id = 1;    
		let tail = self.io_sq.submit(entry);
        self.stats.submissions += 1;
		self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);

        match self.complete_io(1) {
            Ok(completion_entry) => {
                self.io_sq.head = completion_entry.sq_head as usize;
                let result = (completion_entry.command_specific2 as u64) << 32 | completion_entry.command_specific1 as u64;
                Ok(result)
            }
            Err(completion_entry) => match completion_entry.status & 0xFF {
                0xb9 => {
                    Err("Zone is full".into())
                }
                _ => {
                    Err("Zone append failed for some other reason".into())
                }
            }
        }
    }

    pub fn zns_zone_mgmt_rcv(
		&mut self,
		ns_id: u32,
		slba: u64,
		n_dwords: u32,
		zra: u8, 
		zra_field: u8, 
		zra_spec_feats: bool
	) -> Result<(), Box<dyn Error>> {

        let bytes = (n_dwords as u64) * 4;
        let ptr0 = self.buffer.phys as u64;
        let ptr1 = self.get_prp2(bytes, self.buffer.phys as u64);

		let entry = NvmeCommand::zone_management_rcv(
            self.io_sq.tail as u16, 
            ns_id, 
            slba, 
            n_dwords, 
            zra, 
            zra_field, 
            zra_spec_feats,
            ptr0,
            ptr1);

        let q_id = 1;    
		let tail = self.io_sq.submit(entry);
        self.stats.submissions += 1;
		self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);
		self.io_sq.head = self.complete_io(1).unwrap().sq_head as usize;

        Ok(())
	}

    pub fn zns_zone_mgmt_send(
		&mut self,
		ns_id: u32,
		slba: u64,
		select_all: bool,
		zsa: u8, 
	) -> Result<(), Box<dyn Error>> {

        let q_id = 1;
        let ptr0 = self.buffer.phys as u64;

		let entry = NvmeCommand::zone_management_send(
            self.io_sq.tail as u16, 
            ns_id, 
            slba, 
            select_all, 
            zsa,
            ptr0);

		let tail = self.io_sq.submit(entry);
        self.stats.submissions += 1;
		self.write_reg_idx(NvmeArrayRegs::SQyTDBL, q_id as u16, tail as u32);
		self.io_sq.head = self.complete_io(1).unwrap().sq_head as usize;

        Ok(())
	}

    
    /// Gets PRP2 value depending on the size of the data to be transferred
    fn get_prp2(&self, bytes : u64, addr: u64) -> u64 {
        if bytes <= 4096 {
            0
        } else if bytes <= 8192 {
            addr + 4096 // self.page_size
        } else {
            self.prp_list.phys as u64
        }
    }

    /// Sets Queue `qid` Tail Doorbell to `val`
    fn write_reg_idx(&self, reg: NvmeArrayRegs, qid: u16, val: u32) {
        match reg {
            NvmeArrayRegs::SQyTDBL => unsafe {
                std::ptr::write_volatile(
                    (self.addr as usize + 0x1000 + ((4 << self.dstrd) * (2 * qid)) as usize)
                        as *mut u32,
                    val,
                );
            },
            NvmeArrayRegs::CQyHDBL => unsafe {
                std::ptr::write_volatile(
                    (self.addr as usize + 0x1000 + ((4 << self.dstrd) * (2 * qid + 1)) as usize)
                        as *mut u32,
                    val,
                );
            },
        }
    }

    /// Sets the register at `self.addr` + `reg` to `value`.
    ///
    /// # Panics
    ///
    /// Panics if `self.addr` + `reg` does not belong to the mapped memory of the pci device.
    fn set_reg32(&self, reg: u32, value: u32) {
        assert!(reg as usize <= self.len - 4, "memory access out of bounds");

        unsafe {
            std::ptr::write_volatile((self.addr as usize + reg as usize) as *mut u32, value);
        }
    }

    /// Returns the register at `self.addr` + `reg`.
    ///
    /// # Panics
    ///
    /// Panics if `self.addr` + `reg` does not belong to the mapped memory of the pci device.
    fn get_reg32(&self, reg: u32) -> u32 {
        assert!(reg as usize <= self.len - 4, "memory access out of bounds");

        unsafe { std::ptr::read_volatile((self.addr as usize + reg as usize) as *mut u32) }
    }

    /// Sets the register at `self.addr` + `reg` to `value`.
    ///
    /// # Panics
    ///
    /// Panics if `self.addr` + `reg` does not belong to the mapped memory of the pci device.
    fn set_reg64(&self, reg: u32, value: u64) {
        assert!(reg as usize <= self.len - 8, "memory access out of bounds");

        unsafe {
            std::ptr::write_volatile((self.addr as usize + reg as usize) as *mut u64, value);
        }
    }

    /// Returns the register at `self.addr` + `reg`.
    ///
    /// # Panics
    ///
    /// Panics if `self.addr` + `reg` does not belong to the mapped memory of the pci device.
    fn get_reg64(&self, reg: u64) -> u64 {
        assert!(reg as usize <= self.len - 8, "memory access out of bounds");

        unsafe { std::ptr::read_volatile((self.addr as usize + reg as usize) as *mut u64) }
    }
}