use crate::{NvmeDevice, NvmeQueuePair, NvmeZNSInfo, ZnsZsa};
use crate::memory::{Dma, DmaSlice};
use std::error::Error;
use std::sync::{Mutex, RwLock, Condvar};

const ZNS_MAP_UNMAPPED: u64 = 0xFFFFFFFFFFFFFFFF;

pub enum VictimSelectionMethod {
    InvalidBlocks,
    LRU
}

struct ZNSMap {
    l2d: Vec<u64>, //Logical to device mapping
    d2l: Vec<u64>, //Device to logical mapping, needed when copying zones for reclaiming
    invalid_bitmap: Vec<bool> //True means invalid
}

impl ZNSMap {

    //n_blocks_logical is the number of blocks in the exposed zones,
    //n_blocks_device is the number of blocks in the backing device
    pub fn init(n_blocks_logical: usize, n_blocks_device: usize) -> Self {
        let l2d = vec![ZNS_MAP_UNMAPPED; n_blocks_logical];
        let d2l = vec![ZNS_MAP_UNMAPPED; n_blocks_device];
        let invalid_bitmap = vec![false; n_blocks_device];
        Self {
            l2d,
            d2l,
            invalid_bitmap
        }
    }

    pub fn lookup(&self, lba: u64) -> u64 {
        self.l2d[lba as usize]
    }

    // Looks up the longest contiguous physical blocks that are mapped to logical blocks starting from lba
    pub fn lookup_contiguous_physical(&self, lba: u64, len: u64) -> Result<u64, Box<dyn Error>> {
        let mut result = 1;
        let d_lba_start = self.l2d[lba as usize];
        if d_lba_start == ZNS_MAP_UNMAPPED {
            return Err("Block not mapped".into());
        }
        for i in 1..len {
            let d_lba = self.l2d[(lba + i) as usize];
            if d_lba == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }
            if d_lba != d_lba_start + i {
                break;
            }
            result += 1;
        }
        Ok(result)
    }

    // Looks up the longest contiguous block of (mapped or unmapped depending on lba) blocks starting from lba
    pub fn lookup_contiguous_map(&self, lba: u64, len: u64) -> u64 {
        let mut result = 0;
        if self.l2d[lba as usize] != ZNS_MAP_UNMAPPED {
            for i in 0..len {
                if self.l2d[(lba + i) as usize] == ZNS_MAP_UNMAPPED {
                    break;
                }
                result += 1;
            }
        } else {
            for i in 0..len {
                if self.l2d[(lba + i) as usize] != ZNS_MAP_UNMAPPED {
                    break;
                }
                result += 1;
            }
        }
        return result;
    }

    pub fn count_mapped(&self, lba: u64, len: u64) -> u64 {
        let mut count = 0;
        for i in 0..len {
            if self.l2d[(lba + i) as usize] != ZNS_MAP_UNMAPPED {
                count += 1;
            }
        }
        count
    }

    pub fn update(&mut self, lba: u64, d_lba: u64) {
        self.l2d[lba as usize] = d_lba;
        self.d2l[d_lba as usize] = lba;
    }

    pub fn update_len(&mut self, lba: u64, d_lba: u64, len: u64) {
        for i in 0..len {
            self.l2d[(lba + i) as usize] = d_lba + i;
            self.d2l[(d_lba + i) as usize] = lba + i;
        }
    }

    // Arguments d_old and d_new are (backing) device LBAs
    // Maps blocks that are backed by d_old...d_old+len to be backed by d_new...d_new+len
    pub fn remap(&mut self, mut d_old: u64, mut d_new: u64, len: u64) {
        for _ in 0..len {
            let l_lba = self.d2l[d_old as usize];
            self.l2d[l_lba as usize] = d_new;
            self.d2l[d_new as usize] = l_lba;
            self.invalid_bitmap[d_old as usize] = false;
            self.invalid_bitmap[d_new as usize] = false;
            d_old += 1;
            d_new += 1;
        }
    }

    pub fn check_invalid(&self, d_lba: u64) -> bool {
        self.invalid_bitmap[d_lba as usize]
    }

    pub fn mark_invalid(&mut self, d_lba: u64) {
        self.invalid_bitmap[d_lba as usize] = true;
    }

    pub fn mark_invalid_len(&mut self, d_lba: u64, len: u64) {
        for i in 0..len {
            self.invalid_bitmap[(d_lba + i) as usize] = true;
        }
    }

    // Returns the number of contiguous valid blocks starting from d_lba and up to len blocks
    pub fn lookup_contiguous_valid(&self, d_lba: u64, len: u64) -> u64 {
        let mut result = 0;
        for i in 0..len {
            if self.check_invalid(d_lba + i) {
                break;
            }
            result += 1;
        }
        return result;
    }

    // Returns the number of contiguous invalid blocks starting from d_lba and up to len blocks
    pub fn lookup_contiguous_invalid(&self, d_lba: u64, len: u64) -> u64 {
        let mut result = 0;
        for i in 0..len {
            if !self.check_invalid(d_lba + i) {
                break;
            }
            result += 1;
        }
        return result;
    }
}

struct MapperZoneMetadata {
    //Victim selections algorithms data will come here
    invalid_blocks: u64,
    zone_age: u64,
}

impl MapperZoneMetadata {
    pub fn incr_invalid_blocks(&mut self, incr: u64) {
        self.invalid_blocks += incr;
    }
    pub fn reset(&mut self) {
        self.invalid_blocks = 0;
        self.zone_age = 0;
    }
}

struct MapperZone {
    zslba: u64,
    zone_cap: u64,
    wp: u64
}

impl MapperZone {
    pub fn new(zslba: u64, zone_cap: u64) -> Self {
        Self {
            zslba,
            zone_cap,
            wp: zslba
        }
    }
    pub fn incr_wp(&mut self, incr: u64) -> Result<(), Box<dyn Error>> {
        if self.wp + incr > self.zslba + self.zone_cap {
            return Err("Write pointer out of bounds".into());
        }
        self.wp += incr;
        Ok(())
    }

    pub fn is_full (&self) -> bool {
        self.wp == self.zslba + self.zone_cap
    }

    pub fn reset(&mut self) {
        self.wp = self.zslba;
    }
}

struct ZNSZones {
    free_zones: Vec<MapperZone>,
    full_zones: Vec<MapperZone>,
    op_zones: Vec<MapperZone>
}

impl ZNSZones {
    pub fn find_zone(&self, zslba: u64) -> Result<&MapperZone, Box<dyn Error>> {
        self.full_zones.iter()
            .chain(self.free_zones.iter())
            .find(|zone| zone.zslba == zslba).ok_or("Zone not found".into())
    }
    pub fn find_zone_mut(&mut self, zslba: u64) -> Result<&mut MapperZone, Box<dyn Error>> {
        self.full_zones.iter_mut()
            .chain(self.free_zones.iter_mut())
            .find(|zone| zone.zslba == zslba).ok_or("Zone not found".into())
    }
}

pub struct ZNSTarget {
    pub backing: NvmeDevice, //Backing ZNS device
    max_lba: u64, //Last exposed lba (that can be written into)
    exposed_zones: u64,
    ns_id: u32,
    block_size: u64,
    zns_info: NvmeZNSInfo, 
    map: Mutex<ZNSMap>,
    victim_selection_method: VictimSelectionMethod,
    zones: Mutex<ZNSZones>,
    zones_metadata: Vec<Mutex<MapperZoneMetadata>>,
    reclaim_locks: Vec<RwLock<()>>,
    reclaim_condition: Condvar
}

// TODO
unsafe impl Send for ZNSTarget {}
unsafe impl Sync for ZNSTarget {}

impl ZNSTarget {

    pub fn init(mut backing: NvmeDevice, ns_id: u32, op_rate: f32, victim_selection_method: VictimSelectionMethod) -> Result<Self, Box<dyn Error>> {
        if op_rate >= 1. || op_rate < 0. {
            return Err("Invalid overprovisioning rate".into())
        }
        let ns: &crate::NvmeNamespace = backing.namespaces.get(&ns_id).unwrap();
        let block_size = ns.block_size;
        let zns_info = match ns.zns_info {
            Some(info) => info,
            None => return Err("Not a ZNS device".into())
        };
        let exposed_zones = ((zns_info.n_zones as f32) * (1.0 - op_rate)) as u64;
        let exposed_blocks = exposed_zones * zns_info.zone_size;
        let total_blocks = ns.blocks;
        let zone_descriptors = backing.get_zone_descriptors(ns_id)?;

        let mut free_zones = Vec::new();
        for i in 0..exposed_zones {
            let zslba = i * zns_info.zone_size;
            free_zones.push(MapperZone::new(zslba, zone_descriptors[i as usize].zcap));
        }

        let mut op_zones = Vec::new();
        for i in exposed_zones..zns_info.n_zones {
            let zslba = i * zns_info.zone_size;
            op_zones.push(MapperZone::new(zslba, zone_descriptors[i as usize].zcap));        
        }

        let full_zones = Vec::new();

        let mut zone_meta = Vec::new();
        zone_meta.reserve(zns_info.n_zones as usize);
        for _ in 0..zns_info.n_zones {
            zone_meta.push(Mutex::new(MapperZoneMetadata {
                invalid_blocks: 0,
                zone_age: 0
            }));
        }

        let reclaim_locks = std::iter::repeat_with(|| RwLock::new(()))
            .take(zns_info.n_zones as usize)
            .collect();

        let dev = Self {
            backing,
            max_lba: exposed_blocks - 1,
            exposed_zones,
            ns_id,
            block_size,
            zns_info,
            map: Mutex::new(ZNSMap::init(exposed_blocks as usize, total_blocks as usize)),
            victim_selection_method,
            zones: Mutex::new(ZNSZones {
                free_zones,
                full_zones,
                op_zones
            }),
            zones_metadata: zone_meta,
            reclaim_locks,
            reclaim_condition: Condvar::new()
        };

        Ok(dev)
    }

    // TODO on this and the rest, bypass mutexes with into_inner since it's meant for single threaded use
    pub fn read(&mut self, dest: &Dma<u8>, lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (dest.size as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = dest;
        let mut rest;

        if(lba + blocks as u64) > self.max_lba {
            return Err("Read out of bounds".into());
        }

        while blocks > 0 {
            let backing_block = self.map.lock().unwrap().lookup(current_lba);
            if backing_block == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }

            // Find the zslba of the backing block
            let zslba: u64 = (backing_block / self.zns_info.zone_size) * self.zns_info.zone_size;
            let zone_boundary: u64 = zslba + self.zns_info.zone_size;
            match self.reclaim_locks[self.get_zone_number(zslba)].try_read() {
                Ok(_lock) => {
                    let length: u64 = Ord::min(blocks, zone_boundary - backing_block);
                    let length_contiguous = self.map.lock().unwrap().lookup_contiguous_physical(current_lba, length)?;
                    
                    let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.size);

                    self.backing.read(self.ns_id, &mut current_array.slice(0..split_index), backing_block)?;

                    rest = current_array.slice(split_index..current_array.size);
                    current_array = &rest;
                    blocks -= length_contiguous;
                    current_lba += length_contiguous;
                },
                Err(_) => {
                    let _unused = self.reclaim_locks[self.get_zone_number(zslba)].read().unwrap();
                    continue; // Need to restart the loop, mapping information also changed
                }
            };
        }

        Ok(())
    }

    pub fn read_copied(&mut self, dest: &mut [u8], lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (dest.len() as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = dest;

        if(lba + blocks as u64) > self.max_lba {
            return Err("Read out of bounds".into());
        }

        while blocks > 0 {
            let backing_block = self.map.lock().unwrap().lookup(current_lba);
            if backing_block == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }

            // Find the zslba of the backing block
            let zslba: u64 = (backing_block / self.zns_info.zone_size) * self.zns_info.zone_size;
            let zone_boundary = zslba + self.zns_info.zone_size;
            match self.reclaim_locks[self.get_zone_number(zslba)].try_read() {
                Ok(_lock) => {
                    let length: u64 = Ord::min(blocks, zone_boundary - backing_block);
                    let length_contiguous = self.map.lock().unwrap().lookup_contiguous_physical(current_lba, length)?;
                    
                    let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.len());
                    let (first, rest) = current_array.split_at_mut(split_index);
                    current_array = rest;
        
                    self.backing.read_copied(self.ns_id, first, backing_block)?;
                    blocks -= length_contiguous;
                    current_lba += length_contiguous;
                },
                Err(_) => {
                    let _unused = self.reclaim_locks[self.get_zone_number(zslba)].read().unwrap();
                    continue; // Need to restart the loop, mapping information also changed
                }
            };
        }

        Ok(())
    }

    pub fn write(&mut self, data: &Dma<u8>, lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (data.size as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = data;
        let mut rest;

        if (lba + blocks as u64) > self.max_lba {
            return Err("Write out of bounds".into());
        }

        while blocks > 0 {

            let mut current_zone = match self.zones.lock().unwrap().free_zones.pop() {
                Some(zone) => zone,
                None => {
                    return Err("No free zones for write".into());
                }
            };

            let zone_boundary = current_zone.zslba + current_zone.zone_cap;

            let length = Ord::min(blocks, zone_boundary - current_zone.wp);

            let map: std::sync::MutexGuard<ZNSMap> = self.map.lock().unwrap();
            let backing_block = map.lookup(current_lba);
            let length_contiguous = map.lookup_contiguous_map(current_lba, length);
            drop(map);

            let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.size);

            let d_lba = self.backing.append_io(1, current_zone.zslba, &current_array.slice(0..split_index))?;

            let mut map = self.map.lock().unwrap();
            if backing_block != ZNS_MAP_UNMAPPED {
                map.mark_invalid_len(backing_block, length_contiguous);
                let zone_number = self.get_zone_number(current_lba);
                self.zones_metadata[zone_number].lock().unwrap().incr_invalid_blocks(length_contiguous);
                assert!(map.count_mapped(current_lba, length_contiguous) == length_contiguous);
            }
            map.update_len(current_lba, d_lba, length_contiguous as u64);
            drop(map);

            current_zone.incr_wp(length_contiguous)?;

            blocks -= length_contiguous;
            current_lba += length_contiguous;

            rest = current_array.slice(split_index..current_array.size);
            current_array = &rest;

            if current_zone.is_full() {
                let mut zones = self.zones.lock().unwrap();
                zones.full_zones.push(current_zone);
                self.reclaim_condition.notify_all();
            }
            else {
                self.zones.lock().unwrap().free_zones.push(current_zone);
                }         
            }
        return Ok(())
    }

    pub fn write_copied(&mut self, data: &[u8],  lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (data.len() as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = data;

        if (lba + blocks as u64) > self.max_lba {
            return Err("Write out of bounds".into());
        }

        while blocks > 0 {

            let mut current_zone = match self.zones.lock().unwrap().free_zones.pop() {
                Some(zone) => zone,
                None => {
                    return Err("No free zones for write".into());
                }
            };

            let zone_boundary = current_zone.zslba + current_zone.zone_cap;

            let length = Ord::min(blocks, zone_boundary - current_zone.wp);

            let map: std::sync::MutexGuard<ZNSMap> = self.map.lock().unwrap();
            let backing_block = map.lookup(current_lba);
            let length_contiguous = map.lookup_contiguous_map(current_lba, length);
            drop(map);

            let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.len());
            let (first, rest) = current_array.split_at(split_index);
            current_array = rest;

            let d_lba = self.backing.append_io_copied(1, current_zone.zslba, first)?;

            let mut map = self.map.lock().unwrap();
            if backing_block != ZNS_MAP_UNMAPPED {
                map.mark_invalid_len(backing_block, length_contiguous);
                let zone_number = self.get_zone_number(current_lba);
                self.zones_metadata[zone_number].lock().unwrap().incr_invalid_blocks(length_contiguous);
                assert!(map.count_mapped(current_lba, length_contiguous) == length_contiguous);
            }
            map.update_len(current_lba, d_lba, length_contiguous as u64);
            drop(map);

            current_zone.incr_wp(length_contiguous)?;

            blocks -= length_contiguous;
            current_lba += length_contiguous;

            if current_zone.is_full() {
                let mut zones = self.zones.lock().unwrap();
                zones.full_zones.push(current_zone);
                self.reclaim_condition.notify_all();
            }
            else {
                self.zones.lock().unwrap().free_zones.push(current_zone);
            }         
        }
        return Ok(())
    }

    fn pick_victim(&self) -> Result<MapperZone, Box<dyn Error>> {
        match self.victim_selection_method {
            VictimSelectionMethod::InvalidBlocks => {
                let mut zones = self.zones.lock().unwrap();
                // TODO looks cursed tbh
                zones.full_zones.sort_by(|a, b| {
                    let a_meta = self.zones_metadata[self.get_zone_number(a.zslba)].lock().unwrap();
                    let b_meta = self.zones_metadata[self.get_zone_number(b.zslba)].lock().unwrap();
                    a_meta.invalid_blocks.cmp(&b_meta.invalid_blocks)
                });
                Ok(zones.full_zones.pop().unwrap())
            },
            VictimSelectionMethod::LRU => {
                Err("LRU not implemented".into())
            }
        }
    }

    pub fn reclaim(&self, nvme_queue_pair: &mut NvmeQueuePair) -> Result<(), Box<dyn Error>> {

        let mut zones = self
            .reclaim_condition
            .wait_while(self.zones.lock().unwrap(), |zones| {
                zones.free_zones.len() > zones.full_zones.len()
            })
            .unwrap();

        if zones.op_zones.is_empty() && zones.free_zones.is_empty() {
            return Err("No free zones to reclaim to".into());
        }

        let mut op_zone = if zones.op_zones.is_empty() {

            //TODO I should probably think about this means and when it can happen
            eprintln!("No op zones, using free zone. I'm curious if this ever happens lol");
            zones.free_zones.pop().unwrap()
        } else {
            zones.op_zones.pop().unwrap()
        };

        drop(zones);

        let mut victim = self.pick_victim()?;
        let victim_zone_number = self.get_zone_number(victim.zslba);
        let mut victim_metadata = self.zones_metadata[victim_zone_number].lock().unwrap();
        
        if victim_metadata.invalid_blocks == 0 {
            return Ok(());
        }

        // Need to lock reads to the victim
        let _lock = self.reclaim_locks[victim_zone_number].write().unwrap();

        // Copy the valid data from the victim to the op zone
        let mut victim_block = victim.zslba;
        while victim_block < victim.zslba + victim.zone_cap {
            let valid_len = self.map.lock().unwrap().lookup_contiguous_valid(victim_block, victim.zone_cap);
            if valid_len == 0 {
                let invalid_len = self.map.lock().unwrap().lookup_contiguous_invalid(victim_block, victim.zone_cap);
                victim_block += invalid_len;
            }
            else {
                // Append valid_len blocks from victim to op_zone and update wp
                // Note: this is making the assumptions that all zones have the same capacity
                // TODO replace with copy
                let mut data : Dma<u8> = Dma::allocate((valid_len * self.block_size) as usize)?;
                nvme_queue_pair.submit_io(self.ns_id, self.block_size, &mut data, victim_block, false);
                nvme_queue_pair.complete_io(1);
                nvme_queue_pair.submit_io(self.ns_id, self.block_size, &mut data, op_zone.zslba, true);
                nvme_queue_pair.complete_io(1);
                assert!(op_zone.wp == op_zone.zslba);
                op_zone.incr_wp(valid_len)?;
                victim_block += valid_len;
            }
        }

        self.map.lock().unwrap().remap(victim.zslba, op_zone.zslba, victim.zone_cap);
        self.zones.lock().unwrap().free_zones.push(op_zone);
        drop(_lock); // Remap is done, we can unlock the victim

        // The victim block is now free and can be reset and added to the overprovisioning zones.
        // and The overprovisioning zone can now be used as a free zone
        nvme_queue_pair.zone_action(self.ns_id, victim.zslba, ZnsZsa::ResetZone);
        nvme_queue_pair.complete_io(1);
        victim.reset();
        victim_metadata.reset();
        self.zones.lock().unwrap().op_zones.push(victim);
        
        Ok(())
    }        

    pub fn read_concurrent(&self, nvme_queue_pair: &mut NvmeQueuePair, dest: &Dma<u8>, lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (dest.size as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = dest;
        let mut rest;

        if(lba + blocks as u64) > self.max_lba {
            return Err("Read out of bounds".into());
        }

        while blocks > 0 {
            let backing_block = self.map.lock().unwrap().lookup(current_lba);
            if backing_block == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }

            // Find the zslba of the backing block
            let zslba: u64 = (backing_block / self.zns_info.zone_size) * self.zns_info.zone_size;
            let zone_boundary = zslba + self.zns_info.zone_size;
            match self.reclaim_locks[self.get_zone_number(zslba)].try_read() {
                Ok(_lock) => {
                    let length: u64 = Ord::min(blocks, zone_boundary - backing_block);
                    let length_contiguous = self.map.lock().unwrap().lookup_contiguous_physical(current_lba, length)?;
                    
                    let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.size);
        
                    nvme_queue_pair.submit_io(self.ns_id, self.block_size,&current_array.slice(0..split_index), backing_block, false);

                    rest = current_array.slice(split_index..current_array.size);
                    current_array = &rest;
                    blocks -= length_contiguous;
                    current_lba += length_contiguous;
                },
                Err(_) => {
                    let _unused = self.reclaim_locks[self.get_zone_number(zslba)].read().unwrap();
                    continue; // Need to restart the loop, mapping information also changed
                }
            };
        }

        Ok(())
    }

    pub fn write_concurrent(&self, nvme_queue_pair: &mut NvmeQueuePair, data: &Dma<u8>, lba: u64) -> Result<(), Box<dyn Error>> {

        let mut blocks = (data.size as u64 + self.block_size - 1) / self.block_size;
        let mut current_lba = lba;
        let mut current_array = data;
        let mut rest;

        if (lba + blocks as u64) > self.max_lba {
            return Err("Write out of bounds".into());
        }

        while blocks > 0 {

            let mut current_zone = match self.zones.lock().unwrap().free_zones.pop() {
                Some(zone) => zone,
                None => {
                    return Err("No free zones for write".into());
                }
            };

            let zone_boundary = current_zone.zslba + current_zone.zone_cap;

            let length = Ord::min(blocks, zone_boundary - current_zone.wp);

            let map: std::sync::MutexGuard<ZNSMap> = self.map.lock().unwrap();
            let length_contiguous = map.lookup_contiguous_map(current_lba, length);
            drop(map);

            let split_index = Ord::min((length_contiguous * self.block_size) as usize, current_array.size);

            // Idea ignore d_lba and assume it's the write pointer, should always work out? Worth a try
            // Otherwise qd > 1 is gonna be impossible :( qd1t1 qd32t1 / qd1t32
            nvme_queue_pair.append_io(self.ns_id, self.block_size, data, current_zone.zslba);

            let mut map = self.map.lock().unwrap();
            let backing_block = map.lookup(current_lba);
            if backing_block != ZNS_MAP_UNMAPPED {
                map.mark_invalid_len(backing_block, length_contiguous);
                let zone_number = self.get_zone_number(current_lba);
                self.zones_metadata[zone_number].lock().unwrap().incr_invalid_blocks(length_contiguous);
                assert!(map.count_mapped(current_lba, length_contiguous) == length_contiguous);
            }
            map.update_len(current_lba, current_zone.wp, length_contiguous as u64);
            drop(map);

            current_zone.incr_wp(length_contiguous)?;

            blocks -= length_contiguous;
            current_lba += length_contiguous;

            rest = current_array.slice(split_index..current_array.size);
            current_array = &rest;

            if current_zone.is_full() {
                self.zones.lock().unwrap().full_zones.push(current_zone);
                self.reclaim_condition.notify_all();
            }
            else {
                self.zones.lock().unwrap().free_zones.push(current_zone);
                }         
            }
            
        Ok(())
    }

    fn get_zone_number(&self, lba: u64) -> usize {
        (lba / self.zns_info.zone_size) as usize
    }
}