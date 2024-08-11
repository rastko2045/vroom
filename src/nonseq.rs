use crate::{NvmeDevice, NvmeZNSInfo, ZnsZsa};
use std::error::Error;

const ZNS_MAP_UNMAPPED: u64 = 0xFFFFFFFFFFFFFFFF;

pub struct ZNSMap {

    l2d: Vec<u64>, //Logical to device mapping
    d2l: Vec<u64>, //Device to logical mapping, needed when copying zones for reclaiming
    invalid_bitmap: Vec<bool> //True means invalid
    // TODO will probably need a mutex for thread safety

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

pub struct MapperZone {
    zslba: u64,
    zone_cap: u64, // Kinda unnecessary because all zones have the same size, but this is more convenient. Maybe TODO and refactor
    wp: u64,
    invalid_blocks: u64
    //GC algorithms data will come here
}

impl MapperZone {
    pub fn new(zslba: u64, zone_cap: u64) -> Self {
        Self {
            zslba,
            zone_cap,
            wp: zslba,
            invalid_blocks: 0
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
        self.invalid_blocks = 0;
    }
}

pub struct ZNSTarget {

    pub backing: NvmeDevice, //Backing ZNS device
    max_lba: u64, //last exposed lba (that can be written into)
    exposed_zones: u64,
    zns_info: NvmeZNSInfo, 
    pub map: ZNSMap, //TODO remove pub on this and ZNSMap, for debugging purposes
    // These are mutually exclusive, and the union of them all represents all zones in the backing device
    current_zone: Option<MapperZone>,
    free_zones: Vec<MapperZone>,
    full_zones: Vec<MapperZone>,
    op_zones: Vec<MapperZone>

}

impl ZNSTarget {

    //TODO Backing being moved is bothering me 
    pub fn init(op_rate: f32, mut backing: NvmeDevice) -> Result<Self, Box<dyn Error>> { //TODO backing ref
        if op_rate >= 1. || op_rate < 0. {
            return Err("Invalid overprovisioning rate".into())
        }
        let ns = backing.namespaces.get(&1).unwrap();
        let zns_info = match ns.zns_info {
            Some(info) => info,
            None => return Err("Not a ZNS device".into())
        };
        let exposed_zones = ((zns_info.n_zones as f32) * (1.0 - op_rate)) as u64;
        let exposed_blocks = exposed_zones * zns_info.zone_size;
        let total_blocks = ns.blocks;
        let zone_descriptors = backing.get_zone_descriptors(1)?;

        let current_zone = Some(MapperZone::new(0, zone_descriptors[0].zcap));

        let mut free_zones = Vec::new();
        for i in 1..exposed_zones {
            let zslba = i * zns_info.zone_size;
            free_zones.push(MapperZone::new(zslba, zone_descriptors[i as usize].zcap));
        }

        let mut op_zones = Vec::new();
        for i in exposed_zones..zns_info.n_zones {
            let zslba = i * zns_info.zone_size;
            op_zones.push(MapperZone::new(zslba, zone_descriptors[i as usize].zcap));        
        }

        let full_zones = Vec::new();
        let dev = Self {
            backing,
            max_lba: exposed_blocks - 1,
            exposed_zones,
            zns_info,
            map: ZNSMap::init(exposed_blocks as usize, total_blocks as usize),
            current_zone,
            free_zones,
            full_zones,
            op_zones
        };
        Ok(dev)
    }

    pub fn read_copied(&mut self, dest: &mut [u8], lba: u64) -> Result<(), Box<dyn Error>> {

        let block_size = self.backing.namespaces.get(&1).unwrap().block_size;
        let mut blocks = (dest.len() as u64 + block_size - 1) / block_size;
        let mut current_lba = lba;
        let mut current_array = dest;

        if(lba + blocks as u64) > self.max_lba {
            return Err("Read out of bounds".into());
        }

        while blocks > 0 {
            let backing_block = self.map.lookup(current_lba);
            if backing_block == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }

            // Find the zslba of the backing block
            let backing_zone = backing_block / self.zns_info.zone_size;
            let backing_zone_zslba: u64 = backing_zone * self.zns_info.zone_size;
            let zone_boundary = backing_zone_zslba + self.zns_info.zone_size;

            let length: u64 = Ord::min(blocks, zone_boundary - backing_block);
            let length_contiguous = self.map.lookup_contiguous_physical(current_lba, length)?;
            
            let split_index = Ord::min((length_contiguous * block_size) as usize, current_array.len());
            let (first, rest) = current_array.split_at_mut(split_index);
            current_array = rest;

            self.backing.read_copied(first, backing_block)?;
            blocks -= length_contiguous;
            current_lba += length_contiguous;
        }

        Ok(())
    }

    pub fn write_copied(&mut self, data: &[u8],  lba: u64) -> Result<(), Box<dyn Error>> {

        let block_size = self.backing.namespaces.get(&1).unwrap().block_size;
        let mut blocks = (data.len() as u64 + block_size - 1) / block_size;
        let mut current_lba = lba;
        let mut current_array = data;

        if (lba + blocks as u64) > self.max_lba {
            return Err("Write out of bounds".into());
        }

        while blocks > 0 {

            let current_zone = match &mut self.current_zone {
                Some(current_zone) => current_zone,
                None => return Err("Ran out of space!".into())
            };

            let zone_boundary = current_zone.zslba + current_zone.zone_cap;
            let backing_block = self.map.lookup(current_lba);

            let length = Ord::min(blocks, zone_boundary - current_zone.wp);
            let length_contiguous = self.map.lookup_contiguous_map(current_lba, length);

            let split_index = Ord::min((length_contiguous * block_size) as usize, current_array.len());
            let (first, rest) = current_array.split_at(split_index);
            current_array = rest;
            let d_lba = self.backing.append_io(1, current_zone.zslba, first)?;
            self.map.update_len(current_lba, d_lba, length_contiguous as u64);
            current_zone.incr_wp(length_contiguous)?;

            if backing_block != ZNS_MAP_UNMAPPED {
                self.map.mark_invalid_len(backing_block, length_contiguous);
                current_zone.invalid_blocks += length_contiguous;
                assert!(self.map.count_mapped(current_lba, length_contiguous) == length_contiguous);
            }

            blocks -= length_contiguous;
            current_lba += length_contiguous;

            if current_zone.is_full() {
                if self.free_zones.is_empty() {
                    self.reclaim()?;
                }
                match self.free_zones.pop() {
                    Some(zone) => {
                        let full_zone = std::mem::replace(&mut self.current_zone, Some(zone));
                        self.full_zones.push(full_zone.unwrap());
                    },
                    None => {
                        self.full_zones.push(self.current_zone.take().unwrap());
                    }
                }
            }
        }
        return Ok(())
    }

    fn reclaim(&mut self) -> Result<(), Box<dyn Error>> {
        
        self.full_zones.sort_by(|a, b| a.invalid_blocks.cmp(&b.invalid_blocks));
        
        if self.op_zones.is_empty() && self.free_zones.is_empty() {
            return Err("No free zones to reclaim to".into());
        }

        let mut op_zone = if self.op_zones.is_empty() {
            //TODO I should probably think about this means and when it can happen
            self.free_zones.pop().unwrap()
        } else {
            self.op_zones.pop().unwrap()
        };

        let mut victim = self.full_zones.pop().unwrap(); //This should be an entire method depending on the victim selection method
        if victim.invalid_blocks == 0 {
            return Ok(());
        }

        // Copy the valid data from the victim to the op zone
        let mut victim_block = victim.zslba;
        while victim_block < victim.zslba + victim.zone_cap {
            let valid_len = self.map.lookup_contiguous_valid(victim_block, victim.zone_cap);
            if valid_len == 0 {
                let invalid_len = self.map.lookup_contiguous_invalid(victim_block, victim.zone_cap);
                victim_block += invalid_len;
            }
            else {
                // Append valid_len blocks from victim to op_zone and update wp
                // Note: this is making the assumptions that all zones have the same capacity
                let block_size = self.backing.namespaces.get(&1).unwrap().block_size;
                let mut data = vec![0u8; (valid_len * block_size) as usize];
                self.backing.read_copied(&mut data, victim_block)?;
                self.backing.append_io(1, op_zone.zslba, &data)?;
                assert!(op_zone.wp == op_zone.zslba);
                op_zone.incr_wp(valid_len)?;
                victim_block += valid_len;
            }
        }

        self.map.remap(victim.zslba, op_zone.zslba, victim.zone_cap);

        // The victim block is now free and can be reset and added to the overprovisioning zones.
        // and The overprovisioning zone can now be used as a free zone
        self.backing.zone_action(1, victim.zslba, false, ZnsZsa::ResetZone)?;
        victim.reset();
        self.op_zones.push(victim);
        self.free_zones.push(op_zone);
        
        Ok(())
    }
}