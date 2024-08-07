use crate::NvmeDevice;
use crate::NvmeZNSInfo;
use crate::ZnsZsa;
use std::error::Error;

const ZNS_MAP_UNMAPPED: u64 = 0xFFFFFFFFFFFFFFFF;

struct ZNSMap {

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
        let d2l = vec![ZNS_MAP_UNMAPPED; n_blocks_logical]; //TODO I think this should be n_blocks_device
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

    // Looks up the longest contiguous block of unmapped blocks starting from the block mapped to lba
    // Returns the mapped block and the length of the contiguous unmapped blocks (potentially 0)
    pub fn lookup_unmapped(&self, lba: u64) -> (u64, u64) {
        let mut len = 0;
        let mut d_lba = self.l2d[lba as usize];
        while d_lba == ZNS_MAP_UNMAPPED {
            d_lba = self.l2d[(lba + len) as usize];
            len += 1;
        }
        (d_lba, len)
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
    slba: u64,
    invalid_blocks: u64
    //GC algorithms data will come here
}


impl MapperZone {
    pub fn new(slba: u64) -> Self {
        Self {
            slba,
            invalid_blocks: 0
        }
    }
}

pub struct ZNSTarget {

    backing: NvmeDevice, //Backing ZNS device
    max_lba: u64, //last exposed lba (that can be written into)
    exposed_zones: u64,
    zns_info: NvmeZNSInfo, 
    map: ZNSMap,
    // These are mutually exclusive, and the union of them all represents all zones in the backing device
    current_zone: MapperZone, //SLBA of the current zone
    free_zones: Vec<MapperZone>,
    full_zones: Vec<MapperZone>,
    op_zones: Vec<MapperZone>

}

impl ZNSTarget {

    pub fn init(op_rate: f32, backing: NvmeDevice) -> Result<Self, Box<dyn Error>> { //TODO backing ref
        if op_rate >= 1. || op_rate < 0. {
            return Err("Invalid overprovisioning rate".into())
        }
        let ns = backing.namespaces.get(&1).unwrap();
        let zns_info = match ns.zns_info {
            Some(info) => info,
            None => return Err("Not a ZNS device".into())
        };
        let exposed_zones = zns_info.n_zones * (1.0 - op_rate) as u64;
        let exposed_blocks = exposed_zones * zns_info.zone_size;
        let total_blocks = ns.blocks;

        let current_zone = MapperZone::new(0);

        let mut free_zones = Vec::new();
        for i in 1..exposed_zones {
            free_zones.push(MapperZone::new(i * zns_info.zone_size));
        }

        let mut op_zones = Vec::new();
        for i in exposed_zones..zns_info.n_zones {
            op_zones.push(MapperZone::new(i * zns_info.zone_size));
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

        if(lba + dest.len() as u64) > self.max_lba {
            return Err("Read out of bounds".into());
        }

        let block_size = self.backing.namespaces.get(&1).unwrap().block_size;
        let mut blocks = (dest.len() as u64 + block_size - 1) / block_size;
        let mut current_lba = lba;
        let mut current_array = dest;

        while blocks > 0 {
            let zone_boundary = ((current_lba / self.zns_info.zone_size) + 1) * self.zns_info.zone_size; // the next zone boundary
            let backing_block = self.map.lookup(current_lba);
            if backing_block == ZNS_MAP_UNMAPPED {
                return Err("Block not mapped".into());
            }

            let length = if blocks > (zone_boundary - current_lba) {
                zone_boundary - current_lba
            } else {
                blocks
            };

            let (first, rest) = current_array.split_at_mut((length * block_size) as usize);
            current_array = rest;

            self.backing.read_copied(first, backing_block)?;
            blocks -= length;
            current_lba += length;
        }

        Ok(())
    }

    pub fn write_copied(&mut self, data: &[u8],  lba: u64) -> Result<(), Box<dyn Error>> {

        if(lba + data.len() as u64) > self.max_lba {
            return Err("Write out of bounds".into());
        }

        let block_size = self.backing.namespaces.get(&1).unwrap().block_size;
        let mut blocks = (data.len() as u64 + block_size - 1) / block_size;
        let mut current_lba = lba;
        let mut current_array = data;

        while blocks > 0 {
            let zone_boundary = ((current_lba / self.zns_info.zone_size) + 1) * self.zns_info.zone_size; // the next zone boundary
            let backing_block = self.map.lookup(current_lba);

            let length = if blocks > (zone_boundary - current_lba) {
                zone_boundary - current_lba
            } else {
                blocks
            };

            let length_contiguous = self.map.lookup_contiguous_map(current_lba, length);
            let (first, rest) = current_array.split_at((length_contiguous * block_size) as usize);
            current_array = rest;

            let d_lba = self.backing.append_io(1, self.current_zone.slba, first)?;
            self.map.update_len(current_lba, d_lba, length_contiguous as u64);

            if backing_block != ZNS_MAP_UNMAPPED {
                self.map.mark_invalid_len(backing_block, length_contiguous);
                self.current_zone.invalid_blocks += length_contiguous;
                assert!(self.map.count_mapped(current_lba, length_contiguous) == length_contiguous);
            }

            blocks -= length_contiguous;
            current_lba += length_contiguous;

            if current_lba == zone_boundary {
                if self.free_zones.is_empty() {
                    self.reclaim()?;
                }
                match self.free_zones.pop() {
                    Some(zone) => {
                        let full_zone = std::mem::replace(&mut self.current_zone, zone);
                        self.full_zones.push(full_zone);
                    },
                    None => {
                        return Err("Despite reclaiming, no free zones available".into());
                    }
                }
            }
        }
        Ok(())
    }

    fn reclaim(&mut self) -> Result<(), Box<dyn Error>> {
        self.full_zones.sort_by(|a, b| a.invalid_blocks.cmp(&b.invalid_blocks));
        if self.full_zones.is_empty() {
            return Err("No full zones to reclaim; This shouldn't even happen?".into());
        }
        if self.op_zones.is_empty() && self.free_zones.is_empty() {
            return Err("No free zones to reclaim to".into());
        }

        let op_zone = if self.op_zones.is_empty() {
            //TODO I should probably think about this means and when it can happen
            self.free_zones.pop().unwrap()
        } else {
            self.op_zones.pop().unwrap()
        };
        let victim = self.full_zones.pop().unwrap(); //This should be an entire method depending on the victim selection method

        // Copy the valid data from the victim to the op zone
        let mut victim_block = victim.slba;
        while victim_block < victim.slba + self.zns_info.zone_size {
            let valid_len = self.map.lookup_contiguous_valid(victim_block, self.zns_info.zone_size);
            if valid_len == 0 {
                let invalid_len = self.map.lookup_contiguous_invalid(victim_block, self.zns_info.zone_size);
                victim_block += invalid_len;
            }
            //append valid_len blocks from victim to op_zone
        }
        // Remap the valid data from the victim to the op zone
        self.map.remap(victim.slba, op_zone.slba, self.zns_info.zone_size);
        // The victim block is now free and can be reset and added to the overprovisioning zones.
        // and The overprovisioning zone can now be used as a free zone
        self.backing.zone_action(1, victim.slba, false, ZnsZsa::ResetZone)?;
        self.op_zones.push(victim);
        self.free_zones.push(op_zone);
        
        Ok(())
    }
}