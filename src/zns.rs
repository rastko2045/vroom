use std::vec;

// Identify Namespace Data Structure for the ZNS Command Set
// See section 4.1.5.1 and Figure 48 of the ZNS NVME specification.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
pub struct IdentifyNamespaceZNSData {
    zoc : u16,             // zone operation characteristics 
    ozcs : u16,            // optional zoned command support 
	mar : u32,             // maximum active resources 
	mor : u32,	           // maximum open resources  
	rrl : u32,	           // reset recommended limit
	frl : u32,	           // finish recommended limit  
	rrl1 : u32,            // reset recommended limit 1 
	rrl2 : u32,            // reset recommended limit 2 
	rrl3 : u32,            // reset recommended limit 3 
	frl1 : u32,            // finish recommended limit 1 
	frl2 : u32,            // finish recommended limit 2 
	frl3 : u32,            // finish recommended limit 3
	reserved : [u8; 2772], 
	pub lbafe : [u128; 64],    // zns lba format extension support 
	vendor_specific : [u8; 256]
}

// Zone Descriptor Data Structure
// See Section 3.4.2.2.3 and Figure 37 of the ZNS NVME Specification
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
pub struct ZoneDescriptorData {
    pub zt : u8,        // zone type
    pub zs : u8,        // zone state 
    pub za : u8,        // zone attributes 
    zai : u8,           // zone attributes information 
    _rsvd1 : u32,       // reserved
    pub zcap : u64,     // zone capacity
    pub zslba : u64,    // zone start logical block address 
    pub wp : u64,       // write pointer 
    _rsvd2 : [u8; 32]   // reserved 
}

pub fn zonetype_to_string(ztype: u8) -> &'static str {
	match ztype {
		2 => "Sequential Write Required",
		_ => "unknown"
	}
}

pub fn zonestate_to_string(zstate: u8) -> &'static str {
	match zstate {
		1 => "Empty",
		2 => "Implicitly Open",
		3 => "Explicitly Open",
		4 => "Closed",
		13 => "Read Only",
		14 => "Full",
		15 => "Offline",
		_ => "unknown"
	}
}