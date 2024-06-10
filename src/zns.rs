
// Zone Descriptor Data Structure
// See Section 3.4.2.2.3 and Figure 37 of the ZNS NVME Specification
// TODO prob require some compiler directive to make sure everything is in that order
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
struct ZoneDescriptorData {
    zt : u8,            // zone type
    zs : u8,            // zone state 
    za : u8,            // zone attributes 
    zai : u8,           // zone attributes information 
    _rsvd1 : u32,       // reserved
    zcap : u64,         // zone capacity
    zslba : u64,        // zone start logical block address 
    wp : u64,           // write pointer 
    _rsvd2 : [u8; 32]   // reserved 
}


// Identify Namespace Data Structure for the ZNS Command Set
// See section 4.1.5.1 and Figure 48 of the ZNS NVME specification.
// TODO prob require some compiler directive to make sure everything is in that order
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
#[allow(unused)]
struct IdentifyNamespaceZNSData {
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
	lbafe : [u128; 64],    // zns lba format extension support 
	vendor_specific : [u8; 256]
}