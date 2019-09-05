//! Implementation of the Paxos algorithm (single synod version)
//! 

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct ProposalId {
    /// Round number
    pub number: u32,

    /// Identifies which peer that generated the proposal. This is used for tie-breaking
    /// and preventing collisions which could break the algorithm
    pub peer: u8
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PersistentState {
    pub promised: Option<ProposalId>,
    pub accepted: Option<(ProposalId, bool)>
}