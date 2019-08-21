//! Implementation of the Paxos algorithm (single synod version)
//! 

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct ProposalId {
    /// Round number
    number: u32,

    /// Identifies which peer that generated the proposal. This is used for tie-breaking
    /// and preventing collisions which could break the algorithm
    peer: u8
}

#[derive(Debug)]
pub struct PersistentState {
    promised: Option<ProposalId>,
    accepted: Option<(ProposalId, bool)>
}