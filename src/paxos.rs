//! Implementation of the Paxos algorithm (single synod version)
//! 

pub mod learner;
pub mod acceptor;

pub use learner::Learner;
pub use acceptor::Acceptor;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
    Prepare {
        proposal_id: ProposalId
    },
    Nack {
        from_peer: u8,
        proposal_id: ProposalId,
        promised_proposal_id: ProposalId
    },
    Promise {
        from_peer: u8,
        proposal_id: ProposalId,
        last_accepted: Option<(ProposalId, bool)>
    },
    Accept {
        proposal_id: ProposalId,
        proposal_value: bool
    },
    Accepted {
        from_peer: u8,
        proposal_id: ProposalId,
        proposal_value: bool
    }
}


