//! Implementation of the Paxos algorithm (single synod version)
//! 

pub mod learner;
pub mod acceptor;
pub mod proposer;

pub use learner::Learner;
pub use acceptor::Acceptor;
pub use proposer::Proposer;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
pub struct ProposalId {
    /// Round number
    pub number: u32,

    /// Identifies which peer that generated the proposal. This is used for tie-breaking
    /// and preventing collisions which could break the algorithm
    pub peer: u8
}

impl ProposalId {
    pub fn initial_proposal_id(peer_id: u8) -> ProposalId {
        ProposalId {
            number: 1,
            peer: peer_id
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PersistentState {
    pub promised: Option<ProposalId>,
    pub accepted: Option<(ProposalId, bool)>
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Prepare {
   pub  proposal_id: ProposalId
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Nack {
    pub from_peer: u8,
    pub proposal_id: ProposalId,
    pub promised_proposal_id: ProposalId
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Promise {
    pub from_peer: u8,
    pub proposal_id: ProposalId,
    pub last_accepted: Option<(ProposalId, bool)>
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Accept {
    pub proposal_id: ProposalId,
    pub proposal_value: bool
}

#[derive(Debug, Clone, Copy,  Eq, PartialEq)]
pub struct Accepted {
    pub from_peer: u8,
    pub proposal_id: ProposalId,
    pub proposal_value: bool
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
    Prepare(Prepare),
    Nack(Nack),
    Promise(Promise),
    Accept(Accept),
    Accepted(Accepted)
}



