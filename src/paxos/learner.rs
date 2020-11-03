
use super::ProposalId;

pub struct Learner {
    quorum_size: u8,
    highest_proposal_id: Option<ProposalId>,
    accepted_peers: u64,
    resolved_value: Option<bool>
}

impl Learner {
    pub fn new(num_peers: u8, quorum_size: u8) -> Learner {
        assert!(quorum_size >= num_peers/2 + 1);
        Learner {
            quorum_size,
            highest_proposal_id: None,
            accepted_peers: 0,
            resolved_value: None
        }
    }

    pub fn final_value(&self) -> Option<bool> {
        self.resolved_value
    }
    
    pub fn receive_accepted(
        &mut self, 
        from_peer: u8, 
        proposal_id: ProposalId, 
        proposal_value: bool) -> Option<bool> {

        match self.resolved_value {
            Some(v) => {
                if proposal_id >= self.highest_proposal_id.unwrap() {
                    // If a peer gained permission to send an Accept message for a proposal id higher than the
                    // one we saw achieve resolution, the peer must have learned of the consensual value. We can
                    // therefore update our peer map to show that this peer has correctly accepted the final value
                    self.accepted_peers |= 1 << from_peer;
                }
                Some(v)
            },
            None => {

                let highest = match self.highest_proposal_id {
                    Some(p) => p,
                    None => {
                        self.highest_proposal_id = Some(proposal_id);
                        proposal_id
                    }
                };

                if proposal_id > highest {
                    self.accepted_peers = 0;
                    self.highest_proposal_id = Some(proposal_id);
                }

                if proposal_id == self.highest_proposal_id.unwrap() {
                    self.accepted_peers |= 1 << from_peer;
                }

                if self.accepted_peers.count_ones() >= self.quorum_size as u32 {
                    self.resolved_value = Some(proposal_value);
                }

                self.resolved_value
            }
        }
    }
}
#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn basic_resolution() {
        let mut l = Learner::new(3,2);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert_eq!(l.receive_accepted(1, ProposalId{number: 1, peer: 0}, true), Some(true));
        assert!(l.accepted_peers & 1 << 0 != 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 == 0);
        assert!(l.accepted_peers & 1 << 3 == 0);
    }

    #[test]
    fn ignore_duplicates() {
        let mut l = Learner::new(3,2);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert_eq!(l.receive_accepted(1, ProposalId{number: 1, peer: 0}, true), Some(true));
        assert!(l.accepted_peers & 1 << 0 != 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 == 0);
        assert!(l.accepted_peers & 1 << 3 == 0);
    }

    #[test]
    fn add_peer_bits_after_resolution() {
        let mut l = Learner::new(3,2);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert_eq!(l.receive_accepted(1, ProposalId{number: 1, peer: 0}, true), Some(true));
        assert!(l.accepted_peers & 1 << 0 != 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 == 0);
        assert!(l.accepted_peers & 1 << 3 == 0);

        assert_eq!(l.receive_accepted(2, ProposalId{number: 1, peer: 0}, true), Some(true));
        assert!(l.accepted_peers & 1 << 0 != 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 != 0);
        assert!(l.accepted_peers & 1 << 3 == 0);
    }

    #[test]
    fn watch_only_highest_round() {
        let mut l = Learner::new(3,2);
        assert_eq!(l.receive_accepted(0, ProposalId{number: 1, peer: 0}, true), None);
        assert!(l.accepted_peers & 1 << 0 != 0);

        assert_eq!(l.receive_accepted(1, ProposalId{number: 5, peer: 0}, true), None);
        assert!(l.accepted_peers & 1 << 0 == 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 == 0);

        assert_eq!(l.receive_accepted(2, ProposalId{number: 1, peer: 0}, true), None);
        assert!(l.accepted_peers & 1 << 0 == 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 == 0);

        assert_eq!(l.receive_accepted(2, ProposalId{number: 5, peer: 0}, true), Some(true));
        assert!(l.accepted_peers & 1 << 0 == 0);
        assert!(l.accepted_peers & 1 << 1 != 0);
        assert!(l.accepted_peers & 1 << 2 != 0);
        assert!(l.accepted_peers & 1 << 3 == 0);
    }
}