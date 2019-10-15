use super::*;

pub struct Proposer {
    peer_id: u8,
    num_peers: u8,
    quorum_size: u8,
    local_proposal: Option<bool>,
    proposal_id: ProposalId,
    highest_proposal_id: ProposalId,
    highest_accepted: Option<(ProposalId, bool)>,
    promises_received: u64,
    nacks_received: u64
}
impl Proposer {
    pub fn new(peer_id: u8, num_peers: u8, quorum_size: u8) -> Proposer {
        assert!(quorum_size >= num_peers/2 + 1);
        Proposer {
            peer_id,
            num_peers,
            quorum_size,
            local_proposal: None,
            proposal_id: ProposalId::initial_proposal_id(peer_id),
            highest_proposal_id: ProposalId::initial_proposal_id(peer_id),
            highest_accepted: None,
            promises_received: 0,
            nacks_received: 0
        }
    }

    pub fn prepare_quorum_reached(&self) -> bool {
        self.promises_received.count_ones() >= self.quorum_size as u32
    }
    pub fn have_local_proposal(&self) -> bool {
        self.local_proposal.is_some()
    }
    pub fn num_promises(&self) -> u32 { self.promises_received.count_ones() }
    pub fn num_nacks(&self) -> u32 { self.nacks_received.count_ones() }

    pub fn may_send_accept(&self) -> bool {
        let have_proposal = self.highest_accepted.is_some() || self.local_proposal.is_some();
        self.prepare_quorum_reached() && have_proposal
    }

    fn proposal_value(&self) -> Option<bool> {
        if self.prepare_quorum_reached() {
            match self.highest_accepted {
                Some(t) => Some(t.1),
                None => {
                    match self.local_proposal {
                        Some(v) => Some(v),
                        None => None
                    }
                }
            }
        } else {
            None
        }
    }
    /// Sets the proposal value for this node. Once set, it cannot be unset. Subsequent calls are ignored.
    pub fn set_local_proposal(&mut self, value: bool) {
        if self.local_proposal.is_none() {
            self.local_proposal = Some(value);
        }
    }

    /// Used to reduce the chance of receiving Nacks to our Prepare messages
    pub fn update_highest_proposal_id(&mut self, pid: ProposalId) {
        if pid > self.highest_proposal_id {
            self.highest_proposal_id = pid
        }
    }

    ///Abandons the current Paxos round and prepares for the next. 
    ///      
    /// After calling this method the current Prepare message will contain a ProposalID higher 
    /// than any previously seen and all internal state tracking will be reset to represent 
    /// the new round.
    pub fn next_round(&mut self) {
        self.proposal_id = ProposalId {
            number: self.highest_proposal_id.number + 1,
            peer: self.peer_id
        };
        self.highest_proposal_id = self.proposal_id;
        self.promises_received = 0;
        self.nacks_received = 0;
    }

    pub fn current_proposal_id(&self) -> ProposalId {
        self.proposal_id
    }

    pub fn current_prepare_message(&self) -> Prepare {
        Prepare {
            proposal_id: self.proposal_id
        }
    }

    /// Return value contains an Accept message only if we have reached the quorum threshold and we
    /// have a value to propose. 
    ///   
    /// The value may come either from the local proposal or from a peer via the Paxos proposal requirement
    pub fn current_accept_message(&self) -> Option<Accept> {
        self.proposal_value().map( |v| Accept {
            proposal_id: self.proposal_id,
            proposal_value: v
        })
    }

    /// Returns true if this Nack eliminates the possibility of success for the
    /// current proposal id. Returns false if success is still possible.
    pub fn receive_nack(
        &mut self, 
        from_peer: u8, 
        proposal_id: ProposalId, 
        promised_proposal_id: ProposalId) -> bool {
        
        self.update_highest_proposal_id(promised_proposal_id);

        if proposal_id == self.proposal_id {
            self.nacks_received |= 1 << from_peer;
        }

        self.nacks_received.count_ones() > (self.num_peers - self.quorum_size) as u32
    }

    pub fn receive_promise(
        &mut self,
        from_peer: u8,
        proposal_id: ProposalId,
        last_accepted: Option<(ProposalId, bool)>) {

        self.update_highest_proposal_id(proposal_id);

        if proposal_id == self.proposal_id {
            self.promises_received |= 1 << from_peer;

            if let Some((accepted_id, _accepted_value)) = last_accepted {
                match self.highest_accepted {
                    None => self.highest_accepted = last_accepted,
                    Some(highest) => {
                        if accepted_id > highest.0 {
                            self.highest_accepted = last_accepted
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;
    use crate::paxos::Message;

    #[test]
    fn proposal_id_comparisons() {
        assert!(ProposalId { number: 5, peer: 0 } > ProposalId { number: 4, peer: 2 });
        assert!(ProposalId { number: 5, peer: 1 } > ProposalId { number: 5, peer: 0 });
        assert!(ProposalId { number: 4, peer: 4 } < ProposalId { number: 5, peer: 0 });
        assert!(ProposalId { number: 4, peer: 4 } <= ProposalId { number: 4, peer: 4 });
        assert!(ProposalId { number: 4, peer: 4 } == ProposalId { number: 4, peer: 4 });
    }

    #[test]
    fn update_proposal_id() {
        let mut p = Proposer::new(0, 3, 2);
        assert_eq!(p.current_prepare_message(), Prepare {
            proposal_id: ProposalId::initial_proposal_id(0) 
        });
        p.update_highest_proposal_id(ProposalId{number: 4, peer: 3});
        p.next_round();
        assert_eq!(p.current_prepare_message(), Prepare {
            proposal_id: ProposalId { number: 5, peer: 0 }
        });
    }

    #[test]
    fn prepare_quorum_ignores_duplicate_promises() {
        let mut p = Proposer::new(0, 3, 2);

        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(1, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), true);
    }

    #[test]
    fn next_round_discards_previous_state() {
        let mut p = Proposer::new(0, 3, 2);
        p.set_local_proposal(true);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(1, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), true);

        assert_eq!(p.current_accept_message(), Some(Accept {
            proposal_id: ProposalId { number: 1, peer: 0 },
            proposal_value: true
        }));
        p.next_round();
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.current_accept_message(), None);
    }

    #[test]
    fn nack_handling() {
        let mut p = Proposer::new(0, 3, 2);
        p.set_local_proposal(true);
        p.next_round();
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.current_proposal_id(), ProposalId { number: 2, peer: 0 });
        p.receive_promise(0, ProposalId { number: 2, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.promises_received.count_ones(), 1);
        p.receive_nack(2, ProposalId { number: 2, peer: 0 }, ProposalId { number: 3, peer: 1 });
        assert_eq!(p.promises_received.count_ones(), 1);
        assert_eq!(p.nacks_received.count_ones(), 1);
        p.receive_promise(1, ProposalId { number: 2, peer: 0 }, None);
        assert!(p.prepare_quorum_reached());

        p.next_round();
        assert!(!p.prepare_quorum_reached());
        assert_eq!(p.current_accept_message(), None);
        assert_eq!(p.nacks_received.count_ones(), 0);
        assert_eq!(p.promises_received.count_ones(), 0);

        assert_eq!(p.receive_nack(2, ProposalId { number: 4, peer: 0 }, 
          ProposalId { number: 5, peer: 1 }), false);
        assert_eq!(p.receive_nack(2, ProposalId { number: 4, peer: 0 }, 
          ProposalId { number: 5, peer: 1 }), false);
        assert_eq!(p.receive_nack(1, ProposalId { number: 4, peer: 0 }, 
          ProposalId { number: 5, peer: 1 }), true);
    }

     #[test]
    fn leadership_acquisition_before_local_value_proposal() {
        let mut p = Proposer::new(0, 3, 2);

        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        p.receive_promise(1, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), true);
        assert_eq!(p.current_accept_message(), None);
        p.set_local_proposal(false);
        assert_eq!(p.current_accept_message(), Some(Accept {
            proposal_id: ProposalId { number: 1, peer: 0 },
            proposal_value: false
        }));
    }

    #[test]
    fn leadership_acquisition_after_local_value_proposal() {
        let mut p = Proposer::new(0, 3, 2);
        p.set_local_proposal(false);
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(0, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(1, ProposalId { number: 1, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), true);
        assert_eq!(p.current_accept_message(), Some(Accept {
            proposal_id: ProposalId { number: 1, peer: 0 },
            proposal_value: false
        }));
    }

    #[test]
    fn leadership_acquisition_with_already_accepted_proposals() {
        let mut p = Proposer::new(0, 3, 2);
        p.next_round();
        
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(0, ProposalId { number: 2, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(1, ProposalId { number: 2, peer: 0 }, 
            Some((ProposalId{ number: 1, peer: 1}, false)));
        assert_eq!(p.prepare_quorum_reached(), true);
        assert_eq!(p.current_accept_message(), Some(Accept {
            proposal_id: ProposalId { number: 2, peer: 0 },
            proposal_value: false
        }));
    }

    #[test]
    fn leadership_acquisition_with_already_accepted_proposals_overrides_local_proposal() {
        let mut p = Proposer::new(0, 3, 2);
        p.set_local_proposal(true);
        p.next_round();
        
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(0, ProposalId { number: 2, peer: 0 }, None);
        assert_eq!(p.prepare_quorum_reached(), false);
        assert_eq!(p.current_accept_message(), None);
        p.receive_promise(1, ProposalId { number: 2, peer: 0 }, 
            Some((ProposalId{ number: 1, peer: 1}, false)));
        assert_eq!(p.prepare_quorum_reached(), true);
        assert_eq!(p.current_accept_message(), Some(Accept {
            proposal_id: ProposalId { number: 2, peer: 0 },
            proposal_value: false
        }));
    }
}