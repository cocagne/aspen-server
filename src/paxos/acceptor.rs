use super::*;

pub struct Acceptor {
    peer_id: u8,
    state: PersistentState
}

impl Acceptor {
    pub fn new(peer_id: u8, initial_state: Option<PersistentState>) -> Acceptor {
        let state = match initial_state {
            Some(s) => s,
            None => PersistentState { promised: None, accepted: None }
        };
        Acceptor {
            peer_id,
            state
        }
    }

    pub fn load_saved_state(&mut self, saved_state: PersistentState) {
        self.state = saved_state;
    }

    pub fn persistent_state(&self) -> PersistentState {
        self.state
    }

    pub fn receive_prepare(&mut self, proposal_id: ProposalId) -> Result<Promise, Nack> {

        let promised = self.state.promised;

        let mut promise = || {
            self.state = PersistentState {
                promised: Some(proposal_id),
                accepted: self.state.accepted
            };
            Ok(Promise {
                from_peer: self.peer_id,
                proposal_id,
                last_accepted: self.state.accepted
            })
        };

        match promised {
            None => promise(),
            Some(promised_id) => {
                if proposal_id >= promised_id {
                    promise()
                } else {
                    Err(Nack { 
                        from_peer: self.peer_id,
                        proposal_id,
                        promised_proposal_id: promised_id
                    })
                }
            }
        }
    }

    pub fn receive_accept(
        &mut self, 
        proposal_id: ProposalId, 
        proposal_value: bool) -> Result<Accepted, Nack> {

        let promised = self.state.promised;

        let mut accept = || {
            self.state = PersistentState {
                promised: Some(proposal_id),
                accepted: Some((proposal_id, proposal_value))
            };
            Ok(Accepted {
                from_peer: self.peer_id,
                proposal_id,
                proposal_value
            })
        };

        match promised {
            None => accept(),
            Some(promised_id) => {
                if proposal_id >= promised_id {
                    accept()
                } else {
                    Err(Nack {
                        from_peer: self.peer_id,
                        proposal_id,
                        promised_proposal_id: promised_id
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn recover_state() {
        let istate = PersistentState{
            promised: Some(ProposalId{number: 1, peer: 1}),
            accepted: Some((ProposalId{number: 2, peer: 2}, false))
        };

        let a = Acceptor::new(0, Some(istate.clone()));

        assert_eq!(a.state, istate);
    }

    #[test]
    fn promise_first() {
        let mut a = Acceptor::new(0, None);

        let pid = ProposalId{number:1, peer:1};

        assert_eq!(
            a.receive_prepare(pid),
            Ok(Promise {
                from_peer: 0,
                proposal_id: pid,
                last_accepted: None
            })
        );

        assert_eq!(a.state, PersistentState{
            promised: Some(pid),
            accepted: None
        });
    }

    #[test]
    fn promise_higher() {
        let mut a = Acceptor::new(0, None);

        let pid1 = ProposalId{number:1, peer:1};
        let pid2 = ProposalId{number:2, peer:2};

        assert_eq!(
            a.receive_prepare(pid1),
            Ok(Promise {
                from_peer: 0,
                proposal_id: pid1,
                last_accepted: None
            })
        );
        assert_eq!(
            a.receive_prepare(pid2),
            Ok(Promise {
                from_peer: 0,
                proposal_id: pid2,
                last_accepted: None
            })
        );

        assert_eq!(a.state, PersistentState{
            promised: Some(pid2),
            accepted: None
        });
    }

    #[test]
    fn promise_nack_lower() {
        let mut a = Acceptor::new(0, None);

        let pid1 = ProposalId{number:1, peer:1};
        let pid2 = ProposalId{number:2, peer:2};

        assert_eq!(
            a.receive_prepare(pid2),
            Ok(Promise {
                from_peer: 0,
                proposal_id: pid2,
                last_accepted: None
            })
        );
        assert_eq!(
            a.receive_prepare(pid1),
            Err(Nack {
                from_peer: 0,
                proposal_id: pid1,
                promised_proposal_id: pid2
            })
        );

        assert_eq!(a.state, PersistentState{
            promised: Some(pid2),
            accepted: None
        });
    }

    #[test]
    fn accept_first() {
        let mut a = Acceptor::new(0, None);

        let pid = ProposalId{number:1, peer:1};

        assert_eq!(
            a.receive_accept(pid, true),
            Ok(Accepted {
                from_peer: 0,
                proposal_id: pid,
                proposal_value: true
            })
        );
        assert_eq!(a.state, PersistentState{
            promised: Some(pid),
            accepted: Some((pid, true))
        });
    }

    #[test]
    fn accept_higher() {
        let mut a = Acceptor::new(0, None);

        let pid1 = ProposalId{number:1, peer:1};
        let pid2 = ProposalId{number:2, peer:2};

        assert_eq!(
            a.receive_accept(pid1, true),
            Ok(Accepted {
                from_peer: 0,
                proposal_id: pid1,
                proposal_value: true
            })
        );
        assert_eq!(
            a.receive_accept(pid2, false),
            Ok(Accepted {
                from_peer: 0,
                proposal_id: pid2,
                proposal_value: false
            })
        );
        assert_eq!(a.state, PersistentState{
            promised: Some(pid2),
            accepted: Some((pid2, false))
        });
    }

    #[test]
    fn accept_nack_lower() {
        let mut a = Acceptor::new(0, None);

        let pid1 = ProposalId{number:1, peer:1};
        let pid2 = ProposalId{number:2, peer:2};

        assert_eq!(
            a.receive_accept(pid2, false),
            Ok(Accepted {
                from_peer: 0,
                proposal_id: pid2,
                proposal_value: false
            })
        );
        assert_eq!(
            a.receive_accept(pid1, true),
            Err(Nack {
                from_peer: 0,
                proposal_id: pid1,
                promised_proposal_id: pid2
            })
        );
        
        assert_eq!(a.state, PersistentState{
            promised: Some(pid2),
            accepted: Some((pid2, false))
        });
    }
}
