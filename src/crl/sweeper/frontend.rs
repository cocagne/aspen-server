use super::*;
use crate::crl::{Crl, InterfaceFactory, RequestCompletionHandler};
use crossbeam::crossbeam_channel;

pub struct FrontendFactory {
    sender: crossbeam_channel::Sender<Request>
}

impl InterfaceFactory for FrontendFactory {
    
    fn new(&self, save_handler: Box<dyn RequestCompletionHandler>) -> Box<dyn Crl> {
        let (response_sender, receiver) = crossbeam_channel::unbounded();

        self.sender.send(Request::RegisterClientRequest{sender: response_sender, handler: save_handler}).unwrap();

        let client_id = receiver.recv().unwrap().client_id;

        Box::new(Frontend::new(client_id, self.sender.clone()))
    }
}

pub(super) struct Frontend {
    client_id: ClientId,
    sender: crossbeam_channel::Sender<Request>,
    next_request_number: u64
}

impl Frontend {

    pub(super) fn new(client_id: ClientId, sender: crossbeam_channel::Sender<Request>) -> Frontend {
        Frontend { 
            client_id: client_id,
            sender: sender,
            next_request_number: 0
        }
    }

    fn next_request(&mut self) -> RequestId {
        let request_id = RequestId(self.next_request_number);
        self.next_request_number += 1;
        request_id
    }
}

impl crate::crl::Crl for Frontend {

    fn get_full_recovery_state(&self, store_id: store::Id) -> 
        (Vec<TransactionRecoveryState>, Vec<AllocationRecoveryState>) {
        let (response_sender, receiver) = crossbeam_channel::unbounded();
        self.sender.send(Request::GetFullRecoveryState{store_id: store_id, sender: response_sender}).unwrap();
        let t = receiver.recv().unwrap();
        (t.0, t.1)
    }

    fn save_transaction_state(
        &mut self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: ArcData,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState
    ) -> RequestId {
        let request_id = self.next_request();
        
        self.sender.send(Request::SaveTransactionState{
            client_request: ClientRequest(self.client_id, request_id),
            store_id,
            transaction_id,
            serialized_transaction_description,
            object_updates: object_updates.unwrap_or(Vec::new()),
            tx_disposition: tx_disposition,
            paxos_state: paxos_state
        }).unwrap_or(()); // Explicitly ignore any errors
    
        request_id
    }

    fn drop_transaction_object_data(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id
    ) {
        self.sender.send(Request::DropTransactionData{
            store_id: store_id,
            transaction_id: transaction_id,
        }).unwrap_or(()); // Explicitly ignore any errors
    }

    fn delete_transaction_state(
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id
    ) {
        self.sender.send(Request::DeleteTransactionState{
            store_id: store_id,
            transaction_id: transaction_id,
        }).unwrap_or(()); // Explicitly ignore any errors
    }

    fn save_allocation_state(
        &mut self,
        store_id: store::Id,
        store_pointer: store::Pointer,
        id: object::Id,
        kind: object::Kind,
        size: Option<u32>,
        data: ArcDataSlice,
        refcount: object::Refcount,
        timestamp: hlc::Timestamp,
        allocation_transaction_id: transaction::Id,
        serialized_revision_guard: ArcDataSlice
    ) -> RequestId {
        let request_id = self.next_request();
        self.sender.send(Request::SaveAllocationState{
            client_request: ClientRequest(self.client_id, request_id),
            state: AllocationRecoveryState {
                store_id: store_id,
                store_pointer: store_pointer,
                id: id,
                kind: kind,
                size: size,
                data: data,
                refcount: refcount,
                timestamp: timestamp,
                allocation_transaction_id: allocation_transaction_id,
                serialized_revision_guard: serialized_revision_guard
            }
        }).unwrap_or(()); // Explicitly ignore any errors
        request_id
    }

    fn delete_allocation_state(
        &self,
        store_id: store::Id, 
        allocation_transaction_id: transaction::Id) {
        self.sender.send(Request::DeleteAllocationState{
            store_id: store_id,
            allocation_transaction_id: allocation_transaction_id
        }).unwrap_or(()); // Explicitly ignore any errors
    }
}