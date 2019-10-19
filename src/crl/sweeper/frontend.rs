
use super::*;
use crossbeam::crossbeam_channel;


pub(super) struct Frontend {
    client_id: ClientId,
    sender: crossbeam_channel::Sender<Request>,
}

impl Frontend {

    pub(super) fn new(client_id: ClientId, sender: crossbeam_channel::Sender<Request>) -> Frontend {
        Frontend { 
            client_id: client_id,
            sender: sender,
        }
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
        &self,
        store_id: store::Id,
        transaction_id: transaction::Id,
        serialized_transaction_description: ArcDataSlice,
        object_updates: Option<Vec<transaction::ObjectUpdate>>,
        tx_disposition: transaction::Disposition,
        paxos_state: paxos::PersistentState,
        save_id: TxSaveId
    ){        
        self.sender.send(Request::SaveTransactionState{
            client_request: ClientRequest {
                client_id: self.client_id, 
                store_id, 
                transaction_id, 
                save_id
            },
            store_id,
            transaction_id,
            serialized_transaction_description,
            object_updates: object_updates.unwrap_or(Vec::new()),
            tx_disposition: tx_disposition,
            paxos_state: paxos_state
        }).unwrap_or(()); // Explicitly ignore any errors
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
        &self,
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
    ) {
        self.sender.send(Request::SaveAllocationState{
            client_request: ClientRequest {
                client_id: self.client_id, 
                store_id, 
                transaction_id: allocation_transaction_id, 
                save_id: TxSaveId(0) // TODO: redesign so we dont do this?
            },
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