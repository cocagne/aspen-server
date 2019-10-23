use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::store::{State, ObjectCache};
use crate::object;

struct Entry {
    next: usize,
    prev: usize,
    state: Rc<RefCell<State>>
}

pub struct SimpleCache {
    max_entries: usize,
    least_recently_used: usize,
    most_recently_used: usize,
    entries: Vec<Entry>,
    lookup: HashMap<object::Id, usize>
}

impl SimpleCache {
    pub fn new(max_entries: usize) -> SimpleCache {
        SimpleCache {
            max_entries,
            least_recently_used: 0,
            most_recently_used: 0,
            entries: Vec::new(),
            lookup: HashMap::new()
        }
    }
}

impl ObjectCache for SimpleCache {

    fn remove(&mut self, object_id: &object::Id) {
        // Just remove the entry from the lookup index. To keep ownership simple,
        // we'll leave it in the LRU chain and let it fall off the end naturally
        // since it can no longer be accessed.
        self.lookup.remove(object_id);
    }
    
    fn get(&mut self, object_id: &object::Id) -> Option<&Rc<RefCell<State>>> {
        if self.entries.is_empty() {
            None
        }
        else if let Some(idx) = self.lookup.get(object_id) {
            //let mut target = &self.entries[*idx];

            if *idx == self.least_recently_used {
                let new_lru = self.entries[self.least_recently_used].next;
                self.entries[new_lru].prev = new_lru; // point to self
                self.least_recently_used = new_lru;
            }

            // No adjustment required if we're accessing the already most-recently-used object
            // so only check for the negative here
            if *idx != self.most_recently_used {
                let mut mru = &mut self.entries[self.most_recently_used];
                mru.next = *idx;

                let prev = self.entries[*idx].prev;
                self.entries[prev].next = self.entries[*idx].next;

                let next = self.entries[*idx].next;
                self.entries[next].prev = self.entries[*idx].prev;

                self.entries[*idx].prev = self.most_recently_used;
                self.entries[*idx].next = *idx; // point to self
            }

            Some(&self.entries[*idx].state)
        }
        else {
            None
        }
    }

    fn insert(&mut self, state: Rc<RefCell<State>>) -> Option<Rc<RefCell<State>>> {
        
        if self.entries.is_empty() {    
            self.least_recently_used = 0;
            self.most_recently_used = 0;
            self.lookup.insert(state.borrow().id, 0);
            self.entries.push(Entry{
                prev: 0,
                next: 0,
                state
            });
            return None;
        }
        else if self.entries.len() < self.max_entries {
            let idx = self.entries.len();
            self.entries[self.most_recently_used].next = idx;
            self.lookup.insert(state.borrow().id, idx);
            self.entries.push(Entry{
                prev: self.most_recently_used,
                next: idx,
                state
            });
            self.most_recently_used = idx;

            None
        } 
        else {
            // Index is full, need to pop an entry. However, we cannot pop objects locked
            // to transactions. So we'll use get() on them to put those at the head of
            // the list until a non-locked object occurs
            while self.entries[self.least_recently_used].state.borrow().transaction_references != 0 {
                let object_id = self.entries[self.least_recently_used].state.borrow().id.clone();
                self.get(&object_id);
            }

            let lru_object_id = self.entries[self.least_recently_used].state.borrow().id.clone();
            let new_lru = self.entries[self.least_recently_used].next;
            let new_mru = self.entries[new_lru].prev;
            self.entries[new_lru].prev = new_lru;

            let new_object_id = state.borrow().id.clone();
            
            let mut e = Entry{
                prev: self.most_recently_used,
                next: self.least_recently_used,
                state
            };

            self.lookup.remove(&lru_object_id);
            self.lookup.insert(new_object_id, new_mru);

            std::mem::swap(&mut e, &mut self.entries[self.least_recently_used]);

            self.most_recently_used = self.least_recently_used;
            self.least_recently_used = new_lru;

            Some(e.state)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use super::*;
    use crate::object;
    use crate::store;
    use std::sync;
    use uuid;

    fn objs() -> (State, State, State, State) {
        let o1 = object::Id(uuid::Uuid::parse_str("1108d237-a26e-4735-b001-6782fb2eac38").unwrap());
        let o2 = object::Id(uuid::Uuid::parse_str("2208d237-a26e-4735-b001-6782fb2eac38").unwrap());
        let o3 = object::Id(uuid::Uuid::parse_str("3308d237-a26e-4735-b001-6782fb2eac38").unwrap());
        let o4 = object::Id(uuid::Uuid::parse_str("4408d237-a26e-4735-b001-6782fb2eac38").unwrap());

        let metadata = object::Metadata {
            revision: object::Revision(uuid::Uuid::parse_str("f308d237-a26e-4735-b001-6782fb2eac38").unwrap()),
            refcount: object::Refcount{update_serial: 1, count: 1},
            timestamp: crate::hlc::Timestamp::from(1)
        };
        
        (State {
            id: o1,
            store_pointer: store::Pointer::None{pool_index: 0},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        },
        State {
            id: o2,
            store_pointer: store::Pointer::None{pool_index: 0},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        },
        State {
            id: o3,
            store_pointer: store::Pointer::None{pool_index: 0},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        },
        State {
            id: o4,
            store_pointer: store::Pointer::None{pool_index: 0},
            metadata,
            object_kind: object::Kind::Data,
            transaction_references: 0,
            locked_to_transaction: None,
            data: sync::Arc::new(vec![]),
            max_size: None,
            kv_state: None
        },
        )
        
    }

    #[test]
    fn max_size() {
        let (o1, o2, o3, o4) = objs();

        let u1 = o1.id.clone();
        //let u2 = o2.id.clone();
        //let u3 = o3.id.clone();
        //let u4 = o3.id.clone();

        let o1 = Rc::new(RefCell::new(o1));
        let o2 = Rc::new(RefCell::new(o2));
        let o3 = Rc::new(RefCell::new(o3));
        let o4 = Rc::new(RefCell::new(o4));

        let mut c = SimpleCache::new(3);

        assert!(c.insert(o1).is_none());
        assert!(c.insert(o2).is_none());
        assert!(c.insert(o3).is_none());
        let x = c.insert(o4);
        assert!(x.is_some());
        let x = x.unwrap();

        assert_eq!(x.borrow().id, u1);
    }

    #[test]
    fn get_increases_priority() {
        let (o1, o2, o3, o4) = objs();

        let u1 = o1.id.clone();
        let u2 = o2.id.clone();
        let u3 = o3.id.clone();
        //let u4 = o3.id.clone();

        let o1 = Rc::new(RefCell::new(o1));
        let o2 = Rc::new(RefCell::new(o2));
        let o3 = Rc::new(RefCell::new(o3));
        let o4 = Rc::new(RefCell::new(o4));

        let mut c = SimpleCache::new(3);

        assert!(c.insert(o1).is_none());
        assert!(c.insert(o2).is_none());
        assert!(c.insert(o3).is_none());

        assert_eq!(c.get(&u1).unwrap().borrow().id, u1);
        assert_eq!(c.get(&u2).unwrap().borrow().id, u2);

        let x = c.insert(o4);
        assert!(x.is_some());
        let x = x.unwrap();

        assert_eq!(x.borrow().id, u3);
    }

    #[test]
    fn two_element_increases_priority() {
        let (o1, o2, o3, o4) = objs();

        let u1 = o1.id.clone();
        let u2 = o2.id.clone();
        let u3 = o3.id.clone();
        //let u4 = o3.id.clone();

        let o1 = Rc::new(RefCell::new(o1));
        let o2 = Rc::new(RefCell::new(o2));
        let o3 = Rc::new(RefCell::new(o3));
        let o4 = Rc::new(RefCell::new(o4));

        let mut c = SimpleCache::new(3);

        assert!(c.insert(o1).is_none());
        assert!(c.insert(o2).is_none());
        assert_eq!(c.get(&u1).unwrap().borrow().id, u1);
        assert!(c.insert(o3).is_none());
        assert_eq!(c.get(&u2).unwrap().borrow().id, u2);

        let x = c.insert(o4);
        assert!(x.is_some());
        let x = x.unwrap();

        assert_eq!(x.borrow().id, u3);
    }

    #[test]
    fn skip_locked_transactions() {
        let (mut o1, mut o2, o3, o4) = objs();

        //let u1 = o1.id.clone();
        //let u2 = o2.id.clone();
        let u3 = o3.id.clone();
        //let u4 = o3.id.clone();

        o1.transaction_references = 1;
        o2.transaction_references = 1;

        let o1 = Rc::new(RefCell::new(o1));
        let o2 = Rc::new(RefCell::new(o2));
        let o3 = Rc::new(RefCell::new(o3));
        let o4 = Rc::new(RefCell::new(o4));

        let mut c = SimpleCache::new(3);

        assert!(c.insert(o1).is_none());
        assert!(c.insert(o2).is_none());
        assert!(c.insert(o3).is_none());
        let x = c.insert(o4);
        assert!(x.is_some());
        let x = x.unwrap();

        assert_eq!(x.borrow().id, u3);
    }
}