

#[derive(PartialEq, Eq)]
pub struct Revision { pub uuid: uuid::Uuid }

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct Refcount {
    pub update_serial: u32, 
    pub count: u32
}

