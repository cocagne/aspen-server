
use std::sync::Arc;
use std::ops;

use integer_encoding::*;

#[derive(Debug)]
pub struct DataMut {
    v: Vec<u8>,
    offset: usize
}

#[derive(Eq, PartialEq, Debug)]
pub struct RawData<'a> {
    pub buffer: &'a [u8],
    offset: usize
}

impl<'a> RawData<'a> {
    pub fn new(buffer: &[u8]) -> RawData {
        RawData {
            buffer,
            offset: 0
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Data {
    pub buffer: Vec<u8>,
    offset: usize
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ArcData {
    pub buffer: Arc<Vec<u8>>,
}

pub struct ArcDataReader {
    pub buffer: ArcData,
    offset: usize
}

#[derive(Clone, Debug)]
pub struct ArcDataSlice {
    buffer: Arc<Vec<u8>>,
    begin: usize,
    end: usize
}

impl PartialEq for ArcDataSlice {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for ArcDataSlice {}

pub struct SliceReader<'a> {
    slice: &'a ArcDataSlice,
    offset: usize
}

pub trait DataReader {
    fn raw(&self) -> &[u8];

    fn offset(&self) -> usize;

    fn set_offset(&mut self, new_offset: usize);

    fn len(&self) -> usize {
        self.raw().len()
    }

    fn remaining(&self) -> usize {
        self.len() - self.offset()
    }

    fn remaining_bytes(&self) -> &[u8] {
        &self.raw()[self.offset() ..]
    }

    fn incr_offset(&mut self, inc: usize) {
        self.set_offset(self.offset() + inc);
    }

    fn get_slice(&mut self, nbytes: usize) -> &[u8] {
        let o = self.offset();
        self.incr_offset(nbytes);
        let s = &self.raw()[o .. o+nbytes];
        s
    }

    fn slice_range(&mut self, range: ops::Range<usize>) -> &[u8] {
        let o = self.offset();
        self.incr_offset(range.end - range.start);
        let s = &self.raw()[o+range.start .. o+range.end];
        s
    }

    fn copy_to_slice(&mut self, s: &mut [u8]) {
        s.copy_from_slice(self.get_slice(s.len()));
    }

    fn get_u8(&mut self) -> u8 {
        let a = self.raw()[self.offset()];
        self.incr_offset(1);
        a
    }
    fn get_u16_le(&mut self) -> u16 {
        let v = self.raw();
        let r = get_u16_le(v, self.offset());
        self.incr_offset(2);
        r
    }
    fn get_u32_le(&mut self) -> u32 {
        let v = self.raw();
        let r = get_u32_le(v, self.offset());
        self.incr_offset(4);
        r
    }
    fn get_u64_le(&mut self) -> u64 {
        let v = self.raw();
        let r = get_u64_le(v, self.offset());
        self.incr_offset(8);
        r
    }

    fn get_i8(&mut self) -> i8 {
        self.get_u8() as i8
    }
    fn get_i16_le(&mut self) -> i16 {
        self.get_u16_le() as i16
    }
    fn get_i32_le(&mut self) -> i32 {
        self.get_u32_le() as i32
    }
    fn get_i64_le(&mut self) -> i64 {
        self.get_u64_le() as i64
    }

    fn get_u16_be(&mut self) -> u16 {
        let v = self.raw();
        let r = get_u16_be(v, self.offset());
        self.incr_offset(2);
        r
    }
    fn get_u32_be(&mut self) -> u32 {
        let v = self.raw();
        let r = get_u32_be(v, self.offset());
        self.incr_offset(4);
        r
    }
    fn get_u64_be(&mut self) -> u64 {
        let v = self.raw();
        let r = get_u64_be(v, self.offset());
        self.incr_offset(8);
        r
    }

    fn get_i16_be(&mut self) -> i16 {
        self.get_u16_be() as i16
    }
    fn get_i32_be(&mut self) -> i32 {
        self.get_u32_be() as i32
    }
    fn get_i64_be(&mut self) -> i64 {
        self.get_u64_be() as i64
    }

    fn get_uuid(&mut self) -> uuid::Uuid {
        let mut ubytes: [u8; 16] = [0; 16];
        self.copy_to_slice(&mut ubytes);
        uuid::Uuid::from_bytes(ubytes)
    }

    fn get_varint(&mut self) -> usize {
        let (value, varint_sz) = usize::decode_var(self.remaining_bytes());
        self.incr_offset(varint_sz);
        value
    }

    fn get_varint_prefixed_slice(&mut self,) -> &[u8] {
        let nbytes = self.get_varint();
        let o = self.offset();
        self.incr_offset(nbytes);
        let s = &self.raw()[o .. o+nbytes];
        s
    }
}

impl Data {
    pub fn new(buffer: Vec<u8>) -> Data {
        Data { buffer, offset: 0}
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl ArcData {
    pub fn new(buffer: Vec<u8>) -> ArcData {
        ArcData {
            buffer: Arc::new(buffer),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[..]
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl ArcDataSlice {
    pub fn from_vec(v: Vec<u8>) -> ArcDataSlice {
        let end = v.len();
        ArcDataSlice {
            buffer: Arc::new(v),
            begin: 0,
            end: end
        }
    }

    pub fn from_bytes(buff: &[u8]) -> ArcDataSlice {
        let mut v = Vec::<u8>::with_capacity(buff.len());
        
        buff.iter().for_each(|b| v.push(*b));
        
        ArcDataSlice {
            buffer: Arc::new(v),
            begin: 0,
            end: buff.len()
        }
    }

    pub fn new(adata: &ArcData, offset: usize, end: usize) -> ArcDataSlice {
        ArcDataSlice {
            buffer: adata.buffer.clone(),
            begin: offset,
            end
        }
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer[self.begin .. self.end]
    }

    pub fn len(&self) -> usize {
        self.end - self.begin
    }
}

impl<'a> SliceReader<'a> {
    pub fn new(slice: &ArcDataSlice) -> SliceReader {
        SliceReader {
            slice,
            offset: 0
        }
    }
}

impl DataMut {
    pub fn with_capacity(capacity: usize) -> DataMut {
        DataMut {
            v : Vec::with_capacity(capacity),
            offset : 0
        }
    }
    pub fn finalize(self) -> Data {
        Data {
            buffer: self.v,
            offset: 0
        }
    }
    pub fn capacity(&self) -> usize {
        self.v.capacity()
    }
    
    pub fn set_offset(&mut self, new_offset: usize) {
        assert!(new_offset <= self.v.len(), "Attempted to set offset the end of valid data");
        self.offset = new_offset;
    }

    pub fn zfill(&mut self, nbytes: u64) {
        let mut remaining = nbytes;

        while remaining > 8 {
            self.put_u64_le(0u64);
            remaining -= 8;
        }

        while remaining > 0 {
            self.put_u8(0u8);
            remaining -= 1;
        }
    }

    pub fn put_slice(&mut self, s: &[u8]) {
        assert!(self.offset + s.len() <= self.capacity(), "Buffer overflow");

        if self.offset == self.len() {
            self.v.extend_from_slice(s);
        } 
        else if self.offset + s.len() <= self.len() {
            self.v[self.offset..self.offset+s.len()].copy_from_slice(s);
        } else {
            let n = self.len() - self.offset();
            let vlen = self.v.len();
            self.v[self.offset..vlen].copy_from_slice(&s[..n]);
            self.v.extend_from_slice(&s[n..]);
        }
        
        self.offset += s.len();
    }
    pub fn put_u8(&mut self, x: u8) {
        if self.offset == self.v.len() {
            self.v.extend_from_slice(&[x]);
        } else {
            self.v[self.offset] = x;
        }
        self.offset += 1;
    }

    pub fn put_i16_le(&mut self, x: i16) { 
        self.put_u16_le(x as u16); 
    }
    pub fn put_u16_le(&mut self, x: u16) {
        const SIZE: usize = 2;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_le_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_le_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    pub fn put_i32_le(&mut self, x: i32) { 
        self.put_u32_le(x as u32); 
    }
    pub fn put_u32_le(&mut self, x: u32) {
        const SIZE: usize = 4;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_le_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_le_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    pub fn put_i64_le(&mut self, x: i64) { 
        self.put_u64_le(x as u64); 
    }
    pub fn put_u64_le(&mut self, x: u64) {
        const SIZE: usize = 8;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_le_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_le_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    // ---- Big Endian ----

    pub fn put_i16_be(&mut self, x: i16) { 
        self.put_u16_be(x as u16); 
    }
    pub fn put_u16_be(&mut self, x: u16) {
        const SIZE: usize = 2;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_be_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_be_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    pub fn put_i32_be(&mut self, x: i32) { 
        self.put_u32_be(x as u32); 
    }
    pub fn put_u32_be(&mut self, x: u32) {
        const SIZE: usize = 4;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_be_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_be_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    pub fn put_i64_be(&mut self, x: i64) { 
        self.put_u64_be(x as u64); 
    }
    pub fn put_u64_be(&mut self, x: u64) {
        const SIZE: usize = 8;

        if self.offset == self.v.len() {
            self.put_slice(&x.to_be_bytes());
        } else {
            while self.offset + SIZE > self.v.len() {
                self.v.push(0u8);
            }
            let n = x.to_be_bytes();
            for i in 0..SIZE {
                self.v[self.offset + i] = n[i];
            }
            self.offset += SIZE;
        }
    }

    pub fn put_uuid(&mut self, uuid: uuid::Uuid) {
        self.put_slice(uuid.as_bytes());
    }
}

impl DataReader for DataMut {
    fn raw(&self) -> &[u8] {
        &self.v
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn set_offset(&mut self, new_offset: usize) {
        self.offset = new_offset;
    }
}

impl<'a> DataReader for RawData<'a> {
    fn raw(&self) -> &[u8] {
        self.buffer
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn set_offset(&mut self, new_offset: usize) {
        self.offset = new_offset;
    }
}

impl DataReader for Data {
    fn raw(&self) -> &[u8] {
        &self.buffer
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn set_offset(&mut self, new_offset: usize) {
        self.offset = new_offset;
    }
}

impl DataReader for ArcDataReader {
    fn raw(&self) -> &[u8] {
        &self.buffer.buffer
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn set_offset(&mut self, new_offset: usize) {
        self.offset = new_offset;
    }
}

impl<'a> DataReader for SliceReader<'a> {
    fn raw(&self) -> &[u8] {
        &self.slice.as_bytes()
    }

    fn offset(&self) -> usize {
        self.offset
    }

    fn set_offset(&mut self, new_offset: usize) {
        self.offset = new_offset;
    }
}


impl From<DataMut> for Data {
    fn from(data_mut: DataMut) -> Data {
        Data {
            buffer: data_mut.v,
            offset: data_mut.offset
        }
    }
}

impl From<Data> for DataMut {
    fn from(data: Data) -> DataMut {
        DataMut {
            v: data.buffer,
            offset: data.offset
        }
    }
}

impl From<DataMut> for ArcData {
    fn from(data_mut: DataMut) -> ArcData {
        ArcData::new(data_mut.v)
    }
}

impl From<Data> for ArcData {
    fn from(data: Data) -> ArcData {
        ArcData {
            buffer: Arc::new(data.buffer),
        }
    }
}

impl From<Vec<u8>> for ArcData {
    fn from(data: Vec<u8>) -> ArcData {
        ArcData {
            buffer: Arc::new(data),
        }
    }
}

impl From<Vec<u8>> for ArcDataSlice {
    fn from(v: Vec<u8>) -> ArcDataSlice {
        let len = v.len();
        
        ArcDataSlice {
            buffer: Arc::new(v),
            begin: 0,
            end: len
        }
    }
}

impl From<Data> for ArcDataSlice {
    fn from (data: Data) -> ArcDataSlice {
        let len = data.buffer.len();
        ArcDataSlice {
            buffer: Arc::new(data.buffer),
            begin: 0,
            end: len
        }
    }
}

impl From<ArcData> for ArcDataSlice {
    fn from(adata: ArcData) -> ArcDataSlice {
        let len = adata.buffer.len();
        ArcDataSlice {
            buffer: adata.buffer,
            begin: 0,
            end: len
        }
    }
}

impl From<&ArcData> for ArcDataSlice {
    fn from(adata: &ArcData) -> ArcDataSlice {
        ArcDataSlice {
            buffer: adata.buffer.clone(),
            begin: 0,
            end: adata.buffer.len()
        }
    }
}

pub fn get_u8(v: &[u8], offset: usize) -> u8 {
    v[offset]
}
pub fn get_u16_le(v: &[u8], offset: usize) -> u16 {
    let o = offset;
    let a = [v[o], v[o+1]];
    u16::from_le_bytes(a)
}
pub fn get_u32_le(v: &[u8], offset: usize) -> u32 {
    let o = offset;
    let a = [v[o], v[o+1], v[o+2], v[o+3]];
    u32::from_le_bytes(a)
}
pub fn get_u64_le(v: &[u8], offset: usize) -> u64 {
    let o = offset;
    let a = [v[o], v[o+1], v[o+2], v[o+3], v[o+4], v[o+5], v[o+6], v[o+7]];
    u64::from_le_bytes(a)
}

pub fn get_i8(v: &[u8], offset: usize) -> i8 {
    get_u8(v, offset) as i8
}
pub fn get_i16_le(v: &[u8], offset: usize) -> i16 {
    get_u16_le(v, offset) as i16
}
pub fn get_i32_le(v: &[u8], offset: usize) -> i32 {
    get_u32_le(v, offset) as i32
}
pub fn get_i64_le(v: &[u8], offset: usize) -> i64 {
    get_u64_le(v, offset) as i64
}

pub fn get_u16_be(v: &[u8], offset: usize) -> u16 {
    let o = offset;
    let a = [v[o], v[o+1]];
    u16::from_be_bytes(a)
}
pub fn get_u32_be(v: &[u8], offset: usize) -> u32 {
    let o = offset;
    let a = [v[o], v[o+1], v[o+2], v[o+3]];
    u32::from_be_bytes(a)
}
pub fn get_u64_be(v: &[u8], offset: usize) -> u64 {
    let o = offset;
    let a = [v[o], v[o+1], v[o+2], v[o+3], v[o+4], v[o+5], v[o+6], v[o+7]];
    u64::from_be_bytes(a)
}

pub fn get_i16_be(v: &[u8], offset: usize) -> i16 {
    get_u16_be(v, offset) as i16
}
pub fn get_i32_be(v: &[u8], offset: usize) -> i32 {
    get_u32_be(v, offset) as i32
}
pub fn get_i64_be(v: &[u8], offset: usize) -> i64 {
    get_u64_be(v, offset) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices() {
        let mut d = DataMut::with_capacity(6);
        d.put_slice(&0u32.to_le_bytes());
        assert_eq!(d.offset, 4);
        assert_eq!(d.v, [0, 0, 0, 0]);
        d.set_offset(0);
        assert_eq!(d.get_u32_le(), 0);
        d.set_offset(1);
        d.put_slice(&0x0A0Bu16.to_le_bytes());
        assert_eq!(d.v, [0, 0xB, 0xA, 0]);
        d.set_offset(2);
        d.put_slice(&0x0D0C0B0Au32.to_le_bytes());
        assert_eq!(d.v, [0, 0xB, 0xA, 0xB, 0xC, 0xD]);
        d.set_offset(2);
        let x = d.get_u32_le();
        assert_eq!(x, 0x0D0C0B0A);
    }

    #[test]
    fn put_u8() {
        let mut d = DataMut::with_capacity(4);
        d.put_slice(&0u16.to_le_bytes());
        assert_eq!(d.offset, 2);
        assert_eq!(d.v, [0, 0]);
        d.set_offset(1);
        d.put_u8(5u8);
        assert_eq!(d.v, [0, 5]);
        assert_eq!(d.offset, 2);
        d.put_u8(6u8);
        assert_eq!(d.v, [0, 5, 6]);
        assert_eq!(d.offset, 3);
    }

    #[test]
    fn put_u16_le() {
        let mut d = DataMut::with_capacity(6);
        assert_eq!(d.offset, 0);
        d.put_u16_le(0x0102u16); // exactly end
        assert_eq!(d.offset, 2);
        assert_eq!(d.v, [0x2, 0x1]);
        d.set_offset(1); // straddle end
        d.put_u16_le(0x0304u16);
        assert_eq!(d.v, [0x2, 0x4, 0x3]);
        assert_eq!(d.offset, 3);
        d.set_offset(0);
        d.put_u16_le(0x0201u16);
        d.put_u16_le(0x0403u16);
        assert_eq!(d.v, [0x1, 0x2, 0x3, 0x4]);
        assert_eq!(d.offset, 4);
        d.set_offset(1); // wholly contained
        d.put_u16_le(0x0908u16);
        assert_eq!(d.v, [0x1, 0x8, 0x9, 0x4]);
        assert_eq!(d.offset, 3);
    }

    #[test]
    fn put_u32_le() {
        let mut d = DataMut::with_capacity(4*3);
        assert_eq!(d.offset, 0);
        d.put_u32_le(0x01020304u32); // exactly end
        assert_eq!(d.offset, 4);
        assert_eq!(d.v, [0x4, 0x3, 0x2, 0x1]);
        d.set_offset(2); // straddle end
        d.put_u32_le(0x05060708u32);
        assert_eq!(d.v, [4, 3, 8, 7, 6, 5]);
        assert_eq!(d.offset, 6);
        d.set_offset(0);
        d.put_u32_le(0x01020304u32);
        d.put_u32_le(0x05060708u32);
        assert_eq!(d.v, [4, 3, 2, 1, 8, 7, 6, 5]);
        assert_eq!(d.offset, 8);
        d.set_offset(2); // wholly contained
        d.put_u32_le(0x0D0C0B0Au32);
        assert_eq!(d.v, [4, 3, 0xA, 0xB, 0xC, 0xD, 6, 5]);
        assert_eq!(d.offset, 6);
        d.set_offset(2);
        assert_eq!(d.get_u32_le(), 0x0D0C0B0Au32);
        assert_eq!(d.offset, 6);
    }

    #[test]
    fn put_i32_le() {
        let mut d = DataMut::with_capacity(4*3);
        assert_eq!(d.offset, 0);
        d.put_i32_le(-40000i32);
        assert_eq!(d.offset, 4);
        d.set_offset(0);
        assert_eq!(d.get_i32_le(), -40000i32);
        assert_eq!(d.offset, 4);
    }

    #[test]
    fn put_i64_le() {
        let mut d = DataMut::with_capacity(8*3);
        assert_eq!(d.offset, 0);
        d.put_i64_le(-40000i64);
        assert_eq!(d.offset, 8);
        d.set_offset(0);
        assert_eq!(d.get_i64_le(), -40000i64);
        assert_eq!(d.offset, 8);
    }

}