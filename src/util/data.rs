
pub struct DataMut {
    v: Vec<u8>,
    offset: usize
}

impl DataMut {
    pub fn new(capacity: usize) -> DataMut {
        DataMut {
            v : Vec::with_capacity(capacity),
            offset : 0
        }
    }
    pub fn capacity(&self) -> usize {
        self.v.capacity()
    }
    pub fn len(&self) -> usize {
        self.v.len()
    }
    pub fn offset(&self) -> usize {
        self.offset
    }
    pub fn set_offset(&mut self, new_offset: usize) {
        assert!(new_offset <= self.v.len(), "Attempted to set offset the end of valid data");
        self.offset = new_offset;
    }
    pub fn put(&mut self, s: &[u8]) {
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
            self.put(&x.to_le_bytes());
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
            self.put(&x.to_le_bytes());
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
            self.put(&x.to_le_bytes());
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
            self.put(&x.to_be_bytes());
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
            self.put(&x.to_be_bytes());
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
            self.put(&x.to_be_bytes());
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

    pub fn get_u8(&mut self) -> u8 {
        let a = self.v[self.offset];
        self.offset += 1;
        a
    }
    pub fn get_u16_le(&mut self) -> u16 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1]];
        self.offset += 2;
        u16::from_le_bytes(a)
    }
    pub fn get_u32_le(&mut self) -> u32 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1], v[o+2], v[o+3]];
        self.offset += 4;
        u32::from_le_bytes(a)
    }
    pub fn get_u64_le(&mut self) -> u64 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1], v[o+2], v[o+3], v[o+4], v[o+5], v[o+6], v[o+7]];
        self.offset += 8;
        u64::from_le_bytes(a)
    }

    pub fn get_i8(&mut self) -> i8 {
        self.get_u8() as i8
    }
    pub fn get_i16_le(&mut self) -> i16 {
        self.get_u16_le() as i16
    }
    pub fn get_i32_le(&mut self) -> i32 {
        self.get_u32_le() as i32
    }
    pub fn get_i64_le(&mut self) -> i64 {
        self.get_u64_le() as i64
    }

    pub fn get_u16_be(&mut self) -> u16 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1]];
        self.offset += 2;
        u16::from_be_bytes(a)
    }
    pub fn get_u32_be(&mut self) -> u32 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1], v[o+2], v[o+3]];
        self.offset += 4;
        u32::from_be_bytes(a)
    }
    pub fn get_u64_be(&mut self) -> u64 {
        let v = &self.v;
        let o = self.offset;
        let a = [v[o], v[o+1], v[o+2], v[o+3], v[o+4], v[o+5], v[o+6], v[o+7]];
        self.offset += 8;
        u64::from_be_bytes(a)
    }

    pub fn get_i16_be(&mut self) -> i16 {
        self.get_u16_be() as i16
    }
    pub fn get_i32_be(&mut self) -> i32 {
        self.get_u32_be() as i32
    }
    pub fn get_i64_be(&mut self) -> i64 {
        self.get_u64_be() as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices() {
        let mut d = DataMut::new(6);
        d.put(&0u32.to_le_bytes());
        assert_eq!(d.offset, 4);
        assert_eq!(d.v, [0, 0, 0, 0]);
        d.set_offset(0);
        assert_eq!(d.get_u32_le(), 0);
        d.set_offset(1);
        d.put(&0x0A0Bu16.to_le_bytes());
        assert_eq!(d.v, [0, 0xB, 0xA, 0]);
        d.set_offset(2);
        d.put(&0x0D0C0B0Au32.to_le_bytes());
        assert_eq!(d.v, [0, 0xB, 0xA, 0xB, 0xC, 0xD]);
        d.set_offset(2);
        let x = d.get_u32_le();
        assert_eq!(x, 0x0D0C0B0A);
    }

    #[test]
    fn put_u8() {
        let mut d = DataMut::new(4);
        d.put(&0u16.to_le_bytes());
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
        let mut d = DataMut::new(6);
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
        let mut d = DataMut::new(4*3);
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
        let mut d = DataMut::new(4*3);
        assert_eq!(d.offset, 0);
        d.put_i32_le(-40000i32);
        assert_eq!(d.offset, 4);
        d.set_offset(0);
        assert_eq!(d.get_i32_le(), -40000i32);
        assert_eq!(d.offset, 4);
    }

    #[test]
    fn put_i64_le() {
        let mut d = DataMut::new(8*3);
        assert_eq!(d.offset, 0);
        d.put_i64_le(-40000i64);
        assert_eq!(d.offset, 8);
        d.set_offset(0);
        assert_eq!(d.get_i64_le(), -40000i64);
        assert_eq!(d.offset, 8);
    }

}