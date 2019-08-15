extern crate time;

pub mod system_clock {
    use super::Clock;

    pub fn system_time_ms() -> u64 {
        let t = time::get_time();
        t.sec as u64 * 1000 + t.nsec as u64 / 1000000
    }

    pub fn new() -> Clock<fn() -> u64> {
        Clock::new(system_time_ms)
    }
}

pub struct Clock<T>
    where T: FnMut() -> u64
{
    get_wall_time_ms: T,
    last: Timestamp
}

impl<T> Clock<T> 
    where T: FnMut() -> u64
{
    pub fn new(f: T) -> Clock<T> {
        Clock { 
            get_wall_time_ms: f,
            last : Timestamp { wall_time_ms: 0, logical: 0 }
        }
    }

    pub fn now(&mut self) -> Timestamp {
        let now_ms = (self.get_wall_time_ms)();
        if now_ms > self.last.wall_time_ms {
            self.last.wall_time_ms = now_ms;
            self.last.logical = 0;
        } else {
            match self.last.logical.checked_add(1) {
                Some(l) => self.last.logical = l,
                None => {
                    self.last.wall_time_ms += 1;
                    self.last.logical = 0;
                }
            }
        }

        Timestamp { 
            wall_time_ms: self.last.wall_time_ms, 
            logical: self.last.logical 
        }
    }

    pub fn update(&mut self, ts: &Timestamp) {
        // TODO: Detect runaway clocks and handle them gracefully
        if ts.wall_time_ms > self.last.wall_time_ms {
            self.last.wall_time_ms = ts.wall_time_ms;
            self.last.logical = ts.logical;
        }
        else if ts.wall_time_ms == self.last.wall_time_ms {
            if ts.logical > self.last.logical {
                self.last.logical = ts.logical
            }
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Timestamp {
    wall_time_ms: u64,
    logical: u16
}

impl Timestamp {
    pub fn from(ts: u64) -> Timestamp {
        Timestamp {
            wall_time_ms: ts & !0xFFFFu64,
            logical: (ts & 0xFFFFu64) as u16
        }
    }

    pub fn to_u64(&self) -> u64 {
        (self.wall_time_ms << 16) | self.logical as u64
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use super::*;

    #[test]
    fn compare() {
        assert!(Timestamp::from(0x2u64) > Timestamp::from(0x1u64));
        assert!(Timestamp::from(0x1u64) == Timestamp::from(0x1u64));
        assert!(Timestamp::from(0x1u64) != Timestamp::from(0x2u64));
        assert!(Timestamp::from(0x10001u64) == Timestamp::from(0x10001u64));
        assert!(Timestamp::from(0x20001u64) > Timestamp::from(0x10001u64));
        assert!(Timestamp::from(0x20001u64) > Timestamp::from(0x10002u64));
    }

    #[test]
    fn update() {
        let v = Cell::new(0u64);
        let mut c = Clock::new(|| v.get());

        assert_eq!(c.now(), Timestamp { wall_time_ms: 0, logical: 1});
        assert_eq!(c.now(), Timestamp { wall_time_ms: 0, logical: 2});

        v.set(1u64);

        assert_eq!(c.now(), Timestamp { wall_time_ms: 1, logical: 0});
        assert_eq!(c.now(), Timestamp { wall_time_ms: 1, logical: 1});

        c.update( &Timestamp { wall_time_ms: 1, logical: 3} );

        assert_eq!(c.now(), Timestamp { wall_time_ms: 1, logical: 4});

        c.update( &Timestamp { wall_time_ms: 3, logical: 3} );

        assert_eq!(c.now(), Timestamp { wall_time_ms: 3, logical: 4});

        v.set(5u64);

        assert_eq!(c.now(), Timestamp { wall_time_ms: 5, logical: 0});
    }

    #[test]
    fn overflow() {
        let v = Cell::new(0u64);
        let mut c = Clock::new(|| v.get());

        assert_eq!(c.now(), Timestamp { wall_time_ms: 0, logical: 1});

        c.update( &Timestamp { wall_time_ms: 0, logical: u16::max_value() } );

        assert_eq!(c.now(), Timestamp { wall_time_ms: 1, logical: 0});
    }
}