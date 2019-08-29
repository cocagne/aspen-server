//! Implementation of Hybrid Logical Clocks
//! 
//! HLC timestamps blend the best attributes of traditional wall-clock timestamps and 
//! vector clocks. They are close to traditional wall-clock timestamps but the sub-millisecond
//! bits of the timestamp are dropped in favor of a logical clock that provides happened-before
//! guarantees for related timestamps. Full details may be found in the following paper
//! https://cse.buffalo.edu/tech-reports/2014-04.pdf
//! 
use std::fmt;
use std::convert::From;

extern crate time;

/// Provides a HLC Clock that uses the normal system clock.
/// 
/// Implemented in terms of time::get_time()
pub mod system_clock {
    use super::Clock;

    /// Returns system time in milleseconds since Jan 1 1970
    pub fn system_time_ms() -> u64 {
        let t = time::get_time();
        t.sec as u64 * 1000 + t.nsec as u64 / 1000000
    }

    /// Returns a new Clock instance that uses the normal system clock
    pub fn new() -> Clock<fn() -> u64> {
        Clock::new(system_time_ms)
    }
}

/// Represents a stateful HLC clock.
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
            last : Timestamp(0)
        }
    }

    /// Returns a Timestamp that is guaranteed to be larger than any previous timestamps passed
    /// to the update() method.
    pub fn now(&mut self) -> Timestamp {
        let now_ms = (self.get_wall_time_ms)();

        if now_ms > self.last.wall_time_ms() {
            self.last.set(now_ms, 0);
        } else {
            match self.last.logical().checked_add(1) {
                Some(l) => self.last.set_logical(l),
                None => {
                    self.last.set(self.last.wall_time_ms() + 1, 0);
                }
            }
        }

        Timestamp(self.last.0)
    }

    /// Updates the internal state of the clock and ensures that all timestamps returned by the
    /// now method will come after this time.
    pub fn update(&mut self, ts: &Timestamp) {
        // TODO: Detect runaway clocks and handle them gracefully
        if ts.wall_time_ms() > self.last.wall_time_ms() {
            self.last.0 = ts.0;
        }
        else if ts.wall_time_ms() == self.last.wall_time_ms() {
            if ts.logical() > self.last.logical() {
                self.last.set_logical(ts.logical());
            }
        }
    }
}

/// Represents an HLC Timestamp
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn from(ts: u64) -> Timestamp {
        Timestamp(ts)
    }

    pub fn to_u64(&self) -> u64 {
        self.0
    }

    pub fn wall_time_ms(&self) -> u64 {
        self.0 >> 16
    }

    pub fn logical(&self) -> u16 {
        (self.0 & 0xFFFFu64) as u16
    }

    fn set_logical(&mut self, l: u16) {
        self.0 = (self.0 & !0xFFFFu64) | l as u64
    }

    fn set(&mut self, wall_ms: u64, l: u16) {
        self.0 = wall_ms << 16 | l as u64
    }
}

impl From<u64> for Timestamp {
    fn from(ts: u64) -> Self {
        Timestamp::from(ts)
    }
}

impl From<Timestamp> for u64 {
    fn from(ts: Timestamp) -> Self {
        ts.to_u64()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({},{})", self.wall_time_ms(), self.logical())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use super::*;

    fn ts(wall: u64, logical: u16) -> Timestamp {
        Timestamp(wall << 16 | logical as u64)
    }

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

        assert_eq!(c.now(), ts(0, 1));
        assert_eq!(c.now(), ts(0, 2));

        v.set(1u64);

        assert_eq!(c.now(), ts(1, 0));
        assert_eq!(c.now(), ts(1, 1));

        c.update( &ts(1, 3));

        assert_eq!(c.now(), ts(1, 4));

        c.update( &ts(3, 3));

        assert_eq!(c.now(), ts(3, 4));

        v.set(5u64);

        assert_eq!(c.now(), ts(5, 0));
    }

    #[test]
    fn overflow() {
        let v = Cell::new(0u64);
        let mut c = Clock::new(|| v.get());

        assert_eq!(c.now(), ts(0, 1));

        c.update( &ts(0, u16::max_value()));

        assert_eq!(c.now(), ts(1, 0));
    }
}