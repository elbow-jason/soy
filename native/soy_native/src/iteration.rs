use crate::Bin;
use rocksdb::{Direction, IteratorMode};
use rustler::{NifRecord, NifUnitEnum, NifUntaggedEnum};

#[derive(NifUnitEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterFirst {
    First,
}

#[derive(NifUnitEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum IterLast {
    Last,
}

#[derive(NifRecord)]
#[tag = "first"]
pub struct IterFirstCf(String);

impl IterFirstCf {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(NifRecord)]
#[tag = "last"]
pub struct IterLastCf(String);

impl IterLastCf {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(NifRecord)]
#[tag = "forward"]
pub struct IterForward(Bin);

impl IterForward {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "forward"]
pub struct IterForwardCf(String, Bin);

impl IterForwardCf {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.1.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "reverse"]
pub struct IterReverse(Bin);

impl IterReverse {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "reverse"]
pub struct IterReverseCf(String, Bin);

impl IterReverseCf {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.1.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "prefix"]
pub struct IterPrefix(Bin);

impl IterPrefix {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(NifRecord)]
#[tag = "prefix"]
pub struct IterPrefixCf(String, Bin);

impl IterPrefixCf {
    pub fn name(&self) -> &str {
        self.0.as_str()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.1.as_bytes()
    }
}

#[derive(NifUntaggedEnum)]
pub enum IterMode {
    First(IterFirst),
    Last(IterLast),
    Forward(IterForward),
    Reverse(IterReverse),
    Prefix(IterPrefix),
    ForwardCf(IterForwardCf),
    ReverseCf(IterReverseCf),
    FirstCf(IterFirstCf),
    LastCf(IterLastCf),
    PrefixCf(IterPrefixCf),
}

impl Default for IterMode {
    fn default() -> IterMode {
        IterMode::First(IterFirst::First)
    }
}

impl<'a> From<&'a IterMode> for IteratorMode<'a> {
    fn from(im: &'a IterMode) -> Self {
        match im {
            IterMode::First(_) => IteratorMode::Start,
            // IterMode::FirstCf(_) => IteratorMode::Start,
            IterMode::Last(_) => IteratorMode::End,
            // IterMode::LastCf(_) => IteratorMode::End,
            IterMode::Forward(m) => IteratorMode::From(m.as_bytes(), Direction::Forward),
            // IterMode::ForwardCf(m) => IteratorMode::From(m.as_bytes(), Direction::Forward),
            IterMode::Reverse(m) => IteratorMode::From(m.as_bytes(), Direction::Reverse),
            // IterMode::ReverseCf(m) => IteratorMode::From(m.as_bytes(), Direction::Reverse),
            IterMode::Prefix(_) => {
                panic!("prefix IterMode cannot be converted into rockdb::IteratorMode")
            }
            _ => panic!("only non-cf iter modes are supported"),
        }
    }
}
