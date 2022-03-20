use rocksdb::merge_operator::MergeOperands;
use rocksdb::Options;
use rustler::NifUnitEnum;
use std::i64;

#[derive(Debug, NifUnitEnum)]
pub enum MergeOperator {
    Append,
    Prepend,
    I64Add,
    U64Add,
}

impl MergeOperator {
    pub fn set(&self, opts: &mut Options, name: &str) {
        match self {
            MergeOperator::Append => opts.set_merge_operator_associative(name, append_merge),
            MergeOperator::Prepend => opts.set_merge_operator_associative(name, prepend_merge),
            MergeOperator::I64Add => opts.set_merge_operator_associative(name, i64_add_merge),
            MergeOperator::U64Add => opts.set_merge_operator_associative(name, u64_add_merge),
        }
    }
}

pub fn append_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.len());
    existing_val.map(|v| {
        for e in v {
            result.push(*e)
        }
    });
    for op in operands {
        for e in op {
            result.push(*e)
        }
    }
    Some(result)
}

pub fn prepend_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(operands.len());
    for op in operands {
        for e in op {
            result.push(*e)
        }
    }
    existing_val.map(|v| {
        for e in v {
            result.push(*e)
        }
    });
    Some(result)
}

pub fn i64_add_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut int_val = existing_val.map(decode_i64).unwrap_or(0);
    for op in operands {
        int_val += decode_i64(op)
    }
    Some(encode_i64(int_val).to_vec())
}

fn encode_i64(val: i64) -> [u8; 8] {
    i64::to_be_bytes(val)
}

fn decode_i64(b: &[u8]) -> i64 {
    if b.len() != 8 {
        panic!("invalid i64 bytes")
    }
    let byte_arr = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
    i64::from_be_bytes(byte_arr)
}

pub fn u64_add_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut int_val = existing_val.map(decode_u64).unwrap_or(0);
    for op in operands {
        int_val += decode_u64(op)
    }
    Some(encode_u64(int_val).to_vec())
}

fn encode_u64(val: u64) -> [u8; 8] {
    u64::to_be_bytes(val)
}

fn decode_u64(b: &[u8]) -> u64 {
    if b.len() != 8 {
        panic!("invalid i64 bytes")
    }
    let byte_arr = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
    u64::from_be_bytes(byte_arr)
}
