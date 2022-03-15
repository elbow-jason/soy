use rustler::{Binary, Decoder, Encoder, Env, Error as NifError, NifResult, OwnedBinary, Term};
use std::str;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bin(Vec<u8>);

impl Bin {
    pub fn from_vec(v: Vec<u8>) -> Bin {
        Bin(v)
    }

    // pub fn from_str(s: &str) -> Bin {
    //     Bin(s.as_bytes().to_vec())
    // }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..]
    }
}

impl<'a> Decoder<'a> for Bin {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if let Ok(bytes) = term.decode::<Binary>() {
            return Ok(Bin(bytes[..].to_vec()));
        }
        if let Ok(bytes) = term.decode::<Vec<u8>>() {
            return Ok(Bin(bytes));
        }
        if let Ok(bytes) = term.decode::<&str>() {
            return Ok(Bin(bytes.as_bytes().to_vec()));
        }
        Err(NifError::BadArg)
    }
}

impl Encoder for Bin {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        new_binary(&self.0[..], env).encode(env)
    }
}

pub fn new_binary<'a>(val: &[u8], env: Env<'a>) -> Binary<'a> {
    let mut value = OwnedBinary::new(val.len()).unwrap();
    value.clone_from_slice(val);
    Binary::from_owned(value, env)
}

pub struct BinStr<'a>(Binary<'a>);

impl<'a> Decoder<'a> for BinStr<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let bin = term.decode::<Binary>()?;
        let _ = str::from_utf8(&bin[..]).map_err(|_| NifError::BadArg)?;
        Ok(BinStr(bin))
    }
}

impl Encoder for BinStr<'_> {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        self.0.encode(env)
    }
}

impl AsRef<str> for BinStr<'_> {
    fn as_ref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.0[..]) }
    }
}

impl Deref for BinStr<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.0[..]) }
    }
}
