use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Add;
use std::time::{self, Duration, SystemTime};

use crate::error::{Error, WithContext};
use crate::Result;
use nom::bits::complete as bits;
use nom::branch;
use nom::bytes::complete as bytes;
use nom::combinator;
use nom::error::FromExternalError;
use nom::multi;

type NomError<T> = nom::error::VerboseError<T>;
pub(crate) type ParseResult<'a, T> = nom::IResult<&'a [u8], T, NomError<&'a [u8]>>;
pub(crate) type BitParseResult<'a, T> =
    nom::IResult<(&'a [u8], usize), T, NomError<(&'a [u8], usize)>>;

pub struct Database {
    aux: HashMap<String, String>,
    keys: HashMap<String, OwnedValue>,
    expiring: HashMap<String, (OwnedValue, time::SystemTime)>,
}

impl Database {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let sections = Self::parse_sections(data).context("Parsing RDB file")?;
        eprintln!("Parsed sections: {sections:?} in file {data:?}");

        let mut aux = HashMap::new();
        let mut keys = HashMap::new();
        let mut expiring = HashMap::new();

        let size_hint = sections.iter().find_map(|s| {
            if let Section::ResizeDb {
                hash_table_size,
                expire_table_size,
            } = s
            {
                Some((hash_table_size, expire_table_size))
            } else {
                None
            }
        });

        if let Some((hash_table_size, expire_table_size)) = size_hint {
            keys.reserve(*hash_table_size);
            expiring.reserve(*expire_table_size);
        }

        let mut got_db = false;
        for sec in sections {
            match sec {
                Section::SelectDb(_) if !got_db => got_db = true,
                Section::SelectDb(_) if got_db => {
                    return Err(Error::Unimplemented)
                        .context("Multiple databases are not supported")
                }
                Section::Value(key, value) => {
                    keys.insert(key.into_owned(), value.to_owned());
                }
                Section::ExpireTime { time, key, value } => {
                    expiring.insert(
                        key.into_owned(),
                        (
                            value.to_owned(),
                            SystemTime::UNIX_EPOCH.add(Duration::from_secs(time as u64)),
                        ),
                    );
                }
                Section::ExpireTimeMs { time, key, value } => {
                    expiring.insert(
                        key.into_owned(),
                        (
                            value.to_owned(),
                            SystemTime::UNIX_EPOCH.add(Duration::from_millis(time)),
                        ),
                    );
                }
                Section::Aux(key, value) => {
                    aux.insert(key.into_owned(), value.into_owned());
                }
                _ => {}
            }
        }

        Ok(Self {
            aux,
            keys,
            expiring,
        })
    }

    fn parse_sections(data: &[u8]) -> Result<Vec<Section>> {
        let (data, _) = bytes::tag::<_, _, NomError<_>>(b"REDIS0003")(data)?;
        Ok(
            multi::many_till(combinator::cut(Section::parse), Section::parse_eof)(data)?
                .1
                 .0,
        )
    }

    pub fn aux(&self) -> &HashMap<String, String> {
        &self.aux
    }

    pub fn keys(&self) -> &HashMap<String, OwnedValue> {
        &self.keys
    }

    pub fn expiring(&self) -> &HashMap<String, (OwnedValue, time::SystemTime)> {
        &self.expiring
    }
}

fn parse_length_6bit(data: (&[u8], usize)) -> BitParseResult<usize> {
    let (data, _) = bits::tag(0usize, 2usize)(data)?;
    bits::take(6usize)(data)
}

fn parse_length_14bit(data: (&[u8], usize)) -> BitParseResult<usize> {
    let (data, _) = bits::tag(1usize, 2usize)(data)?;
    bits::take(14usize)(data)
}

fn parse_length_32bit(data: (&[u8], usize)) -> BitParseResult<usize> {
    let (data, _) = bits::tag(2usize, 2usize)(data)?;
    let (data, value_slice) = nom::bytes(bytes::take::<_, _, NomError<_>>(4usize))(data)?;

    let value = u32::from_le_bytes(
        value_slice
            .try_into()
            .expect("We took 4 bytes, so this should succeed"),
    );

    Ok((data, value as usize))
}

fn parse_length(data: &[u8]) -> ParseResult<usize> {
    nom::bits(branch::alt((
        parse_length_6bit,
        parse_length_14bit,
        parse_length_32bit,
    )))(data)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedValue {
    String(String),
    Integer(i32),
}

#[derive(Debug)]
enum Value<'a> {
    String(&'a str),
    Integer(i32),
}

impl<'a> Value<'a> {
    fn to_owned(&self) -> OwnedValue {
        match self {
            Value::String(v) => OwnedValue::String(v.to_string()),
            Value::Integer(v) => OwnedValue::Integer(*v),
        }
    }

    fn parse_key_value(data: &'a [u8]) -> ParseResult<(Cow<'a, str>, Value)> {
        branch::alt((Self::parse_kv_string,))(data)
    }

    fn parse_kv_key(data: &'a [u8]) -> ParseResult<Cow<'a, str>> {
        let (data, key) = Self::parse_string(data)?;
        let key = match key {
            Value::String(v) => Cow::Borrowed(v),
            Value::Integer(v) => Cow::Owned(v.to_string()),
            _ => unreachable!(),
        };
        Ok((data, key))
    }

    fn parse_kv_string(data: &'a [u8]) -> ParseResult<(Cow<'a, str>, Value)> {
        let (data, _) = bytes::tag([0u8])(data)?;
        let (data, key) = Self::parse_kv_key(data)?;
        let (data, value) = Self::parse_string(data)?;

        Ok((data, (key, value)))
    }

    fn parse_string(data: &'a [u8]) -> ParseResult<Value> {
        branch::alt((
            Self::parse_length_prefixed_string,
            Self::parse_int_8bit,
            Self::parse_int_16bit,
            Self::parse_int_32bit,
        ))(data)
    }

    fn parse_length_prefixed_string(data: &'a [u8]) -> ParseResult<Self> {
        let (data, length) = parse_length(data)?;
        let (data, value_slice) = bytes::take(length)(data)?;
        let value = std::str::from_utf8(value_slice).map_err(|e| {
            nom::Err::Error(NomError::from_external_error(
                value_slice,
                nom::error::ErrorKind::Verify,
                e,
            ))
        })?;

        Ok((data, Self::String(value)))
    }

    fn parse_int_8bit(data: &[u8]) -> ParseResult<Self> {
        let (data, _) = bytes::tag([0b11000000u8])(data)?;
        let (data, value_slice) = bytes::take(1usize)(data)?;
        let value = i8::from_le_bytes(
            value_slice
                .try_into()
                .expect("We took 1 byte, this should be OK"),
        );
        Ok((data, Self::Integer(value as i32)))
    }

    fn parse_int_16bit(data: &[u8]) -> ParseResult<Self> {
        let (data, _) = bytes::tag([0b11000000u8])(data)?;
        let (data, value_slice) = bytes::take(2usize)(data)?;
        let value = i16::from_le_bytes(
            value_slice
                .try_into()
                .expect("We took 2 bytes, this should be OK"),
        );
        Ok((data, Self::Integer(value as i32)))
    }

    fn parse_int_32bit(data: &[u8]) -> ParseResult<Self> {
        let (data, _) = bytes::tag([0b11000000u8])(data)?;
        let (data, value_slice) = bytes::take(4usize)(data)?;
        let value = i32::from_le_bytes(
            value_slice
                .try_into()
                .expect("We took 4 bytes, this should be OK"),
        );
        Ok((data, Self::Integer(value)))
    }
}

// Temporarily silence warnings
#[allow(dead_code)]
#[derive(Debug)]
enum Section<'a> {
    EndOfFile,
    SelectDb(usize),
    Value(Cow<'a, str>, Value<'a>),
    ExpireTime {
        time: u32,
        key: Cow<'a, str>,
        value: Value<'a>,
    },
    ExpireTimeMs {
        time: u64,
        key: Cow<'a, str>,
        value: Value<'a>,
    },
    ResizeDb {
        hash_table_size: usize,
        expire_table_size: usize,
    },
    Aux(Cow<'a, str>, Cow<'a, str>),
}

impl<'a> Section<'a> {
    fn parse(data: &'a [u8]) -> ParseResult<'a, Self> {
        branch::alt((
            Self::parse_eof,
            Self::parse_select_db,
            Self::parse_expire_time,
            Self::parse_expire_time_ms,
            Self::parse_resize_db,
            Self::parse_aux,
            Self::parse_value,
        ))(data)
    }

    fn parse_eof(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFFu8])(data)?;
        let (data, _chksum) = bytes::take(8usize)(data)?;
        Ok((data, Self::EndOfFile))
    }

    fn parse_select_db(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFEu8])(data)?;
        let (data, value) = parse_length(data)?;

        Ok((data, Self::SelectDb(value)))
    }

    fn parse_expire_time(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFDu8])(data)?;
        let (data, time_slice) = bytes::take(4usize)(data)?;
        let (data, (key, value)) = Value::parse_key_value(data)?;

        let time = u32::from_le_bytes(
            time_slice
                .try_into()
                .expect("We took 4 bytes, so this should be OK"),
        );

        Ok((data, Self::ExpireTime { time, key, value }))
    }

    fn parse_expire_time_ms(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFCu8])(data)?;
        let (data, time_slice) = bytes::take(8usize)(data)?;
        let (data, (key, value)) = Value::parse_key_value(data)?;

        let time = u64::from_le_bytes(
            time_slice
                .try_into()
                .expect("We took 8 bytes, so this should be OK"),
        );

        Ok((data, Self::ExpireTimeMs { time, key, value }))
    }

    fn parse_resize_db(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFBu8])(data)?;
        let (data, hash_table_size) = parse_length(data)?;
        let (data, expire_table_size) = parse_length(data)?;

        Ok((
            data,
            Self::ResizeDb {
                hash_table_size,
                expire_table_size,
            },
        ))
    }

    fn parse_aux(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFAu8])(data)?;
        let (data, key) = Value::parse_kv_key(data)?;
        let (data, value) = Value::parse_kv_key(data)?;

        Ok((data, Self::Aux(key, value)))
    }

    fn parse_value(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, (key, value)) = Value::parse_key_value(data)?;

        Ok((data, Self::Value(key, value)))
    }
}

#[cfg(test)]
mod test {
    use crate::rdb::OwnedValue;

    use super::Database;

    #[test]
    fn test_simple_parse() {
        let data = vec![
            82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 254, 0, 251, 1, 0, 0, 5, 97, 112, 112, 108, 101, 5, 103, 114, 97, 112, 101, 255,
            19, 92, 244, 85, 210, 137, 13, 126, 10,
        ];

        let parsed = Database::parse(&data).expect("data is valid, parsing should succeed");
        assert_eq!(
            parsed.keys().get("apple"),
            Some(&OwnedValue::String("grape".into()))
        );
    }

    #[test]
    fn test_another_parse() {
        let data = vec![
            82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 254, 0, 251, 1, 0, 0, 9, 114, 97, 115, 112, 98, 101, 114, 114, 121, 9, 98, 108,
            117, 101, 98, 101, 114, 114, 121, 255, 83, 196, 222, 77, 197, 84, 192, 150, 10,
        ];

        let parsed = Database::parse(&data).expect("data is valid, parsing should succeed");
        assert_eq!(
            parsed.keys().get("raspberry"),
            Some(&OwnedValue::String("blueberry".into()))
        );
    }

    #[test]
    fn test_muliple_values() {
        let data = vec![
            82, 69, 68, 73, 83, 48, 48, 48, 51, 250, 9, 114, 101, 100, 105, 115, 45, 118, 101, 114,
            5, 55, 46, 50, 46, 48, 250, 10, 114, 101, 100, 105, 115, 45, 98, 105, 116, 115, 192,
            64, 254, 0, 251, 4, 0, 0, 6, 98, 97, 110, 97, 110, 97, 5, 103, 114, 97, 112, 101, 0, 9,
            114, 97, 115, 112, 98, 101, 114, 114, 121, 9, 114, 97, 115, 112, 98, 101, 114, 114,
            121, 0, 5, 109, 97, 110, 103, 111, 6, 111, 114, 97, 110, 103, 101, 0, 6, 111, 114, 97,
            110, 103, 101, 6, 98, 97, 110, 97, 110, 97, 255, 83, 61, 20, 90, 34, 123, 49, 126, 10,
        ];

        let parsed = Database::parse(&data).expect("data is valid, parsing should succeed");
        assert_eq!(
            parsed.keys().get("banana"),
            Some(&OwnedValue::String("grape".into()))
        );
        assert_eq!(
            parsed.keys().get("raspberry"),
            Some(&OwnedValue::String("raspberry".into()))
        );
        assert_eq!(
            parsed.keys().get("mango"),
            Some(&OwnedValue::String("orange".into()))
        );
        assert_eq!(
            parsed.keys().get("orange"),
            Some(&OwnedValue::String("banana".into()))
        );
    }
}
