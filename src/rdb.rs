use crate::error::WithContext;
use crate::Result;
use nom::bits::complete as bits;
use nom::branch;
use nom::bytes::complete as bytes;
use nom::error::FromExternalError;
use nom::multi;
use nom::sequence;
use std::collections::HashMap;

pub(crate) type ParseResult<'a, T> = nom::IResult<&'a [u8], T>;
pub(crate) type BitParseResult<'a, T> = nom::IResult<(&'a [u8], usize), T>;

pub struct Database {}

impl Database {
    pub fn parse(data: &[u8]) -> Result<Self> {
        let sections = Self::parse_sections(data).context("Parsing RDB file")?;
        eprintln!("Parsed sections: {sections:?}");
        todo!()
    }

    fn parse_sections(data: &[u8]) -> Result<Vec<Section>> {
        let (data, _) = bytes::tag(b"REDIS0003")(data)?;
        Ok(multi::many0(Section::parse)(data)?.1)
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
    let (data, value_slice) = nom::bytes(bytes::take::<_, _, nom::error::Error<_>>(4usize))(data)?;

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

// Temporarily silence warnings
#[allow(dead_code)]
#[derive(Debug)]
enum Value<'a> {
    String(&'a str),
    Integer(i32),
}

impl<'a> Value<'a> {
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
            nom::Err::Error(nom::error::Error::from_external_error(
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
    ExpireTime {
        time: u32,
    },
    ExpireTimeMs,
    ResizeDb {
        hash_table_size: usize,
        expire_table_size: usize,
    },
    Aux(Value<'a>, Value<'a>),
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
        ))(data)
    }

    fn parse_eof(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (data, _) = bytes::tag([0xFFu8])(data)?;

        Ok((data, Self::EndOfFile))
    }

    fn parse_select_db(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFEu8])(data)?;
        todo!()
    }

    fn parse_expire_time(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFDu8])(data)?;
        todo!()
    }

    fn parse_expire_time_ms(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFCu8])(data)?;
        todo!()
    }

    fn parse_resize_db(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFBu8])(data)?;
        todo!()
    }

    fn parse_aux(data: &'a [u8]) -> ParseResult<'a, Self> {
        let (_data, _) = bytes::tag([0xFAu8])(data)?;
        let (data, key) = Value::parse_string(data)?;
        let (data, value) = Value::parse_string(data)?;

        Ok((data, Self::Aux(key, value)))
    }
}
