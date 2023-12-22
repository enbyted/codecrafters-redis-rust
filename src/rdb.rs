use crate::error::WithContext;
use crate::Result;
use nom::branch;
use nom::bytes::complete as bytes;
use nom::multi;
use std::collections::HashMap;

pub(crate) type ParseResult<'a, T> = nom::IResult<&'a [u8], T>;

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

// Temporarily silence warnings
#[allow(dead_code)]
#[derive(Debug)]
enum Value<'a> {
    String(&'a str),
    Integer(i64),
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
    Aux(HashMap<&'a str, &'a str>),
}

impl Section<'_> {
    fn parse(data: &[u8]) -> ParseResult<'_, Self> {
        branch::alt((
            Self::parse_eof,
            Self::parse_select_db,
            Self::parse_expire_time,
            Self::parse_expire_time_ms,
            Self::parse_resize_db,
            Self::parse_aux,
        ))(data)
    }

    fn parse_eof(data: &[u8]) -> ParseResult<'_, Self> {
        let (data, _) = bytes::tag([0xFFu8])(data)?;

        Ok((data, Self::EndOfFile))
    }

    fn parse_select_db(data: &[u8]) -> ParseResult<'_, Self> {
        let (_data, _) = bytes::tag([0xFEu8])(data)?;
        todo!()
    }

    fn parse_expire_time(data: &[u8]) -> ParseResult<'_, Self> {
        let (_data, _) = bytes::tag([0xFDu8])(data)?;
        todo!()
    }

    fn parse_expire_time_ms(data: &[u8]) -> ParseResult<'_, Self> {
        let (_data, _) = bytes::tag([0xFCu8])(data)?;
        todo!()
    }

    fn parse_resize_db(data: &[u8]) -> ParseResult<'_, Self> {
        let (_data, _) = bytes::tag([0xFBu8])(data)?;
        todo!()
    }

    fn parse_aux(data: &[u8]) -> ParseResult<'_, Self> {
        let (_data, _) = bytes::tag([0xFAu8])(data)?;
        todo!()
    }
}
