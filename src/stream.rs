use std::{
    collections::{BTreeMap, HashMap},
    num::ParseIntError,
};

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
#[non_exhaustive]
pub enum ItemIdParseError {
    #[error("The item id is missing the '-' separator")]
    MissingDash,
    #[error("The item id has too many '-' separators - only one is allowed")]
    TooManyDashes,
    #[error("'{0} is not a valid number for item id")]
    NotANumber(String, #[source] ParseIntError),
}

#[derive(Debug, Clone, Error, PartialEq)]
#[non_exhaustive]
pub enum InsertionError {
    #[error("New entry has to have ID higher than the latest one ")]
    IdIsNotGreaterThanHighestStored(ItemId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ItemId(u64, u64);

impl std::fmt::Display for ItemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl TryFrom<&str> for ItemId {
    type Error = ItemIdParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let mut parts = value.split('-');
        let timestamp = parts
            .next()
            .expect(".split() always returns at least one item");
        let counter = parts.next().ok_or(ItemIdParseError::MissingDash)?;
        if !parts.next().is_none() {
            return Err(ItemIdParseError::TooManyDashes);
        }

        let timestamp = timestamp
            .parse()
            .map_err(|err| ItemIdParseError::NotANumber(timestamp.to_string(), err))?;
        let counter = counter
            .parse()
            .map_err(|err| ItemIdParseError::NotANumber(counter.to_string(), err))?;

        Ok(Self(timestamp, counter))
    }
}

pub type ItemData = HashMap<String, String>;

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    id: ItemId,
    elements: ItemData,
}

impl Item {
    pub fn new(id: ItemId) -> Self {
        Self {
            id,
            elements: ItemData::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    items: BTreeMap<ItemId, ItemData>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            items: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, id: Option<ItemId>, data: ItemData) -> Result<ItemId, InsertionError> {
        let id = if let Some(id) = id {
            if let Some(last_key) = self.items.last_key_value().map(|(k, _v)| k) {
                if last_key >= &id {
                    return Err(InsertionError::IdIsNotGreaterThanHighestStored(*last_key));
                }
            }

            id
        } else {
            todo!("Generate a unuque id");
        };

        self.items.insert(id, data);

        Ok(id)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_item_ids() {
        assert_eq!(ItemId::try_from("0-1"), Ok(ItemId(0, 1)));
        assert_eq!(ItemId::try_from("999-0"), Ok(ItemId(999, 0)));
        assert!(ItemId::try_from("-1").is_err());
        assert!(ItemId::try_from("0-1-").is_err());
    }
}
