use std::path::{Path, PathBuf};
use std::{collections::HashMap, sync::Arc, time::SystemTime};

use tokio::fs;
use tokio::sync::Mutex;

use crate::error::{Error, WithContext};
use crate::stream::{InsertListener, ItemData, ItemId, ProvidedItemId, Stream};
use crate::{rdb, Result};

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Stream(Stream),
}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Stream(_) => "stream",
        }
    }

    pub fn as_stream(&self) -> Option<&Stream> {
        match self {
            Value::Stream(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_stream_mut(&mut self) -> Option<&mut Stream> {
        match self {
            Value::Stream(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataValue {
    value: Value,
    expires_at: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct DataStore {
    data: Arc<Mutex<HashMap<String, DataValue>>>,
    config: Arc<HashMap<String, String>>,
}

impl DataStore {
    pub fn new(config: HashMap<String, String>) -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
            config: Arc::new(config),
        }
    }

    pub async fn load_from_rdb(&mut self) -> Result<()> {
        match (self.config.get("dir"), self.config.get("dbfilename")) {
            (Some(dir), Some(file)) => {
                let path = Path::new(dir).join(file);
                let data = Self::read_file(&path)
                    .await
                    .context(format!("File path {path:?}").as_str())?;
                eprintln!("RDB file data: {data:?}");

                let parsed = rdb::Database::parse(&data)?;

                let mut data = self.data.lock().await;
                for (key, value) in parsed.keys() {
                    let value = match value {
                        rdb::OwnedValue::String(s) => s.clone(),
                        rdb::OwnedValue::Integer(v) => v.to_string(),
                    };

                    data.insert(
                        key.clone(),
                        DataValue {
                            value: Value::String(value),
                            expires_at: None,
                        },
                    );
                }
                let now = SystemTime::now();
                for (key, (value, expires_at)) in parsed.expiring() {
                    let expires_at = expires_at.clone();

                    if expires_at < now {
                        continue;
                    }

                    let value = match value {
                        rdb::OwnedValue::String(s) => s.clone(),
                        rdb::OwnedValue::Integer(v) => v.to_string(),
                    };

                    data.insert(
                        key.clone(),
                        DataValue {
                            value: Value::String(value),
                            expires_at: Some(expires_at),
                        },
                    );
                }
            }
            (Some(_), None) => eprintln!("Not loading database, `dbfilename` not provided"),
            (None, Some(_)) => eprintln!("Not loading database, `dir` not provided"),
            (None, None) => eprintln!("Not loading database, `dir` and `dbfilename` not provided"),
        }
        Ok(())
    }

    async fn read_file(path: &PathBuf) -> Result<Vec<u8>> {
        Ok(fs::read(path).await?)
    }

    pub async fn set(
        &self,
        key: String,
        value: Value,
        expires_at: Option<SystemTime>,
    ) -> Option<Value> {
        let now = SystemTime::now();

        self.data
            .clone()
            .lock_owned()
            .await
            .insert(key, DataValue { value, expires_at })
            .and_then(|v| match v.expires_at {
                Some(expires_at) if expires_at <= now => None,
                _ => Some(v.value),
            })
    }

    pub async fn get_ref<T>(&self, key: &str, op: impl FnOnce(&Value) -> T) -> Option<T> {
        let now = SystemTime::now();

        self.data
            .lock()
            .await
            .get(key)
            .and_then(|v| match v.expires_at {
                Some(expires_at) if expires_at <= now => None,
                _ => Some(op(&v.value)),
            })
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        self.get_ref(key, |v| v.clone()).await
    }

    pub async fn insert_stream_item(
        &self,
        key: String,
        id: ProvidedItemId,
        data: ItemData,
    ) -> Result<ItemId> {
        Ok(self
            .data
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| DataValue {
                value: Value::Stream(Stream::new(key)),
                expires_at: None,
            })
            .value
            .as_stream_mut()
            .ok_or(Error::ExpectedOtherType("stream"))?
            .insert(id, data)?)
    }

    pub async fn notify_on_stream_insert(
        &self,
        key: String,
        listener: InsertListener,
    ) -> Result<()> {
        self.data
            .lock()
            .await
            .entry(key.clone())
            .or_insert_with(|| DataValue {
                value: Value::Stream(Stream::new(key)),
                expires_at: None,
            })
            .value
            .as_stream_mut()
            .ok_or(Error::ExpectedOtherType("stream"))?
            .notify_on_insert(listener);

        Ok(())
    }

    pub async fn keys(&self) -> Vec<String> {
        self.data.lock().await.keys().map(|k| k.clone()).collect()
    }

    pub fn get_config(&self, key: &str) -> Option<&str> {
        self.config.get(key).map(|s| s.as_str())
    }
}
