use super::redis_object::{RedisCmd, RedisObject, RedisString};

#[derive(Debug, Clone)]
pub struct RedisEntry {
    pub id: u64,
    // whether the command is decoded from dump.rdb file
    pub is_base: bool,
    pub db_id: i64,
    pub timestamp_ms: u64,

    pub expire_ms: i64,
    pub key: RedisString,
    pub value: RedisObject,
    pub value_type_byte: u8,
    pub raw_bytes: Vec<u8>,

    pub cmd: RedisCmd,
}

impl RedisEntry {
    pub fn new() -> Self {
        Self {
            id: 0,
            is_base: false,
            db_id: 0,
            timestamp_ms: 0,

            expire_ms: 0,
            key: RedisString::new(),
            value: RedisObject::Unknown,
            raw_bytes: Vec::new(),
            value_type_byte: 0,

            cmd: RedisCmd::new(),
        }
    }

    pub fn is_raw(&self) -> bool {
        self.is_base && !self.raw_bytes.is_empty()
    }
}
