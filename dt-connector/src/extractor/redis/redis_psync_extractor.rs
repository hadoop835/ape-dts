use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::log_position;
use dt_common::utils::rdb_filter::RdbFilter;
use dt_common::utils::time_util::TimeUtil;
use dt_common::{error::Error, log_info};
use dt_meta::dt_data::{DtData, DtItem};
use dt_meta::position::Position;
use dt_meta::redis::redis_entry::RedisEntry;
use dt_meta::redis::redis_object::RedisCmd;

use std::sync::Arc;

use crate::extractor::redis::rdb::rdb_loader::RdbLoader;
use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;
use crate::extractor::redis::redis_resp_types::Value;
use crate::Extractor;

use super::redis_client::RedisClient;

pub struct RedisPsyncExtractor<'a> {
    pub conn: &'a mut RedisClient,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub run_id: String,
    pub repl_offset: u64,
    pub now_db_id: i64,
    pub repl_port: u64,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for RedisPsyncExtractor<'_> {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "RedisPsyncExtractor starts, run_id: {}, repl_offset: {}, now_db_id: {}",
            self.run_id,
            self.repl_offset,
            self.now_db_id
        );
        if self.start_psync().await? {
            // server won't send rdb if it's NOT full sync
            self.receive_rdb().await?;
        }
        Ok(())
    }
}

impl RedisPsyncExtractor<'_> {
    pub async fn start_psync(&mut self) -> Result<bool, Error> {
        // replconf listening-port [port]
        let repl_port = self.repl_port.to_string();
        let repl_cmd = RedisCmd::from_str_args(&vec!["replconf", "listening-port", &repl_port]);
        self.conn.send(&repl_cmd).await.unwrap();
        if let Value::Okay = self.conn.read().await.unwrap() {
        } else {
            return Err(Error::ExtractorError(
                "replconf listening-port response is not Ok".into(),
            ));
        }

        let full_sync = self.run_id.is_empty() && self.repl_offset == 0;
        let (run_id, repl_offset) = if full_sync {
            ("?".to_string(), "-1".to_string())
        } else {
            (self.run_id.clone(), self.repl_offset.to_string())
        };

        // PSYNC [run_id] [offset]
        let psync_cmd = RedisCmd::from_str_args(&vec!["PSYNC", &run_id, &repl_offset]);
        self.conn.send(&psync_cmd).await.unwrap();
        let value = self.conn.read().await.unwrap();

        if let Value::Status(s) = value {
            log_info!("PSYNC command response status: {:?}", s);
            if full_sync {
                let tokens: Vec<&str> = s.split_whitespace().collect();
                self.run_id = tokens[1].to_string();
                self.repl_offset = tokens[2].parse::<u64>().unwrap();
            } else if s != "CONTINUE" {
                return Err(Error::ExtractorError(
                    "PSYNC command response is NOT CONTINUE".into(),
                ));
            }
        } else {
            return Err(Error::ExtractorError(
                "PSYNC command response is NOT status".into(),
            ));
        };
        Ok(full_sync)
    }

    async fn receive_rdb(&mut self) -> Result<(), Error> {
        // format: \n\n\n$<length>\r\n<rdb>
        loop {
            let buf = self.conn.read_raw(1).await.unwrap();
            if buf[0] == b'\n' {
                continue;
            }
            if buf[0] != b'$' {
                panic!("invalid rdb format");
            }
            break;
        }

        // length of rdb data
        let mut rdb_length_str = String::new();
        loop {
            let buf = self.conn.read_raw(1).await.unwrap();
            if buf[0] == b'\n' {
                break;
            }
            if buf[0] != b'\r' {
                rdb_length_str.push(buf[0] as char);
            }
        }
        let rdb_length = rdb_length_str.parse::<usize>().unwrap();

        let reader = RdbReader {
            conn: &mut self.conn,
            rdb_length,
            position: 0,
            copy_raw: false,
            raw_bytes: Vec::new(),
        };

        let mut loader = RdbLoader {
            reader,
            repl_stream_db_id: 0,
            now_db_id: self.now_db_id,
            expire_ms: 0,
            idle: 0,
            freq: 0,
            is_end: false,
        };

        let version = loader.load_meta()?;
        log_info!("source redis version: {:?}", version);

        loop {
            if let Some(entry) = loader.load_entry()? {
                self.now_db_id = entry.db_id;
                Self::push_to_buf(&self.buffer, &mut self.filter, entry, Position::None).await;
            }

            if loader.is_end {
                log_info!("fetch rdb finished");
                break;
            }
        }

        // this log to mark the snapshot rdb was all received
        log_position!(
            "current_position | {}",
            format!(
                "run_id:{},repl_offset:{},repl_next_offset:{},repl_port:{}",
                self.run_id,
                self.repl_offset,
                self.repl_offset + 1,
                self.repl_port
            )
        );

        Ok(())
    }

    pub async fn push_to_buf(
        buffer: &Arc<ConcurrentQueue<DtItem>>,
        filter: &mut RdbFilter,
        entry: RedisEntry,
        position: Position,
    ) {
        // currently only support db filter
        let db_id = &entry.db_id.to_string();
        if filter.filter_db(db_id) {
            return;
        }

        while buffer.is_full() {
            TimeUtil::sleep_millis(1).await;
        }

        let item = DtItem {
            dt_data: DtData::Redis { entry },
            position,
        };
        buffer.push(item).unwrap();
    }
}
