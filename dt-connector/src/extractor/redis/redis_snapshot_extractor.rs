use super::redis_client::RedisClient;
use super::redis_psync_extractor::RedisPsyncExtractor;
use crate::extractor::base_extractor::BaseExtractor;
use crate::Extractor;
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_common::utils::rdb_filter::RdbFilter;
use dt_meta::dt_data::DtItem;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct RedisSnapshotExtractor {
    pub conn: RedisClient,
    pub repl_port: u64,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub shut_down: Arc<AtomicBool>,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for RedisSnapshotExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        let mut psync_extractor = RedisPsyncExtractor {
            conn: &mut self.conn,
            buffer: self.buffer.clone(),
            run_id: String::new(),
            repl_offset: 0,
            repl_port: self.repl_port,
            now_db_id: 0,
            filter: self.filter.clone(),
        };
        psync_extractor.extract().await?;
        BaseExtractor::wait_task_finish(self.buffer.as_ref(), self.shut_down.as_ref()).await
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.conn.close().await
    }
}
