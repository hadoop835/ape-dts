use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::dt_data::{DtData, DtItem};

use crate::Parallelizer;

use super::base_parallelizer::BaseParallelizer;

pub struct RedisParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for RedisParallelizer {
    fn get_name(&self) -> String {
        "RedisParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        self.base_parallelizer
            .sink_raw(vec![data], sinkers, 1, false)
            .await
    }
}
