use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{
    ddl_data::DdlData,
    dt_data::{DtData, DtItem},
    row_data::RowData,
};

use crate::Parallelizer;

use super::base_parallelizer::BaseParallelizer;

pub struct SerialParallelizer {
    pub base_parallelizer: BaseParallelizer,
}

#[async_trait]
impl Parallelizer for SerialParallelizer {
    fn get_name(&self) -> String {
        "SerialParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        self.base_parallelizer
            .sink_dml(vec![data], sinkers, 1, false)
            .await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        self.base_parallelizer
            .sink_ddl(vec![data], sinkers, 1, false)
            .await
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
