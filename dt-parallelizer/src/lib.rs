pub mod base_parallelizer;
pub mod check_parallelizer;
pub mod merge_parallelizer;
pub mod mongo_merger;
pub mod partition_parallelizer;
pub mod rdb_merger;
pub mod rdb_partitioner;
pub mod redis_parallelizer;
pub mod serial_parallelizer;
pub mod snapshot_parallelizer;
pub mod table_parallelizer;

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
use merge_parallelizer::TbMergedData;

#[async_trait]
pub trait Parallelizer {
    fn get_name(&self) -> String;

    async fn drain(&mut self, _buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        Ok(Vec::new())
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_dml(
        &mut self,
        _data: Vec<RowData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }

    async fn sink_raw(
        &mut self,
        _data: Vec<DtData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
pub trait Merger {
    async fn merge(&mut self, data: Vec<RowData>) -> Result<Vec<TbMergedData>, Error>;
}
