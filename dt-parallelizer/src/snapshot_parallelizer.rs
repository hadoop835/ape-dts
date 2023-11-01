use std::sync::Arc;

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtItem, row_data::RowData};

use crate::Parallelizer;

use super::base_parallelizer::BaseParallelizer;

pub struct SnapshotParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for SnapshotParallelizer {
    fn get_name(&self) -> String {
        "SnapshotParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        let sub_datas = Self::partition(data, self.parallel_size)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, true)
            .await
    }

    async fn sink_ddl(
        &mut self,
        _data: Vec<DdlData>,
        _sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        Ok(())
    }
}

impl SnapshotParallelizer {
    pub fn partition(
        data: Vec<RowData>,
        parallele_size: usize,
    ) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_datas = Vec::new();
        if parallele_size <= 1 {
            sub_datas.push(data);
            return Ok(sub_datas);
        }

        let avg_size = data.len() / parallele_size + 1;
        for _ in 0..parallele_size {
            sub_datas.push(Vec::with_capacity(avg_size));
        }

        for (i, row_data) in data.into_iter().enumerate() {
            sub_datas[i / avg_size].push(row_data);
        }
        Ok(sub_datas)
    }
}
