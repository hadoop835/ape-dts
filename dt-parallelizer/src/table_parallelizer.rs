use std::{collections::HashMap, sync::Arc};

use crate::Parallelizer;
use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{ddl_data::DdlData, dt_data::DtItem, row_data::RowData};

use super::base_parallelizer::BaseParallelizer;

pub struct TableParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
}

#[async_trait]
impl Parallelizer for TableParallelizer {
    fn get_name(&self) -> String {
        "TableParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        self.base_parallelizer.drain(buffer)
    }

    async fn sink_dml(
        &mut self,
        data: Vec<RowData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        let sub_datas = Self::partition_dml(data)?;
        self.base_parallelizer
            .sink_dml(sub_datas, sinkers, self.parallel_size, false)
            .await
    }

    async fn sink_ddl(
        &mut self,
        data: Vec<DdlData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        let sub_datas = Self::partition_ddl(data)?;
        self.base_parallelizer
            .sink_ddl(sub_datas, sinkers, self.parallel_size, false)
            .await
    }
}

impl TableParallelizer {
    /// partition dml vec into sub vecs by full table name
    pub fn partition_dml(data: Vec<RowData>) -> Result<Vec<Vec<RowData>>, Error> {
        let mut sub_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
            if let Some(sub_data) = sub_data_map.get_mut(&full_tb) {
                sub_data.push(row_data);
            } else {
                sub_data_map.insert(full_tb, vec![row_data]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }

    /// partition ddl vec into sub vecs by schema
    pub fn partition_ddl(data: Vec<DdlData>) -> Result<Vec<Vec<DdlData>>, Error> {
        let mut sub_data_map: HashMap<String, Vec<DdlData>> = HashMap::new();
        for ddl in data {
            if let Some(sub_data) = sub_data_map.get_mut(&ddl.schema) {
                sub_data.push(ddl);
            } else {
                sub_data_map.insert(ddl.schema.clone(), vec![ddl]);
            }
        }

        Ok(sub_data_map.into_values().collect())
    }
}
