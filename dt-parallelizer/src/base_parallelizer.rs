use std::collections::VecDeque;
use std::sync::Arc;

use concurrent_queue::ConcurrentQueue;
use dt_common::error::Error;
use dt_connector::Sinker;
use dt_meta::{
    ddl_data::DdlData,
    dt_data::{DtData, DtItem},
    row_data::RowData,
};

pub struct BaseParallelizer {
    pub poped_data: VecDeque<DtItem>,
}

impl BaseParallelizer {
    pub fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        let mut data = Vec::new();
        while let Some(dt_data) = self.poped_data.pop_front() {
            data.push(dt_data);
        }

        // ddls and dmls should be drained seperately
        while let Ok(item) = buffer.pop() {
            if data.is_empty() || data[0].is_ddl() == item.is_ddl() {
                data.push(item);
            } else {
                self.poped_data.push_back(item);
                break;
            }
        }
        Ok(data)
    }

    pub async fn sink_dml(
        &self,
        mut sub_datas: Vec<Vec<RowData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_dml(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    pub async fn sink_ddl(
        &self,
        mut sub_datas: Vec<Vec<DdlData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_ddl(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }

    pub async fn sink_raw(
        &self,
        mut sub_datas: Vec<Vec<DtData>>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
        parallel_size: usize,
        batch: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for i in 0..sub_datas.len() {
            let data = sub_datas.remove(0);
            let sinker = sinkers[i % parallel_size].clone();
            let future =
                tokio::spawn(
                    async move { sinker.lock().await.sink_raw(data, batch).await.unwrap() },
                );
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }
}
