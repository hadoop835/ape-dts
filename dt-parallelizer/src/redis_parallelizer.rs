use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_warn};
use dt_connector::Sinker;
use dt_meta::{
    dt_data::{DtData, DtItem},
    redis::command::key_parser::KeyParser,
};

use crate::Parallelizer;

use super::base_parallelizer::BaseParallelizer;

pub struct RedisParallelizer {
    pub base_parallelizer: BaseParallelizer,
    pub parallel_size: usize,
    // redis cluster
    pub slot_node_map: HashMap<u16, &'static str>,
    pub key_parser: KeyParser,
}

#[async_trait]
impl Parallelizer for RedisParallelizer {
    fn get_name(&self) -> String {
        "RedisParallelizer".to_string()
    }

    async fn drain(&mut self, buffer: &ConcurrentQueue<DtItem>) -> Result<Vec<DtItem>, Error> {
        self.base_parallelizer.drain(buffer).await
    }

    async fn sink_raw(
        &mut self,
        data: Vec<DtData>,
        sinkers: &[Arc<async_mutex::Mutex<Box<dyn Sinker + Send>>>],
    ) -> Result<(), Error> {
        if self.slot_node_map.is_empty() {
            return self
                .base_parallelizer
                .sink_raw(vec![data], sinkers, 1, false)
                .await;
        }

        let mut node_sinker_index_map = HashMap::new();
        for (i, sinker) in sinkers.iter().enumerate() {
            node_sinker_index_map.insert(sinker.lock().await.get_id(), i);
        }

        let mut node_datas = Vec::new();
        for _ in 0..sinkers.len() {
            node_datas.push(Vec::new());
        }

        // for redis cluster
        for mut dt_data in data {
            let slots = if let DtData::Redis { entry } = &mut dt_data {
                let slots = entry.cal_slots(&self.key_parser);
                for i in 1..slots.len() {
                    if slots[i] != slots[0] {
                        return Err(Error::RedisCmdError(format!(
                            "multi keys don't hash to the same slot, cmd: {}",
                            entry.cmd.to_string()
                        )));
                    }
                }

                if slots.is_empty() {
                    log_warn!("entry has no key, cmd: {}", entry.cmd.to_string());
                }
                slots
            } else {
                // never happen
                vec![]
            };

            // example: SWAPDB 0 1
            // sink to all nodes
            if slots.is_empty() {
                for node_data in node_datas.iter_mut() {
                    node_data.push(dt_data.clone());
                }
                continue;
            }

            // find the dst node for entry by slot
            let node = *self.slot_node_map.get(&slots[0]).unwrap();
            let index = *node_sinker_index_map.get(node).unwrap();
            node_datas[index].push(dt_data);
        }

        let mut futures = Vec::new();
        for sinker in sinkers.iter().take(node_datas.len()) {
            let node_data = node_datas.remove(0);
            let sinker = sinker.clone();
            let future = tokio::spawn(async move {
                sinker
                    .lock()
                    .await
                    .sink_raw(node_data, false)
                    .await
                    .unwrap()
            });
            futures.push(future);
        }

        for future in futures {
            future.await.unwrap();
        }
        Ok(())
    }
}
