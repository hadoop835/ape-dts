use std::sync::{atomic::AtomicBool, Arc};

use concurrent_queue::ConcurrentQueue;

use dt_common::error::Error;
use dt_meta::dt_data::DtItem;

use crate::{
    check_log::{check_log::CheckLog, log_reader::LogReader},
    BatchCheckExtractor,
};

use super::base_extractor::BaseExtractor;

pub struct BaseCheckExtractor {
    pub check_log_dir: String,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub batch_size: usize,
    pub shut_down: Arc<AtomicBool>,
}

impl BaseCheckExtractor {
    pub async fn extract(
        &mut self,
        extractor: &mut (dyn BatchCheckExtractor + Send),
    ) -> Result<(), Error> {
        let mut log_reader = LogReader::new(&self.check_log_dir);
        let mut batch = Vec::new();

        while let Some(log) = log_reader.nextval() {
            if log.trim().is_empty() {
                continue;
            }
            let check_log = CheckLog::from_str(&log, log_reader.log_type.clone());

            if Self::can_in_same_batch(&batch, &check_log) {
                batch.push(check_log);
            } else {
                Self::batch_extract_and_clear(extractor, &mut batch).await;
                batch.push(check_log);
            }

            if batch.len() >= self.batch_size
                || (batch.len() == 1 && Self::is_any_col_none(&batch[0]))
            {
                Self::batch_extract_and_clear(extractor, &mut batch).await;
            }
        }

        Self::batch_extract_and_clear(extractor, &mut batch).await;
        BaseExtractor::wait_task_finish(self.buffer.as_ref(), self.shut_down.as_ref()).await
    }

    async fn batch_extract_and_clear(
        extractor: &mut (dyn BatchCheckExtractor + Send),
        batch: &mut Vec<CheckLog>,
    ) {
        extractor.batch_extract(batch).await.unwrap();
        batch.clear();
    }

    fn can_in_same_batch(exist_items: &Vec<CheckLog>, new_item: &CheckLog) -> bool {
        if exist_items.is_empty() {
            return true;
        }

        let same_tb = exist_items[0].schema == new_item.schema && exist_items[0].tb == new_item.tb;
        let same_log_type = exist_items[0].log_type == new_item.log_type;
        let any_col_none = Self::is_any_col_none(new_item);
        same_tb && same_log_type && !any_col_none
    }

    fn is_any_col_none(check_log: &CheckLog) -> bool {
        for i in check_log.col_values.iter() {
            if i.is_none() {
                return true;
            }
        }
        false
    }
}
