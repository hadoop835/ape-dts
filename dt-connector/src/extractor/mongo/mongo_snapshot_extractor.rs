use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use dt_common::{error::Error, log_info, utils::time_util::TimeUtil};
use dt_meta::{
    col_value::ColValue, dt_data::DtItem, mongo::mongo_constant::MongoConstants,
    position::Position, row_data::RowData, row_type::RowType,
};
use mongodb::{
    bson::{doc, oid::ObjectId, Bson, Document},
    options::FindOptions,
    Client,
};

use crate::{
    extractor::{base_extractor::BaseExtractor, snapshot_resumer::SnapshotResumer},
    Extractor,
};

pub struct MongoSnapshotExtractor {
    pub resumer: SnapshotResumer,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub db: String,
    pub tb: String,
    pub shut_down: Arc<AtomicBool>,
    pub mongo_client: Client,
}

#[async_trait]
impl Extractor for MongoSnapshotExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            "MongoSnapshotExtractor starts, schema: {}, tb: {}",
            self.db,
            self.tb
        );
        self.extract_internal().await
    }
}

impl MongoSnapshotExtractor {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        log_info!("start extracting data from {}.{}", self.db, self.tb);

        let filter = if let Some(resume_value) =
            self.resumer
                .get_resume_value(&self.db, &self.tb, MongoConstants::ID)
        {
            let start_id = ObjectId::parse_str(resume_value).unwrap();
            Some(doc! {MongoConstants::ID: {"$gt": start_id}})
        } else {
            None
        };

        // order by asc
        let find_options = FindOptions::builder()
            .sort(doc! {MongoConstants::ID: 1})
            .build();

        let mut all_count = 0;
        let collection = self
            .mongo_client
            .database(&self.db)
            .collection::<Document>(&self.tb);
        let mut cursor = collection.find(filter, find_options).await.unwrap();
        while cursor.advance().await.unwrap() {
            let doc = cursor.deserialize_current().unwrap();
            let id = Self::get_object_id(&doc);

            let mut after = HashMap::new();
            after.insert(MongoConstants::DOC.to_string(), ColValue::MongoDoc(doc));
            let row_data = RowData {
                schema: self.db.clone(),
                tb: self.tb.clone(),
                row_type: RowType::Insert,
                after: Some(after),
                before: None,
            };
            let position = Position::RdbSnapshot {
                order_col: MongoConstants::ID.into(),
                value: id,
            };
            BaseExtractor::push_row(self.buffer.as_ref(), row_data, position)
                .await
                .unwrap();
            all_count += 1;
        }

        log_info!(
            "end extracting data from {}.{}, all count: {}",
            self.db,
            self.tb,
            all_count
        );

        // wait all data to be transfered
        while !self.buffer.is_empty() {
            TimeUtil::sleep_millis(1).await;
        }

        self.shut_down.store(true, Ordering::Release);
        Ok(())
    }

    fn get_object_id(doc: &Document) -> String {
        if let Some(id) = doc.get(MongoConstants::ID) {
            match id {
                Bson::ObjectId(v) => return v.to_string(),
                _ => return String::new(),
            }
        }
        String::new()
    }
}
