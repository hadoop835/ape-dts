use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::error::Error;
use dt_meta::{
    col_value::ColValue,
    mongo::{mongo_constant::MongoConstants, mongo_key::MongoKey},
    row_data::RowData,
    row_type::RowType,
};

use crate::{merge_parallelizer::TbMergedData, Merger};

pub struct MongoMerger {}

#[async_trait]
impl Merger for MongoMerger {
    async fn merge(&mut self, data: Vec<RowData>) -> Result<Vec<TbMergedData>, Error> {
        let mut tb_data_map: HashMap<String, Vec<RowData>> = HashMap::new();
        for row_data in data {
            let full_tb = format!("{}.{}", row_data.schema, row_data.tb);
            if let Some(tb_data) = tb_data_map.get_mut(&full_tb) {
                tb_data.push(row_data);
            } else {
                tb_data_map.insert(full_tb, vec![row_data]);
            }
        }

        let mut results = Vec::new();
        for (tb, tb_data) in tb_data_map.drain() {
            let (insert_rows, delete_rows, unmerged_rows) = Self::merge_row_data(tb_data)?;
            let tb_merged = TbMergedData {
                tb,
                insert_rows,
                delete_rows,
                unmerged_rows,
            };
            results.push(tb_merged);
        }
        Ok(results)
    }
}

impl MongoMerger {
    /// partition dmls of the same table into insert vec and delete vec
    pub fn merge_row_data(
        mut data: Vec<RowData>,
    ) -> Result<(Vec<RowData>, Vec<RowData>, Vec<RowData>), Error> {
        let mut insert_map = HashMap::new();
        let mut delete_map = HashMap::new();

        while !data.is_empty() {
            let hash_key = Self::get_hash_key(&data[0]);
            if hash_key.is_none() {
                break;
            }

            let id = hash_key.unwrap();
            let row_data = data.remove(0);
            match row_data.row_type {
                RowType::Insert => {
                    insert_map.insert(id, row_data);
                }

                RowType::Delete => {
                    insert_map.remove(&id);
                    delete_map.insert(id, row_data);
                }

                RowType::Update => {
                    let before = row_data.before.unwrap();
                    let after: HashMap<String, ColValue> = row_data.after.unwrap();
                    let delete_row = RowData {
                        row_type: RowType::Delete,
                        schema: row_data.schema.clone(),
                        tb: row_data.tb.clone(),
                        before: Some(before),
                        after: Option::None,
                    };
                    delete_map.insert(id.clone(), delete_row);

                    let insert_row = RowData {
                        row_type: RowType::Insert,
                        schema: row_data.schema,
                        tb: row_data.tb,
                        before: Option::None,
                        after: Some(after),
                    };
                    insert_map.insert(id, insert_row);
                }
            }
        }

        let inserts = insert_map.drain().map(|i| i.1).collect::<Vec<_>>();
        let deletes = delete_map.drain().map(|i| i.1).collect::<Vec<_>>();
        Ok((inserts, deletes, data))
    }

    fn get_hash_key(row_data: &RowData) -> Option<MongoKey> {
        match row_data.row_type {
            RowType::Insert => {
                let after = row_data.after.as_ref().unwrap();
                if let Some(ColValue::MongoDoc(doc)) = after.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }

            RowType::Delete => {
                let before = row_data.before.as_ref().unwrap();
                if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }

            RowType::Update => {
                let before = row_data.before.as_ref().unwrap();
                let after = row_data.after.as_ref().unwrap();
                // for Update row_data from oplog (NOT change stream), after contains diff_doc instead of doc,
                // in which case we can NOT transfer Update into Delete + Insert
                if after.get(MongoConstants::DOC).is_none() {
                    return None;
                } else if let Some(ColValue::MongoDoc(doc)) = before.get(MongoConstants::DOC) {
                    return MongoKey::from_doc(doc);
                }
            }
        }
        return None;
    }
}
