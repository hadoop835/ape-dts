use std::collections::HashMap;

use dt_common::error::Error;

use super::{
    mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
    rdb_tb_meta::RdbTbMeta,
};

#[derive(Clone)]
pub struct RdbMetaManager {
    mysql_meta_manager: Option<MysqlMetaManager>,
    pg_meta_manager: Option<PgMetaManager>,
}

impl RdbMetaManager {
    pub fn from_mysql(mysql_meta_manager: MysqlMetaManager) -> Self {
        Self {
            mysql_meta_manager: Some(mysql_meta_manager),
            pg_meta_manager: Option::None,
        }
    }

    pub fn from_pg(pg_meta_manager: PgMetaManager) -> Self {
        Self {
            mysql_meta_manager: Option::None,
            pg_meta_manager: Some(pg_meta_manager),
        }
    }

    pub async fn get_tb_meta(&mut self, schema: &str, tb: &str) -> Result<RdbTbMeta, Error> {
        if let Some(mysql_meta_manager) = self.mysql_meta_manager.as_mut() {
            let tb_meta = mysql_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(tb_meta.basic);
        }

        if let Some(pg_meta_manager) = self.pg_meta_manager.as_mut() {
            let tb_meta = pg_meta_manager.get_tb_meta(schema, tb).await?;
            return Ok(tb_meta.basic);
        }

        Err(Error::Unexpected(
            "no available meta_manager in partitioner".into(),
        ))
    }

    pub fn parse_rdb_cols(
        key_map: &HashMap<String, Vec<String>>,
        cols: &[String],
    ) -> Result<(Option<String>, String, Vec<String>), Error> {
        let mut id_cols = Vec::new();
        if let Some(cols) = key_map.get("primary") {
            // use primary key
            id_cols = cols.clone();
        } else if !key_map.is_empty() {
            // use the unique key with least cols
            for key_cols in key_map.values() {
                if id_cols.is_empty() || id_cols.len() > key_cols.len() {
                    id_cols = key_cols.clone();
                }
            }
        }

        let order_col = if id_cols.len() == 1 {
            Some(id_cols.get(0).unwrap().clone())
        } else {
            None
        };

        if id_cols.is_empty() {
            id_cols = cols.to_owned();
        }

        let partition_col = id_cols[0].clone();
        Ok((order_col, partition_col, id_cols))
    }
}
