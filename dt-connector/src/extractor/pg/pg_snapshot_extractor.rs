use std::sync::{atomic::AtomicBool, Arc};

use async_trait::async_trait;
use concurrent_queue::ConcurrentQueue;
use futures::TryStreamExt;

use sqlx::{Pool, Postgres};

use dt_common::log_info;

use dt_meta::{
    adaptor::{pg_col_value_convertor::PgColValueConvertor, sqlx_ext::SqlxPgExt},
    col_value::ColValue,
    dt_data::DtItem,
    pg::{pg_col_type::PgColType, pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    position::Position,
    row_data::RowData,
};

use dt_common::error::Error;

use crate::{
    extractor::{base_extractor::BaseExtractor, snapshot_resumer::SnapshotResumer},
    rdb_query_builder::RdbQueryBuilder,
    Extractor,
};

pub struct PgSnapshotExtractor {
    pub conn_pool: Pool<Postgres>,
    pub meta_manager: PgMetaManager,
    pub resumer: SnapshotResumer,
    pub buffer: Arc<ConcurrentQueue<DtItem>>,
    pub slice_size: usize,
    pub schema: String,
    pub tb: String,
    pub shut_down: Arc<AtomicBool>,
}

#[async_trait]
impl Extractor for PgSnapshotExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!(
            r#"PgSnapshotExtractor starts, schema: "{}", tb: "{}", slice_size: {}"#,
            self.schema,
            self.tb,
            self.slice_size
        );
        self.extract_internal().await
    }

    async fn close(&mut self) -> Result<(), Error> {
        if self.conn_pool.is_closed() {
            return Ok(());
        }
        self.conn_pool.close().await;
        Ok(())
    }
}

impl PgSnapshotExtractor {
    async fn extract_internal(&mut self) -> Result<(), Error> {
        let tb_meta = self
            .meta_manager
            .get_tb_meta(&self.schema, &self.tb)
            .await?;

        if let Some(order_col) = &tb_meta.basic.order_col {
            let order_col_type = tb_meta.col_type_map.get(order_col).unwrap();

            let resume_value = if let Some(value) =
                self.resumer
                    .get_resume_value(&self.schema, &self.tb, order_col)
            {
                PgColValueConvertor::from_str(order_col_type, &value, &mut self.meta_manager)
                    .unwrap()
            } else {
                ColValue::None
            };

            self.extract_by_slices(&tb_meta, order_col, order_col_type, resume_value)
                .await?;
        } else {
            self.extract_all(&tb_meta).await?;
        }

        BaseExtractor::wait_task_finish(self.buffer.as_ref(), self.shut_down.as_ref()).await
    }

    async fn extract_all(&mut self, tb_meta: &PgTbMeta) -> Result<(), Error> {
        log_info!(
            r#"start extracting data from "{}"."{}" without slices"#,
            self.schema,
            self.tb
        );

        let mut all_count = 0;
        let sql = self.build_extract_sql(tb_meta, false);
        let mut rows = sqlx::query(&sql).fetch(&self.conn_pool);
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, tb_meta);
            BaseExtractor::push_row(self.buffer.as_ref(), row_data, Position::None)
                .await
                .unwrap();
            all_count += 1;
        }

        log_info!(
            r#"end extracting data from "{}"."{}", all count: {}"#,
            self.schema,
            self.tb,
            all_count
        );
        Ok(())
    }

    async fn extract_by_slices(
        &mut self,
        tb_meta: &PgTbMeta,
        order_col: &str,
        order_col_type: &PgColType,
        resume_value: ColValue,
    ) -> Result<(), Error> {
        log_info!(
            r#"start extracting data from "{}"."{}" by slices"#,
            self.schema,
            self.tb
        );

        let mut all_count = 0;
        let mut start_value = resume_value;
        let sql1 = self.build_extract_sql(tb_meta, false);
        let sql2 = self.build_extract_sql(tb_meta, true);
        loop {
            let start_value_for_bind = start_value.clone();
            let query = if let ColValue::None = start_value {
                sqlx::query(&sql1)
            } else {
                sqlx::query(&sql2).bind_col_value(Some(&start_value_for_bind), order_col_type)
            };

            let mut rows = query.fetch(&self.conn_pool);
            let mut slice_count = 0usize;
            while let Some(row) = rows.try_next().await.unwrap() {
                let row_data = RowData::from_pg_row(&row, tb_meta);
                start_value =
                    PgColValueConvertor::from_query(&row, order_col, order_col_type).unwrap();
                let position = if let Some(value) = start_value.to_option_string() {
                    Position::RdbSnapshot {
                        order_col: order_col.into(),
                        value,
                    }
                } else {
                    Position::None
                };
                BaseExtractor::push_row(self.buffer.as_ref(), row_data, position)
                    .await
                    .unwrap();
                slice_count += 1;
                all_count += 1;
            }

            // all data extracted
            if slice_count < self.slice_size {
                break;
            }
        }

        log_info!(
            r#"end extracting data from "{}"."{}"", all count: {}"#,
            self.schema,
            self.tb,
            all_count
        );
        Ok(())
    }

    fn build_extract_sql(&mut self, tb_meta: &PgTbMeta, has_start_value: bool) -> String {
        let query_builder = RdbQueryBuilder::new_for_pg(tb_meta);
        let cols_str = query_builder.build_extract_cols_str().unwrap();

        // SELECT col_1, col_2::text FROM tb_1 WHERE col_1 > $1 ORDER BY col_1;
        if let Some(order_col) = &tb_meta.basic.order_col {
            if has_start_value {
                let order_col_type = tb_meta.col_type_map.get(order_col).unwrap();
                format!(
                    r#"SELECT {} FROM "{}"."{}" WHERE "{}" > $1::{} ORDER BY "{}" ASC LIMIT {}"#,
                    cols_str,
                    self.schema,
                    self.tb,
                    order_col,
                    order_col_type.short_name,
                    order_col,
                    self.slice_size
                )
            } else {
                format!(
                    r#"SELECT {} FROM "{}"."{}" ORDER BY "{}" ASC LIMIT {}"#,
                    cols_str, self.schema, self.tb, order_col, self.slice_size
                )
            }
        } else {
            format!(
                r#"SELECT {} FROM "{}"."{}""#,
                cols_str, self.schema, self.tb
            )
        }
    }
}
